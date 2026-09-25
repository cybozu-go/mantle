#!/bin/bash

# This shell script is forked from:
#
#     https://github.com/rook/rook/blob/fb02f500be4e0b80478366e973abf4e6870693a9/images/ceph/toolbox.sh
#
# It is distributed under Apache-2.0 license:
#
#     Copyright 2016 The Rook Authors. All rights reserved.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

set -e
set -o pipefail

CEPH_CONFIG="${CEPH_CONFIG:-/etc/ceph/ceph.conf}"
MON_CONFIG="${MON_CONFIG:-/etc/rook/mon-endpoints}"
KEYRING_FILE="${KEYRING_FILE:-/etc/ceph/keyring}"

# create a ceph config file in its default location so ceph/rados tools can be used
# without specifying any arguments
write_endpoints() {
  endpoints=$(cat "${MON_CONFIG}")
  # filter out the mon names
  # external cluster can have numbers or hyphens in mon names, handling them in regex
  # shellcheck disable=SC2001
  mon_endpoints=$(echo "${endpoints}"| sed 's/[a-z0-9_-]\+=//g')
  DATE=$(date)
  echo "$DATE writing mon endpoints to ${CEPH_CONFIG}: ${endpoints}"
    cat <<EOF > "${CEPH_CONFIG}"
[global]
mon_host = ${mon_endpoints}
[client.admin]
keyring = ${KEYRING_FILE}
EOF
}
# read the secret from an env var (for backward compatibility), or from the secret file
ceph_secret=${ROOK_CEPH_SECRET}
if [[ "$ceph_secret" == "" ]]; then
  ceph_secret=$(cat /var/lib/rook-ceph-mon/secret.keyring)
fi
# create the keyring file
cat <<EOF > "${KEYRING_FILE}"
[${ROOK_CEPH_USERNAME}]
key = ${ceph_secret}
EOF
# write the initial config file
write_endpoints

# import
rbd_import() {
    echo "start import"
    if [ "${CERT_FILE}" != "" ]; then
        if [ "${TRANSFER_COMPRESSION}" = "zstd" ]; then
            s5cmd --endpoint-url "${OBJECT_STORAGE_ENDPOINT}" --credentials-file ${CERT_FILE} cat "s3://${BUCKET_NAME}/${OBJ_NAME}" | zstd -d -q -c | rbd import-diff -p ${POOL_NAME} - ${DST_IMAGE_NAME}
        else
            s5cmd --endpoint-url "${OBJECT_STORAGE_ENDPOINT}" --credentials-file ${CERT_FILE} cat "s3://${BUCKET_NAME}/${OBJ_NAME}" | rbd import-diff -p ${POOL_NAME} - ${DST_IMAGE_NAME}
        fi
    else
        if [ "${TRANSFER_COMPRESSION}" = "zstd" ]; then
            s5cmd --endpoint-url "${OBJECT_STORAGE_ENDPOINT}" cat "s3://${BUCKET_NAME}/${OBJ_NAME}" | zstd -d -q -c | rbd import-diff -p ${POOL_NAME} - ${DST_IMAGE_NAME}
        else
            s5cmd --endpoint-url "${OBJECT_STORAGE_ENDPOINT}" cat "s3://${BUCKET_NAME}/${OBJ_NAME}" | rbd import-diff -p ${POOL_NAME} - ${DST_IMAGE_NAME}
        fi
    fi
    echo "finish import"
}

rbd_snap_rollback() {
    echo "start rollback"
    rbd snap rollback "$@"
    echo "finish rollback"
}

# Roll back ${POOL_NAME}/${DST_IMAGE_NAME} to its snapshot $1 to guarantee that
# the import target is exactly the expected state, so that the subsequent
# import-diff applies correctly. Note that "rbd import-diff" itself only checks
# that the start snapshot recorded in the diff exists in the image; it never
# checks that the HEAD is identical to that snapshot.
#
# The rollback is expensive, so it is skipped if the HEAD already has exactly
# the same size and contents as the snapshot, in which case the rollback would
# be a no-op. Note that this check must be conservative: the rollback must be
# performed whenever there is any chance that the HEAD differs from the
# snapshot. Otherwise the subsequent import-diff would corrupt the image. In
# particular, the following cases must be detected:
#
#   - The previous import job was interrupted, say by a power failure, after it
#     had partially applied the diff to the HEAD.
#   - The snapshot the incremental data is based on is not the latest snapshot
#     of this image. It happens when a MantleBackup is deleted only in the
#     primary cluster.
#
# This function assumes that no other process writes to the image while it is
# running. The import Job is the only writer of the destination image, and its
# Pod is not expected to run concurrently with another Pod of the same Job.
#
# This function must not be called in a context where its exit status is
# tested, e.g. in an if statement or in a command substitution, because then
# "set -e" wouldn't abort the script when an rbd command fails.
rbd_snap_rollback_if_needed() {
    local snap_name=$1
    local head_size snap_size
    # "not compared" records that "rbd diff" wasn't run because the sizes
    # already differ, so that the log below is unambiguous about why the
    # rollback is needed.
    local diff="not compared"

    # "rbd snap rollback" restores the size of the image as well as its
    # contents. "rbd diff" scans only the range [0, size of the HEAD), so it
    # can't detect that the HEAD was shrunk after the snapshot was taken.
    # Therefore we have to compare the sizes by ourselves.
    head_size=$(rbd info --format json "${POOL_NAME}/${DST_IMAGE_NAME}" | jq -r '.size')
    snap_size=$(rbd info --format json "${POOL_NAME}/${DST_IMAGE_NAME}@${snap_name}" | jq -r '.size')

    # Compare the sizes with -eq, which fails unless both of them are integers.
    # Anything else, e.g. the "null" jq prints for a missing size field, then
    # leads to the rollback instead of being silently taken for equal.
    if [ "${head_size}" -eq "${snap_size}" ]; then
        # "rbd diff --from-snap" lists every extent of the HEAD that may have
        # been updated after the snapshot was taken. It may report extents that
        # were rewritten with the same contents, but it never misses updated
        # ones. Hence, an empty list guarantees that the HEAD is identical to
        # the snapshot.
        #
        # Use "--format json" rather than the default plain output, because it
        # gives us a machine-readable answer that doesn't depend on how rbd
        # lays out its text table. The JSON output is an array of extents, so
        # the HEAD is identical to the snapshot if and only if it is "[]".
        # Comparing the whole JSON rather than counting the elements also
        # rejects anything that isn't an empty array, e.g. an empty output.
        #
        # Do NOT pass "--whole-object" here. It makes rbd answer from the
        # object map, which can't be trusted after an interrupted rollback:
        # "rbd snap rollback" writes the object map of the snapshot back to the
        # HEAD before it restores the data objects, and it doesn't flag the
        # object map invalid in between. A rollback killed between the two
        # steps therefore leaves an object map claiming that the HEAD matches
        # the snapshot while the data doesn't, and the retry would skip the
        # rollback that is still needed. Without "--whole-object" rbd asks the
        # OSDs about the objects themselves, which reflects the data that is
        # really there.
        diff=$(rbd diff --format json --from-snap "${snap_name}" \
            "${POOL_NAME}/${DST_IMAGE_NAME}" | jq -c)
        if [ "${diff}" = "[]" ]; then
            echo "skip rollback: ${POOL_NAME}/${DST_IMAGE_NAME} is already identical to ${snap_name}"
            return
        fi
    fi

    # The diff can list a huge number of extents, so log only the head of it.
    echo "rollback needed: ${POOL_NAME}/${DST_IMAGE_NAME} differs from ${snap_name}:" \
        "head_size=${head_size} snap_size=${snap_size} diff=${diff:0:200}"
    rbd_snap_rollback "${POOL_NAME}/${DST_IMAGE_NAME}@${snap_name}"
}

if [ -z "${FROM_SNAP_NAME}" ]; then
    set +o pipefail
    set +e
    count=$(rbd snap ls ${POOL_NAME}/${DST_IMAGE_NAME} | grep -c initialsnap || true)
    set -e
    set -o pipefail
    if [ "$count" -eq 0 ]; then
        echo "start initialsnap creation"
        rbd snap create ${POOL_NAME}/${DST_IMAGE_NAME}@initialsnap
        echo "finish initialsnap creation"
    else
        rbd_snap_rollback_if_needed initialsnap
    fi
    rbd_import
    echo "start initialsnap deletion"
    rbd snap rm ${POOL_NAME}/${DST_IMAGE_NAME}@initialsnap
    echo "finish initialsnap deletion"
else
    rbd_snap_rollback_if_needed "${FROM_SNAP_NAME}"
    rbd_import
fi
