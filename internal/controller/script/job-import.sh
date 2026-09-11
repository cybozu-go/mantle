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
  endpoints=$(cat ${MON_CONFIG})
  # filter out the mon names
  # external cluster can have numbers or hyphens in mon names, handling them in regex
  # shellcheck disable=SC2001
  mon_endpoints=$(echo "${endpoints}"| sed 's/[a-z0-9_-]\+=//g')
  DATE=$(date)
  echo "$DATE writing mon endpoints to ${CEPH_CONFIG}: ${endpoints}"
    cat <<EOF > ${CEPH_CONFIG}
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
cat <<EOF > ${KEYRING_FILE}
[${ROOK_CEPH_USERNAME}]
key = ${ceph_secret}
EOF
# write the initial config file
write_endpoints

# import

# Key of the RBD image metadata that holds the name of the snapshot the image
# head is identical to. If the key is absent, the contents of the image head
# are unknown, e.g., because an import-diff was interrupted halfway.
CLEAN_SNAP_KEY="mantle.clean-snap"

# get_clean_snap prints the name of the snapshot the image head is identical
# to, or nothing if it is unknown.
#
# Note that we list the metadata instead of getting the single key, because
# rbd image-meta get reports an absent key only with its exit status, which we
# would have to distinguish from the other errors. Note also that rbd may
# write warnings to its stderr, which must not be mixed into the name.
#
# The failure of rbd is reported with an explicit return, because this function
# is called in a command substitution, where set -e is ignored unless the
# inherit_errexit option is enabled.
get_clean_snap() {
    local metadata
    metadata=$(rbd image-meta list --format json "${POOL_NAME}/${DST_IMAGE_NAME}") || return $?

    # rbd prints nothing if the image has no metadata at all. Note that this
    # pipeline is the last command of this function, and thus its failure is
    # reported to the caller as the status of the command substitution.
    printf '%s' "${metadata}" | jq -r --arg key "${CLEAN_SNAP_KEY}" '.[$key] // empty'
}

set_clean_snap() {
    rbd image-meta set "${POOL_NAME}/${DST_IMAGE_NAME}" "${CLEAN_SNAP_KEY}" "$1"
}

# snapshot_exists reports whether the given snapshot exists on the destination
# image.
#
# It terminates this script if rbd or jq fails, instead of returning the error
# to the caller, because this function is called in a condition list, where
# set -e is disabled and thus a failure would silently be taken as the absence
# of the snapshot.
snapshot_exists() {
    local snaps status=0

    snaps=$(rbd snap ls --format json "${POOL_NAME}/${DST_IMAGE_NAME}") || exit $?

    # jq -e exits with 1 if the last output is false, and with a status greater
    # than 1 if it fails, e.g., because the input is not valid JSON. Note that
    # rbd prints at least an empty array, so an empty output is an error too,
    # which jq -e reports with the status 4.
    printf '%s' "${snaps}" |
        jq -e --arg name "$1" 'any(.[]; .name == $name)' > /dev/null || status=$?
    case "${status}" in
        0) return 0 ;;
        1) return 1 ;;
    esac

    echo "failed to check whether the snapshot $1 exists" >&2
    exit "${status}"
}

# unset_clean_snap declares that the contents of the image head are unknown.
# It must be called before the image is modified.
unset_clean_snap() {
    local clean_snap
    clean_snap=$(get_clean_snap)
    if [ -n "${clean_snap}" ]; then
        rbd image-meta remove "${POOL_NAME}/${DST_IMAGE_NAME}" "${CLEAN_SNAP_KEY}"
    fi
}

rbd_import() {
    echo "start import"

    # rbd import-diff modifies the image and creates a snapshot at the end of
    # it, so the image head is identical to no snapshot while it is running.
    unset_clean_snap

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

    # The image head is now identical to the snapshot import-diff has just
    # created, whose name is given as TO_SNAP_NAME. The state of the image head
    # is left unknown if the snapshot is missing, i.e., import-diff didn't
    # create the expected snapshot, or if TO_SNAP_NAME is empty, i.e., this Job
    # was created by an older version of mantle. In both cases the next Job
    # just rolls back the image as before.
    if [ -z "${TO_SNAP_NAME}" ]; then
        echo "TO_SNAP_NAME is not given; the state of the image head is left unknown"
    elif snapshot_exists "${TO_SNAP_NAME}"; then
        set_clean_snap "${TO_SNAP_NAME}"
    else
        echo "${TO_SNAP_NAME} does not exist; the state of the image head is left unknown"
    fi

    echo "finish import"
}

# get_image_size prints the size of the given image or snapshot in bytes.
#
# Note that jq -e is used so that a missing size is reported as a failure
# instead of being printed as the string "null". Two such sizes would compare
# equal, and the rollback would wrongly be skipped. Note also that this
# pipeline is the only command of this function, and thus its failure is
# reported to the caller as the status of the command substitution.
get_image_size() {
    rbd info --format json "$1" | jq -e -r '.size'
}

# rbd_snap_rollback rolls the image head back to the given snapshot, unless the
# head is already identical to it.
#
# Note that we don't compare the contents of the image and the snapshot with
# rbd diff, because it is impractically slow unless the fast-diff feature is
# enabled. Instead, we track the state of the image head with the image
# metadata; this is possible because only the import and zeroout Jobs modify
# the image while mantle holds the lock of the volume. Both invalidate the
# metadata before modifying the image.
rbd_snap_rollback() {
    local snap_name="$1"
    local clean_snap head_size snap_size

    # Note that every rbd command below is called outside a condition list,
    # because set -e is disabled in a command of a condition list and thus
    # their errors would be silently ignored there.
    clean_snap=$(get_clean_snap)
    if [ "${clean_snap}" = "${snap_name}" ]; then
        # rbd snap rollback resizes the image to the size of the snapshot, so
        # the rollback is needed if the sizes differ. Note that the image can
        # be resized by ceph-csi without changing its contents.
        head_size=$(get_image_size "${POOL_NAME}/${DST_IMAGE_NAME}")
        snap_size=$(get_image_size "${POOL_NAME}/${DST_IMAGE_NAME}@${snap_name}")
        if [ "${head_size}" = "${snap_size}" ]; then
            echo "skip rollback because the image is already the same as ${snap_name}"

            return
        fi
    fi

    echo "start rollback"
    # A failed rollback can leave the image only partially restored.
    unset_clean_snap
    rbd snap rollback "${POOL_NAME}/${DST_IMAGE_NAME}@${snap_name}"
    # The image head is identical to the snapshot just after the rollback, so
    # the rollback is not repeated even if this Job is retried from here.
    set_clean_snap "${snap_name}"
    echo "finish rollback"
}

# is_import_applied reports whether the diff this Job imports has already been
# applied to the destination image.
#
# rbd import-diff creates the snapshot TO_SNAP_NAME only after it has applied
# the whole diff, and aborts with EEXIST if that snapshot already exists. The
# existence of the snapshot therefore means that the import has completed, and
# repeating it is not only wasteful but impossible: without this check, a Job
# that failed after its import-diff, e.g., while recording the clean snapshot,
# could never succeed again, and the lock of the volume would never be released.
# cf. do_image_snap_to() and do_import_diff_fd() in rbd/action/Import.cc
#
# Note that this relies on TO_SNAP_NAME identifying this import uniquely, which
# holds because mantle removes the snapshot of a MantleBackup when the
# MantleBackup is deleted, and the middle snapshots are named after the UID of
# the MantleBackup on the primary cluster.
is_import_applied() {
    if [ -z "${TO_SNAP_NAME}" ]; then
        # This Job was created by an older version of mantle, which doesn't
        # tell us the name of the snapshot import-diff creates.
        return 1
    fi

    snapshot_exists "${TO_SNAP_NAME}"
}

# remove_initialsnap removes initialsnap if it is still there. This Job can be
# restarted after the removal succeeded but before the Job was recorded as
# complete, so the removal must be skipped if the snapshot is already gone.
remove_initialsnap() {
    if ! snapshot_exists initialsnap; then
        return
    fi

    echo "start initialsnap deletion"
    rbd snap rm "${POOL_NAME}/${DST_IMAGE_NAME}@initialsnap"
    echo "finish initialsnap deletion"
}

if is_import_applied; then
    # Neither the rollback nor the import must run here: import-diff would
    # abort with EEXIST, and the rollback would discard the imported data.
    #
    # Note that the clean snapshot metadata is left as it is, because the
    # existence of the snapshot tells nothing about the current contents of the
    # image head. If it is absent, the next Job just rolls back as before.
    echo "skip rollback and import because ${TO_SNAP_NAME} already exists"
elif [ -z "${FROM_SNAP_NAME}" ]; then
    # Keep this for older zeroout Jobs that did not invalidate the metadata
    # before modifying the image head.
    unset_clean_snap

    if ! snapshot_exists initialsnap; then
        echo "start initialsnap creation"
        rbd snap create ${POOL_NAME}/${DST_IMAGE_NAME}@initialsnap
        # Note that the image head is identical to initialsnap here, but there
        # is no point in recording it: a full import discards the metadata
        # above before it uses it, and so would the retry of this Job.
        echo "finish initialsnap creation"
    else
        # Roll back here to guarantee that the import target is exactly
        # the expected state, so that the subsequent import-diff applies
        # correctly. The rollback is skipped if the image is already in the
        # expected state, which is the usual case.
        rbd_snap_rollback initialsnap
    fi
    rbd_import
else
    # See the comment above for why we roll back here.
    rbd_snap_rollback "${FROM_SNAP_NAME}"
    rbd_import
fi

# initialsnap is needed only while a full import is in progress. Remove it
# outside the branches above, because the import may have been skipped after an
# interrupted run had already created it.
if [ -z "${FROM_SNAP_NAME}" ]; then
    remove_initialsnap
fi
