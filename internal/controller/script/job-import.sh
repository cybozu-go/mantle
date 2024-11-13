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

CEPH_CONFIG="/etc/ceph/ceph.conf"
MON_CONFIG="/etc/rook/mon-endpoints"
KEYRING_FILE="/etc/ceph/keyring"
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
rbd_import() {
    if [ "${CERT_FILE}" != "" ]; then
        s5cmd --endpoint-url "${OBJECT_STORAGE_ENDPOINT}" --credentials-file ${CERT_FILE} cat "s3://${BUCKET_NAME}/${OBJ_NAME}" | rbd import-diff -p ${POOL_NAME} - ${DST_IMAGE_NAME}
    else
        s5cmd --endpoint-url "${OBJECT_STORAGE_ENDPOINT}" cat "s3://${BUCKET_NAME}/${OBJ_NAME}" | rbd import-diff -p ${POOL_NAME} - ${DST_IMAGE_NAME}
    fi
}
if [ -z "${FROM_SNAP_NAME}" ]; then
    set +o pipefail
    set +e
    count=$(rbd snap ls ${POOL_NAME}/${DST_IMAGE_NAME} | grep -c initialsnap || true)
    set -e
    set -o pipefail
    if [ "$count" -eq 0 ]; then
        rbd snap create ${POOL_NAME}/${DST_IMAGE_NAME}@initialsnap
    else
        rbd snap rollback ${POOL_NAME}/${DST_IMAGE_NAME}@initialsnap
    fi
    rbd_import
    rbd snap rm ${POOL_NAME}/${DST_IMAGE_NAME}@initialsnap
else
    rbd snap rollback ${POOL_NAME}/${DST_IMAGE_NAME}@${FROM_SNAP_NAME}
    rbd_import
fi
