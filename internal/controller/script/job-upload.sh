#!/bin/bash

set -e
if [ -e "/mantle/uploaded" ]; then
  exit
fi
export_file="/mantle/export-${PART_NUM}.bin"
if [ "${TRANSFER_COMPRESSION}" = "zstd" ]; then
  export_file="${export_file}.zst"
fi
if [ "${CERT_FILE}" = "" ]; then
  # --region ceph is necessary to avoid AWS CLI trying to fetch the region from EC2 metadata service.
  aws --endpoint-url "${OBJECT_STORAGE_ENDPOINT}" --region ceph s3 cp "${export_file}" "s3://${BUCKET_NAME}/${OBJ_NAME}"
else
  aws --endpoint-url "${OBJECT_STORAGE_ENDPOINT}" --region ceph --ca-bundle "${CERT_FILE}" s3 cp "${export_file}" "s3://${BUCKET_NAME}/${OBJ_NAME}"
fi
touch /mantle/uploaded
