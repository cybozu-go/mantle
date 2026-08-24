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
  s5cmd --endpoint-url ${OBJECT_STORAGE_ENDPOINT} cp "${export_file}" "s3://${BUCKET_NAME}/${OBJ_NAME}"
else
  s5cmd --endpoint-url ${OBJECT_STORAGE_ENDPOINT} --credentials-file ${CERT_FILE} cp "${export_file}" "s3://${BUCKET_NAME}/${OBJ_NAME}"
fi
touch /mantle/uploaded
