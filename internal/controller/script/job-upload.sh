#!/bin/bash

set -e
if [ -e "/mantle/uploaded" ]; then
  exit
fi
if [ "${CERT_FILE}" = "" ]; then
  s5cmd --endpoint-url ${OBJECT_STORAGE_ENDPOINT} cp /mantle/export-${PART_NUM}.bin "s3://${BUCKET_NAME}/${OBJ_NAME}"
else
  s5cmd --endpoint-url ${OBJECT_STORAGE_ENDPOINT} --credentials-file ${CERT_FILE} cp /mantle/export-${PART_NUM}.bin "s3://${BUCKET_NAME}/${OBJ_NAME}"
fi
touch /mantle/uploaded
