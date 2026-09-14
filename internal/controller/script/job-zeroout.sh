#!/bin/bash

set -e
set -o pipefail

# Key of the RBD image metadata that holds the name of the snapshot the image
# head is identical to. See docs/design.md and job-import.sh for the details.
CLEAN_SNAP_KEY="mantle.clean-snap"

# rbd reads the file given to --keyfile with fstat(2) and read(2), so it must
# be a regular file: a pipe such as a process substitution has a size of 0 and
# thus is read as an empty key, which silently disables cephx.
# cf. bufferlist::read_file() called from md_config_t::parse_argv()
KEYFILE="$(mktemp)"
trap 'rm -f "${KEYFILE}"' EXIT
printf '%s' "${ROOK_CEPH_SECRET}" > "${KEYFILE}"

# filter out the mon names, as job-import.sh does for /etc/rook/mon-endpoints
# shellcheck disable=SC2001
MON_HOST=$(echo "${MON_ENDPOINTS}" | sed 's/[a-z0-9_-]\+=//g')

rbd_admin() {
    rbd --mon-host "${MON_HOST}" --name "${ROOK_CEPH_USERNAME}" --keyfile "${KEYFILE}" "$@"
}

# has_clean_snap reports whether the image has the clean snapshot metadata.
#
# It terminates this script if rbd or jq fails, instead of returning the error
# to the caller, because this function is called in a condition list, where
# set -e is disabled and thus a failure would silently be taken as the absence
# of the metadata. We must not zero out the image in that case, because a
# leftover key would make a later import Job skip a rollback that is actually
# necessary.
has_clean_snap() {
    local metadata result

    metadata=$(rbd_admin image-meta list --format json "${POOL_NAME}/${DST_IMAGE_NAME}") || exit $?

    # rbd prints nothing if the image has no metadata at all, which is not an
    # error here. Note that the exit status of jq -e cannot tell such an empty
    # input from an invalid one, because jq 1.6 exits with 0 for the former and
    # with 4 for the latter, while jq 1.7 and later do the opposite. So we look
    # at the printed value instead, and treat everything else as a failure.
    if [ -z "${metadata}" ]; then
        return 1
    fi
    result=$(printf '%s' "${metadata}" |
        jq --arg key "${CLEAN_SNAP_KEY}" 'has($key)') || result=""
    case "${result}" in
        true) return 0 ;;
        false) return 1 ;;
    esac

    echo "failed to check whether ${CLEAN_SNAP_KEY} exists" >&2
    exit 1
}

# The image head is modified below, so the recorded state of the head must be
# invalidated first.
if has_clean_snap; then
    echo "start invalidating ${CLEAN_SNAP_KEY}"
    rbd_admin image-meta remove "${POOL_NAME}/${DST_IMAGE_NAME}" "${CLEAN_SNAP_KEY}"
    echo "finish invalidating ${CLEAN_SNAP_KEY}"
fi

blkdiscard -z /dev/zeroout-rbd
