# Build the manager binary
FROM golang:1.23 AS builder

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY main.go main.go
COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/
COPY pkg/ pkg/

# Build
# the GOARCH has not a default value to allow the binary be built according to the host where the command
# was called. For example, if we call make docker-build in a local env which has the Apple Silicon M1 SO
# the docker BUILDPLATFORM arg will be linux/arm64 when for Apple x86 it will be linux/amd64. Therefore,
# by leaving it empty we can ensure that the container and binary shipped on it will have the same platform.
# TODO: RUN CGO_ENABLED=0 go build -a -o manager cmd/main.go
RUN CGO_ENABLED=0 go build -o manager main.go

# Build s5cmd
FROM golang:1.23 AS s5cmd-builder
WORKDIR /workspace
## https://github.com/peak/s5cmd/releases
## use v2.3.0
RUN go install github.com/peak/s5cmd/v2@48f7e59e2d02954e218d2ddb947566d57e495fd8

# Download and install custom RBD command
# cf. https://github.com/cybozu/neco-containers/blob/a504569e337e41d3a9c7e0b6c0fc0482c2a31008/ceph/Dockerfile
FROM ubuntu:22.04 AS custom-ceph

ARG EXPORT_DIFF_VERSION=19.2.1.0

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
        libstdc++-11-dev jq kmod lvm2 gdisk ca-certificates e2fsprogs attr udev libgflags2.2 \
        curl unzip &&\
    cd /tmp && \
    curl -L -o packages.zip https://github.com/cybozu-go/mantle/releases/download/ceph-export-diff-v${EXPORT_DIFF_VERSION}/packages.zip && \
    unzip packages.zip && \
    mkdir -p /usr/local/share/doc/ceph && \
    cp /tmp/COPYING* /usr/local/share/doc/ceph && \
    mkdir -p /var/run/ceph && \
    apt-get install --no-install-recommends -y /tmp/*.deb && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /tmp/*.deb && \
    sed -i -e 's/udev_rules = 1/udev_rules = 0/' -e 's/udev_sync = 1/udev_sync = 0/' -e 's/obtain_device_list_from_udev = 1/obtain_device_list_from_udev = 0/' /etc/lvm/lvm.conf && \
    # validate the sed command worked as expected
    grep -sqo "udev_sync = 0" /etc/lvm/lvm.conf && \
    grep -sqo "udev_rules = 0" /etc/lvm/lvm.conf && \
    grep -sqo "obtain_device_list_from_udev = 0" /etc/lvm/lvm.conf && \
    # Clean common files like /tmp, /var/lib, etc.
    rm -rf \
        /etc/{selinux,systemd,udev} \
        /lib/{lsb,udev} \
        /tmp/* \
        /usr/lib{,64}/{locale,systemd,udev,dracut} \
        /usr/share/{doc,info,locale,man} \
        /usr/share/{bash-completion,pkgconfig/bash-completion.pc} \
        /var/log/* \
        /var/tmp/* && \
    find / -xdev \( -name "*.pyc" -o -name "*.pyo" \) -exec rm -f {} \; && \
    mkdir -p /usr/local/share/doc/ceph

# Squash the layers
FROM scratch

COPY --from=custom-ceph / /

WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=s5cmd-builder /go/bin/s5cmd /usr/bin/s5cmd
USER 65532:65532

ENTRYPOINT ["/manager"]
