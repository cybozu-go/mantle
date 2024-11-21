# Build the manager binary
FROM golang:1.22 AS builder

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
FROM golang:1.22 AS s5cmd-builder
WORKDIR /workspace
## https://github.com/peak/s5cmd/releases
## use v2.2.2
RUN go install github.com/peak/s5cmd/v2@48f7e59e2d02954e218d2ddb947566d57e495fd8

FROM quay.io/cybozu/ceph:18.2.1.1
WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=s5cmd-builder /go/bin/s5cmd /usr/bin/s5cmd
USER 65532:65532

ENTRYPOINT ["/manager"]
