# https://github.com/kubernetes-sigs/controller-tools/releases
CONTROLLER_TOOLS_VERSION := v0.16.4
# https://github.com/helm/helm/releases
HELM_VERSION := 3.16.2
# It is set by CI using the environment variable, use conditional assignment.
KUBERNETES_VERSION := 1.31.1
# https://github.com/kubernetes-sigs/kustomize/releases
KUSTOMIZE_VERSION := v5.5.0
# https://github.com/kubernetes/minikube/releases
MINIKUBE_VERSION := v1.34.0
# https://github.com/uber-go/mock/releases
MOCKGEN_VERSION := v0.5.0
# https://github.com/protocolbuffers/protobuf/releases
PROTOC_VERSION := 28.3
# https://github.com/rook/rook/releases
ROOK_CHART_VERSION := v1.15.4
# https://github.com/golangci/golangci-lint/releases
GOLANGCI_LINT_VERSION := v1.61.0

# Tools versions which are defined in go.mod
SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
CONTROLLER_RUNTIME_VERSION := $(shell awk '$$1 == "sigs.k8s.io/controller-runtime" {print substr($$2, 2)}' $(SELF_DIR)/go.mod)
GINKGO_VERSION := $(shell awk '$$1 == "github.com/onsi/ginkgo/v2" {print $$2}' $(SELF_DIR)/go.mod)
PROTOC_GEN_DOC_VERSION := $(shell awk '$$1 == "github.com/pseudomuto/protoc-gen-doc" {print substr($$2, 2)}' $(SELF_DIR)/go.mod)
PROTOC_GEN_GO_GRPC_VERSION := $(shell awk '$$1 == "google.golang.org/grpc/cmd/protoc-gen-go-grpc" {print substr($$2, 2)}' $(SELF_DIR)/go.mod)
PROTOC_GEN_GO_VERSION := $(shell awk '$$1 == "google.golang.org/protobuf" {print substr($$2, 2)}' $(SELF_DIR)/go.mod)

ENVTEST_BRANCH := release-$(shell echo $(CONTROLLER_RUNTIME_VERSION) | cut -d "." -f 1-2)
ENVTEST_KUBERNETES_VERSION := $(shell echo $(KUBERNETES_VERSION) | cut -d "." -f 1-2)

# CSI sidecar versions
EXTERNAL_PROVISIONER_VERSION := 4.0.0
EXTERNAL_RESIZER_VERSION := 1.9.3
EXTERNAL_SNAPSHOTTER_VERSION := 6.3.3
LIVENESSPROBE_VERSION := 2.12.0
NODE_DRIVER_REGISTRAR_VERSION := 2.10.0
