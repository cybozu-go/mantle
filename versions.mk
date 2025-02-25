# https://github.com/kubernetes-sigs/controller-tools/releases
CONTROLLER_TOOLS_VERSION := v0.16.4
# https://github.com/helm/helm/releases
HELM_VERSION := 3.17.1
# It is set by CI using the environment variable, use conditional assignment.
KUBERNETES_VERSION := 1.31.1
# https://github.com/kubernetes-sigs/kustomize/releases
KUSTOMIZE_VERSION := v5.6.0
# https://github.com/kubernetes/minikube/releases
MINIKUBE_VERSION := v1.35.0
# https://github.com/uber-go/mock/releases
MOCKGEN_VERSION := v0.5.0
# https://github.com/protocolbuffers/protobuf/releases
PROTOC_VERSION := 29.3
# https://github.com/rook/rook/releases
ROOK_CHART_VERSION := v1.16.4
# https://github.com/golangci/golangci-lint/releases
GOLANGCI_LINT_VERSION := v1.64.5

# Tools versions which are defined in go.mod
SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
CONTROLLER_RUNTIME_VERSION := $(shell awk '$$1 == "sigs.k8s.io/controller-runtime" {print substr($$2, 2)}' $(SELF_DIR)/go.mod)
GINKGO_VERSION := $(shell awk '$$1 == "github.com/onsi/ginkgo/v2" {print $$2}' $(SELF_DIR)/go.mod)
PROTOC_GEN_DOC_VERSION := $(shell awk '$$1 == "github.com/pseudomuto/protoc-gen-doc" {print substr($$2, 2)}' $(SELF_DIR)/go.mod)
PROTOC_GEN_GO_GRPC_VERSION := $(shell awk '$$1 == "google.golang.org/grpc/cmd/protoc-gen-go-grpc" {print substr($$2, 2)}' $(SELF_DIR)/go.mod)
PROTOC_GEN_GO_VERSION := $(shell awk '$$1 == "google.golang.org/protobuf" {print substr($$2, 2)}' $(SELF_DIR)/go.mod)

ENVTEST_BRANCH := release-$(shell echo $(CONTROLLER_RUNTIME_VERSION) | cut -d "." -f 1-2)
ENVTEST_KUBERNETES_VERSION := $(shell echo $(KUBERNETES_VERSION) | cut -d "." -f 1-2)
