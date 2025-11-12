# https://github.com/kubernetes-sigs/controller-tools/releases
CONTROLLER_TOOLS_VERSION := v0.18.0
# https://github.com/helm/helm/releases
HELM_VERSION := 3.18.2
# It is set by CI using the environment variable, use conditional assignment.
KUBERNETES_VERSION := 1.33.1
# https://github.com/kubernetes-sigs/kustomize/releases
KUSTOMIZE_VERSION := v5.6.0
# https://github.com/kubernetes/minikube/releases
MINIKUBE_VERSION := v1.36.0
# https://github.com/protocolbuffers/protobuf/releases
PROTOC_VERSION := 31.1
# https://github.com/rook/rook/releases
ROOK_CHART_VERSION := v1.18.6
# https://github.com/golangci/golangci-lint/releases
GOLANGCI_LINT_VERSION := v2.1.6
# https://github.com/cert-manager/cert-manager/releases
CERT_MANAGER_VERSION := v1.17.2

# Tools versions which are defined in go.mod
SELF_DIR := $(dir $(lastword $(MAKEFILE_LIST)))
CONTROLLER_RUNTIME_VERSION := $(shell awk '$$1 == "sigs.k8s.io/controller-runtime" {print substr($$2, 2)}' $(SELF_DIR)/go.mod)
GINKGO_VERSION := $(shell awk '$$1 == "github.com/onsi/ginkgo/v2" {print $$2}' $(SELF_DIR)/go.mod)
PROTOC_GEN_DOC_VERSION := $(shell awk '$$1 == "github.com/pseudomuto/protoc-gen-doc" {print substr($$2, 2)}' $(SELF_DIR)/go.mod)
PROTOC_GEN_GO_GRPC_VERSION := $(shell awk '$$1 == "google.golang.org/grpc/cmd/protoc-gen-go-grpc" {print substr($$2, 2)}' $(SELF_DIR)/go.mod)
PROTOC_GEN_GO_VERSION := $(shell awk '$$1 == "google.golang.org/protobuf" {print substr($$2, 2)}' $(SELF_DIR)/go.mod)
MOCKGEN_VERSION := $(shell awk '$$1 == "go.uber.org/mock" {print $$2}' $(SELF_DIR)/go.mod)

ENVTEST_KUBERNETES_VERSION := $(shell echo $(KUBERNETES_VERSION) | cut -d "." -f 1-2).0
