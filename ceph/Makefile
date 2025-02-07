include ../versions.mk

PROJECT_DIR := $(shell cd ..; pwd)
E2E_DIR := $(PROJECT_DIR)/test/e2e
MINIKUBE_HOME := $(E2E_DIR)/bin

GO_FILES := $(shell find -name '*.go')
PATCH_FILES := $(shell find -name '*.patch')

KUBECTL_CMD := $(E2E_DIR)/bin/kubectl-$(KUBERNETES_VERSION)
MINIKUBE_CMD := $(E2E_DIR)/bin/minikube

export MINIKUBE_HOME

.PHONY: build
build: ceph-custom.tar packages/touch

ceph-custom.tar: Dockerfile $(PATCH_FILES)
	docker build -f Dockerfile -t ceph-custom .
	docker save ceph-custom -o $@

packages/touch: ceph-custom.tar
	rm -rf packages
	docker create --name ceph-proc ceph-custom
	docker cp ceph-proc:/packages .
	docker rm ceph-proc
	touch $@

.PHONY: test
test: ceph-custom.tar
	$(MAKE) -C ${E2E_DIR} launch-minikube MINIKUBE_PROFILE=minikube
	$(MAKE) -C ${E2E_DIR} install-rook-ceph-operator
	$(MAKE) -C ${E2E_DIR} install-rook-ceph-cluster1
	# $(MINIKUBE_CMD) image load ceph-custom.tar
	# $(KUBECTL_CMD) apply -k ./manifests/
	env \
	  CEPH_CMD_TEST=1 \
	  KUBECTL=$(KUBECTL_CMD) \
	  go test -v -count=1 ./test/

.PHONY: setup
setup:
	$(MAKE) -C $(E2E_DIR) setup

.PHONY: clean
clean:
	rm -rf packages ceph-custom.tar
	$(MAKE) -C $(E2E_DIR) clean
