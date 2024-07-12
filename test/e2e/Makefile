include ../../versions.mk

SHELL := /bin/bash
BINDIR := $(shell pwd)/bin
CURL := curl -sSLf
GINKGO := $(BINDIR)/ginkgo-$(GINKGO_VERSION)
GINKGO_FLAGS :=
MINIKUBE := $(BINDIR)/minikube
HELM := $(BINDIR)/helm
KUBECTL := $(BINDIR)/kubectl-$(KUBERNETES_VERSION)
MINIKUBE_HOME = $(BINDIR)
CEPH_CLUSTER1_NAMESPACE := rook-ceph
CEPH_CLUSTER2_NAMESPACE := rook-ceph2
NODE_NAME := minikube-worker
SUDO := sudo
DD := dd
LOSETUP := losetup
LOOP_FILE := loop.img
LOOP_FILE2 := loop2.img
CNI_PLUGIN_INSTALL_DIR := /opt/cni/bin
POLLING_INTERVAL := 1
TIMEOUT_SECS := 180
LOOP_DEV := /dev/loop0
LOOP_DEV2 := /dev/loop1
MINIKUBE_PROFILE_PRIMARY=profile1
MINIKUBE_PROFILE_SECONDARY=profile2

export MINIKUBE_HOME

.PHONY: setup
setup:
	$(MAKE) $(GINKGO)
	$(MAKE) $(KUBECTL)
	$(MAKE) $(MINIKUBE)
	$(MAKE) $(HELM)

.PHONY: test
test:
	$(MAKE) launch-cluster MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_PRIMARY)
	$(MINIKUBE) profile $(MINIKUBE_PROFILE_PRIMARY)
	$(MAKE) do_test

.PHONY: test-multiple-k8s-clusters
test-multiple-k8s-clusters:
	$(MAKE) launch-cluster MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_PRIMARY)
	$(MAKE) launch-cluster MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_SECONDARY)
	$(MINIKUBE) profile $(MINIKUBE_PROFILE_PRIMARY)
	env \
		MINIKUBE=$(MINIKUBE) \
		MINIKUBE_HOME=$(MINIKUBE_HOME) \
		MINIKUBE_PROFILE_PRIMARY=$(MINIKUBE_PROFILE_PRIMARY) \
		MINIKUBE_PROFILE_SECONDARY=$(MINIKUBE_PROFILE_SECONDARY) \
		./test-multiple-k8s-clusters.sh

.PHONY: clean
clean:
	$(MINIKUBE) delete --all || true

$(BINDIR):
	mkdir -p $@

$(GINKGO): | $(BINDIR)
	GOBIN=$(BINDIR) go install github.com/onsi/ginkgo/v2/ginkgo@$(GINKGO_VERSION)
	mv $(BINDIR)/ginkgo $@

$(KUBECTL): | $(BINDIR)
	$(CURL) -o $@ https://storage.googleapis.com/kubernetes-release/release/v$(KUBERNETES_VERSION)/bin/linux/amd64/kubectl
	chmod a+x $@

$(MINIKUBE): | $(BINDIR)
	$(CURL) -o $@ https://github.com/kubernetes/minikube/releases/download/$(MINIKUBE_VERSION)/minikube-linux-amd64
	chmod a+x $@

$(HELM): | $(BINDIR)
	$(CURL) https://get.helm.sh/helm-v$(HELM_VERSION)-linux-amd64.tar.gz \
		| tar xvz -C $(BINDIR) --strip-components 1 linux-amd64/helm

.PHONY: launch-cluster
launch-cluster: MINIKUBE_PROFILE=
launch-cluster:
	# TODO: Is there any better way to verify whether k8s cluster is available or not?
	if $(MINIKUBE) profile $(MINIKUBE_PROFILE) |& grep "not found" > /dev/null; then \
		$(MINIKUBE) start \
			--kubernetes-version="v$(KUBERNETES_VERSION)" \
			--driver=kvm2 \
			--memory 6g \
			--cpus=2 \
			--extra-config=kubeadm.node-name=$(NODE_NAME) \
			--extra-config=kubelet.hostname-override=$(NODE_NAME) \
			--network mantle-test \
			-p $(MINIKUBE_PROFILE) ; \
	fi
	$(MINIKUBE) profile $(MINIKUBE_PROFILE)
	$(MAKE) image-build
	$(MAKE) launch-rook-ceph
	$(MAKE) setup-components

.PHONY: create-loop-dev
create-loop-dev:
	$(MINIKUBE) ssh -- $(DD) if=/dev/zero of=$(LOOP_FILE) bs=1G seek=32 count=0
	$(MINIKUBE) ssh -- $(SUDO) $(LOSETUP) $(LOOP_DEV) $(LOOP_FILE) || :
	$(MINIKUBE) ssh $(DD) if=/dev/zero of=$(LOOP_FILE2) bs=1G seek=32 count=0
	$(MINIKUBE) ssh -- $(SUDO) $(LOSETUP) $(LOOP_DEV2) $(LOOP_FILE2) || :
	$(MINIKUBE) ssh -- lsblk

.PHONY: wait-deploy-ready
wait-deploy-ready: NS=
wait-deploy-ready: DEPLOY=
wait-deploy-ready:
	is_ok="false"; \
	for ((i=0;i<$(TIMEOUT_SECS);i+=$(POLLING_INTERVAL))); do \
		available_replicas=$$($(KUBECTL) -n $(CEPH_CLUSTER1_NAMESPACE) get deploy $(DEPLOY) -o json | jq -r ".status.availableReplicas"); \
		if [ "$$available_replicas" = 1 ]; then \
			is_ok="true"; \
			break; \
		fi; \
		echo "waiting for deploy $(DEPLOY) to be available" > /dev/stderr; \
		sleep $(POLLING_INTERVAL); \
	done; \
	if [ "$$is_ok" = "false" ]; then \
		echo "failed to start deploy $(DEPLOY)" > /dev/stderr; \
		exit 1; \
	fi

.PHONY: launch-rook-ceph
launch-rook-ceph: create-loop-dev
	$(HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
		--create-namespace --namespace $(CEPH_CLUSTER1_NAMESPACE) -f testdata/values.yaml --wait \
		rook-ceph rook-ceph
	sed \
		-e "s%{LOOP_DEV}%$(LOOP_DEV)%" \
		-e "s%{LOOP_DEV2}%$(LOOP_DEV2)%" \
		-e "s%{NODE_NAME}%$(NODE_NAME)%" \
		testdata/persistentvolumes-template.yaml \
		> testdata/persistentvolumes.yaml
	$(KUBECTL) apply -f testdata/persistentvolumes.yaml
	$(HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
		--namespace $(CEPH_CLUSTER1_NAMESPACE) -f testdata/values-cluster.yaml \
		--wait rook-ceph-cluster rook-ceph-cluster
	$(HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
		--create-namespace --namespace $(CEPH_CLUSTER2_NAMESPACE) -f testdata/values-cluster.yaml \
		--set cephClusterSpec.dataDirHostPath=/var/lib/rook2 \
		--wait rook-ceph-cluster2 rook-ceph-cluster
	$(MAKE) wait-deploy-ready NS=$(CEPH_CLUSTER1_NAMESPACE) DEPLOY=rook-ceph-operator
	$(MAKE) wait-deploy-ready NS=$(CEPH_CLUSTER1_NAMESPACE) DEPLOY=rook-ceph-osd-0
	$(MAKE) wait-deploy-ready NS=$(CEPH_CLUSTER2_NAMESPACE) DEPLOY=rook-ceph-osd-0

.PHONY: image-build
image-build:
	eval $$($(MINIKUBE) docker-env); \
	$(MAKE) -C ../.. docker-build
	$(MINIKUBE) ssh -- docker images

.PHONY: setup-components
setup-components:
	$(HELM) upgrade --install mantle-cluster-wide ../../charts/mantle-cluster-wide/ --wait
	$(HELM) upgrade --install --namespace=$(CEPH_CLUSTER1_NAMESPACE) mantle ../../charts/mantle/ --wait -f testdata/values-mantle.yaml
	$(HELM) upgrade --install --namespace=$(CEPH_CLUSTER2_NAMESPACE) mantle2 ../../charts/mantle/ --wait
	$(KUBECTL) rollout restart -n $(CEPH_CLUSTER1_NAMESPACE) deploy/mantle-controller
	$(KUBECTL) rollout restart -n $(CEPH_CLUSTER2_NAMESPACE) deploy/mantle2-controller

.PHONY: do_test
do_test: $(GINKGO)
	env \
	PATH=${PATH} \
	E2ETEST=1 \
	KUBECTL=$(KUBECTL) \
	$(GINKGO) --fail-fast -v $(GINKGO_FLAGS) .
