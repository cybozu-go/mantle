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
TMPDIR := tmp

export MINIKUBE_HOME

.PHONY: setup
setup:
	$(MAKE) $(GINKGO)
	$(MAKE) $(KUBECTL)
	$(MAKE) $(MINIKUBE)
	$(MAKE) $(HELM)
	mkdir -p $(TMPDIR)

.PHONY: test
test:
	$(MAKE) launch-minikube MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_PRIMARY)
	$(MAKE) install-rook-ceph-operator
	$(MAKE) install-rook-ceph-cluster1
	$(MAKE) install-rook-ceph-cluster2
	$(MAKE) install-mantle-cluster-wide
	$(MAKE) install-mantle \
		NAMESPACE=$(CEPH_CLUSTER1_NAMESPACE) \
		HELM_RELEASE=mantle \
		VALUES_YAML=testdata/values-mantle1.yaml
	$(MAKE) install-mantle \
		NAMESPACE=$(CEPH_CLUSTER2_NAMESPACE) \
		HELM_RELEASE=mantle2 \
		VALUES_YAML=testdata/values-mantle2.yaml
	$(MAKE) do_test

.PHONY: test-multiple-k8s-clusters
test-multiple-k8s-clusters:
# set up a k8s cluster for secondary mantle
	$(MAKE) launch-minikube MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_SECONDARY)
	$(MAKE) install-rook-ceph-operator
	$(MAKE) install-rook-ceph-cluster1
	$(MAKE) install-ceph-object-store
	$(MAKE) install-mantle-cluster-wide
	sed \
		-e "s%{OBJECT_STORAGE_BUCKET_NAME}%$$(cat $(TMPDIR)/cm-export-data.json | jq -r .data.BUCKET_NAME)%" \
		-e "s%{OBJECT_STORAGE_ENDPOINT}%http://$$(cat $(TMPDIR)/cm-export-data.json | jq -r .data.BUCKET_HOST)%" \
		testdata/values-mantle-secondary-template.yaml \
		> testdata/values-mantle-secondary.yaml
	$(MAKE) install-mantle \
		NAMESPACE=$(CEPH_CLUSTER1_NAMESPACE) \
		HELM_RELEASE=mantle \
		VALUES_YAML=testdata/values-mantle-secondary.yaml
# set up a k8s cluster for primary mantle
	$(MAKE) launch-minikube MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_PRIMARY)
	$(MAKE) install-rook-ceph-operator
	$(MAKE) install-rook-ceph-cluster1
	$(MAKE) install-ceph-object-store-secret
	$(MAKE) install-mantle-cluster-wide
	sed \
		-e "s%{ENDPOINT}%$$($(MINIKUBE) service list -p $(MINIKUBE_PROFILE_SECONDARY) -o json | jq -r '.[] | select(.Name == "mantle" and .Namespace == "rook-ceph") | .URLs | select(. | length > 0)[]' | head -1 | sed -r 's/^http:\/\///')%" \
		-e "s%{OBJECT_STORAGE_BUCKET_NAME}%$$(cat $(TMPDIR)/cm-export-data.json | jq -r .data.BUCKET_NAME)%" \
		-e "s%{OBJECT_STORAGE_ENDPOINT}%$$($(MINIKUBE) service list -p $(MINIKUBE_PROFILE_SECONDARY) -o json | jq -r '.[] | select(.Name == "rook-ceph-rgw-ceph-object-store-node-port" and .Namespace == "rook-ceph") | .URLs | select(. | length > 0)[]' | head -1)%" \
		testdata/values-mantle-primary-template.yaml \
		> testdata/values-mantle-primary.yaml
	$(MAKE) install-mantle \
		NAMESPACE=$(CEPH_CLUSTER1_NAMESPACE) \
		HELM_RELEASE=mantle \
		VALUES_YAML=testdata/values-mantle-primary.yaml
# start testing
	$(MINIKUBE) profile $(MINIKUBE_PROFILE_PRIMARY)
	$(MAKE) do-test-multik8s

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

.PHONY: launch-minikube
launch-minikube: MINIKUBE_PROFILE=
launch-minikube:
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
	$(MAKE) create-loop-dev
	sed \
		-e "s%{LOOP_DEV}%$(LOOP_DEV)%" \
		-e "s%{LOOP_DEV2}%$(LOOP_DEV2)%" \
		-e "s%{NODE_NAME}%$(NODE_NAME)%" \
		testdata/persistentvolumes-template.yaml \
		> testdata/persistentvolumes.yaml
	$(KUBECTL) apply -f testdata/persistentvolumes.yaml

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

.PHONY: install-rook-ceph-operator
install-rook-ceph-operator:
	$(HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
		--create-namespace --namespace $(CEPH_CLUSTER1_NAMESPACE) -f testdata/values.yaml --wait \
		rook-ceph rook-ceph
	$(MAKE) wait-deploy-ready NS=$(CEPH_CLUSTER1_NAMESPACE) DEPLOY=rook-ceph-operator

.PHONY: install-rook-ceph-cluster1
install-rook-ceph-cluster1:
	$(HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
		--namespace $(CEPH_CLUSTER1_NAMESPACE) -f testdata/values-cluster.yaml \
		--wait rook-ceph-cluster rook-ceph-cluster
	$(MAKE) wait-deploy-ready NS=$(CEPH_CLUSTER1_NAMESPACE) DEPLOY=rook-ceph-osd-0

.PHONY: install-rook-ceph-cluster2
install-rook-ceph-cluster2:
	$(HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
		--create-namespace --namespace $(CEPH_CLUSTER2_NAMESPACE) -f testdata/values-cluster.yaml \
		--set cephClusterSpec.dataDirHostPath=/var/lib/rook2 \
		--wait rook-ceph-cluster2 rook-ceph-cluster
	$(MAKE) wait-deploy-ready NS=$(CEPH_CLUSTER2_NAMESPACE) DEPLOY=rook-ceph-osd-0

.PHONY: install-ceph-object-store
install-ceph-object-store:
	$(KUBECTL) apply -f testdata/objectstorage.yaml
	until $(KUBECTL) get -n rook-ceph cm export-data &> /dev/null ; do sleep 3 ; done
	$(KUBECTL) get -n rook-ceph configmap export-data -o json | jq -r '{ apiVersion: "v1", kind: "ConfigMap", metadata: { name: "export-data", namespace: "rook-ceph" }, data: .data }' > $(TMPDIR)/cm-export-data.json
	until $(KUBECTL) get -n rook-ceph secret export-data &> /dev/null ; do sleep 3 ; done
	$(KUBECTL) get -n rook-ceph secret export-data -o json | jq -r '{ apiVersion: "v1", kind: "Secret", metadata: { name: "export-data", namespace: "rook-ceph" }, type: "Opaque", data: .data }' > $(TMPDIR)/secret-export-data.json

.PHONY: install-ceph-object-store-secret
install-ceph-object-store-secret:
	$(KUBECTL) apply -f $(TMPDIR)/secret-export-data.json

.PHONY: image-build
image-build:
	eval $$($(MINIKUBE) docker-env); \
	$(MAKE) -C ../.. docker-build
	$(MINIKUBE) ssh -- docker images

.PHONY: install-mantle-cluster-wide
install-mantle-cluster-wide:
	$(HELM) upgrade --install mantle-cluster-wide ../../charts/mantle-cluster-wide/ --wait

.PHONY: install-mantle
install-mantle: NAMESPACE=
install-mantle: HELM_RELEASE=
install-mantle: VALUES_YAML=
install-mantle:
	$(HELM) upgrade --install --namespace=$(NAMESPACE) $(HELM_RELEASE) ../../charts/mantle/ --wait -f $(VALUES_YAML)
	$(KUBECTL) rollout restart -n $(NAMESPACE) deploy/$(HELM_RELEASE)-controller

.PHONY: do_test
do_test: $(GINKGO)
	env \
	PATH=${PATH} \
	E2ETEST=1 \
	KUBECTL=$(KUBECTL) \
	$(GINKGO) --fail-fast -v $(GINKGO_FLAGS) singlek8s

.PHONY: do-test-multik8s
do-test-multik8s: $(GINKGO)
	env \
	PATH=${PATH} \
	E2ETEST=1 \
	KUBECTL_PRIMARY="$(MINIKUBE) -p $(MINIKUBE_PROFILE_PRIMARY) kubectl -- " \
	KUBECTL_SECONDARY="$(MINIKUBE) -p $(MINIKUBE_PROFILE_SECONDARY) kubectl -- " \
	$(GINKGO) --fail-fast -v $(GINKGO_FLAGS) multik8s
