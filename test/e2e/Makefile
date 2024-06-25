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

export MINIKUBE_HOME

.PHONY: setup
setup:
	$(MAKE) $(GINKGO)
	$(MAKE) $(KUBECTL)
	$(MAKE) $(MINIKUBE)
	$(MAKE) $(HELM)

.PHONY: test
test:
	$(MAKE) launch-minikube
	$(MAKE) image-build
	$(MAKE) launch-rook-ceph
	$(MAKE) setup-components
	$(MAKE) do_test

.PHONY: clean
clean:
	$(MAKE) delete-components
	$(MAKE) delete-rook-ceph
	$(MAKE) delete-minikube

$(BINDIR):
	mkdir -p $@

$(GINKGO): | $(BINDIR)
	GOBIN=$(BINDIR) go install github.com/onsi/ginkgo/v2/ginkgo@$(GINKGO_VERSION)
	mv $(BINDIR)/ginkgo $@

$(KUBECTL): | $(BINDIR)
	$(CURL) -o $@ https://storage.googleapis.com/kubernetes-release/release/v$(KUBERNETES_VERSION)/bin/linux/amd64/kubectl
	chmod a+x $@

$(MINIKUBE): | $(BINDIR)
	$(SUDO) apt update
	$(SUDO) apt install -y conntrack socat
	$(CURL) -o $@ https://github.com/kubernetes/minikube/releases/download/$(MINIKUBE_VERSION)/minikube-linux-amd64
	chmod a+x $@

	$(CURL) -o cri-dockerd.deb https://github.com/Mirantis/cri-dockerd/releases/download/$(CRI_DOCKERD_VERSION)/cri-dockerd_$(CRI_DOCKERD_VERSION:v%=%).3-0.ubuntu-focal_amd64.deb
	$(SUDO) dpkg -i cri-dockerd.deb
	rm -f cri-dockerd.deb

	$(CURL) -o crictl.tar.gz https://github.com/kubernetes-sigs/cri-tools/releases/download/$(CRICTL_VERSION)/crictl-$(CRICTL_VERSION)-linux-amd64.tar.gz
	$(SUDO) tar zxvf crictl.tar.gz -C /usr/local/bin
	rm -f crictl.tar.gz
	$(SUDO) sysctl fs.protected_regular=0

	$(CURL) -o cni-plugins.tgz https://github.com/containernetworking/plugins/releases/download/$(CNI_PLUGINS_VERSION)/cni-plugins-linux-amd64-$(CNI_PLUGINS_VERSION).tgz
	$(SUDO) mkdir -p $(CNI_PLUGIN_INSTALL_DIR)
	$(SUDO) tar -xf cni-plugins.tgz -C $(CNI_PLUGIN_INSTALL_DIR)
	rm -f cni-plugins.tgz

$(HELM): | $(BINDIR)
	$(CURL) https://get.helm.sh/helm-v$(HELM_VERSION)-linux-amd64.tar.gz \
		| tar xvz -C $(BINDIR) --strip-components 1 linux-amd64/helm

.PHONY: launch-minikube
launch-minikube:
	# TODO: Is there any better way to verify whether k8s cluster is available or not?
	kubectl get pod >/dev/null 2>&1; \
	RET=$$? ; \
	if [ $$RET -eq 0 ] ; then exit; fi ; \
	$(MINIKUBE) start \
		--kubernetes-version="v$(KUBERNETES_VERSION)" \
		--driver=kvm2 \
		--memory 6g \
		--cpus=2 \
		--extra-config=kubeadm.node-name=$(NODE_NAME) \
		--extra-config=kubelet.hostname-override=$(NODE_NAME)

.PHONY: delete-minikube
delete-minikube:
	$(MINIKUBE) stop || true
	$(MINIKUBE) delete || true

.PHONY: create-loop-dev
create-loop-dev:
	$(MINIKUBE) ssh -- $(DD) if=/dev/zero of=$(LOOP_FILE) bs=1G seek=32 count=0
	$(MINIKUBE) ssh -- $(SUDO) $(LOSETUP) $(LOOP_DEV) $(LOOP_FILE) || :
	$(MINIKUBE) ssh $(DD) if=/dev/zero of=$(LOOP_FILE2) bs=1G seek=32 count=0
	$(MINIKUBE) ssh -- $(SUDO) $(LOSETUP) $(LOOP_DEV2) $(LOOP_FILE2) || :
	$(MINIKUBE) ssh -- lsblk

.PHONY: delete-loop-dev
delete-loop-dev:
	$(MINIKUBE) ssh -- $(SUDO) $(LOSETUP) -D || :
	$(MINIKUBE) ssh -- $(SUDO) rm -f $(LOOP_DEV) $(LOOP_DEV2)

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
		--namespace $(CEPH_CLUSTER1_NAMESPACE) -f testdata/values-cluster.yaml --wait rook-ceph-cluster rook-ceph-cluster
	$(HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
		--create-namespace --namespace $(CEPH_CLUSTER2_NAMESPACE) -f testdata/values-cluster.yaml \
		--set cephClusterSpec.dataDirHostPath=/var/lib/rook2 \
		--wait rook-ceph-cluster2 rook-ceph-cluster
	$(MAKE) wait-deploy-ready NS=$(CEPH_CLUSTER1_NAMESPACE) DEPLOY=rook-ceph-operator
	$(MAKE) wait-deploy-ready NS=$(CEPH_CLUSTER1_NAMESPACE) DEPLOY=rook-ceph-osd-0
	$(MAKE) wait-deploy-ready NS=$(CEPH_CLUSTER2_NAMESPACE) DEPLOY=rook-ceph-osd-0

.PHONY: delete-rook-ceph
delete-rook-ceph:
	$(HELM) uninstall --namespace $(CEPH_CLUSTER2_NAMESPACE) rook-ceph-cluster2 --wait || :
	$(HELM) uninstall --namespace $(CEPH_CLUSTER1_NAMESPACE) rook-ceph-cluster --wait || :
	$(SUDO) rm -rf /var/lib/rook
	$(SUDO) rm -rf /var/lib/rook2
	ls /dev/mapper/ceph-* | xargs -I% -- $(SUDO) dmsetup remove %
	$(SUDO) rm -rf /dev/mapper/ceph-*
	$(HELM) uninstall --namespace $(CEPH_CLUSTER1_NAMESPACE) rook-ceph --wait || :
	$(KUBECTL) delete -f testdata/persistentvolumes.yaml || :
	$(MAKE) delete-loop-dev

.PHONY: image-build
image-build:
	eval $$($(MINIKUBE) docker-env); \
	$(MAKE) -C ../.. docker-build
	$(MINIKUBE) ssh -- docker images

.PHONY: setup-components
setup-components:
	$(HELM) upgrade --install mantle-cluster-wide ../../charts/mantle-cluster-wide/ --wait
	$(HELM) upgrade --install --namespace=$(CEPH_CLUSTER1_NAMESPACE) mantle ../../charts/mantle/ --wait
	$(HELM) upgrade --install --namespace=$(CEPH_CLUSTER2_NAMESPACE) mantle2 ../../charts/mantle/ --wait

.PHONY: delete-components
delete-components:
	$(HELM) uninstall --namespace=$(CEPH_CLUSTER2_NAMESPACE) mantle2 --wait || true
	$(HELM) uninstall --namespace=$(CEPH_CLUSTER1_NAMESPACE) mantle --wait || true
	$(HELM) uninstall mantle-cluster-wide --wait || true

.PHONY: do_test
do_test: $(GINKGO)
	env \
	PATH=${PATH} \
	E2ETEST=1 \
	KUBECTL=$(KUBECTL) \
	$(GINKGO) --fail-fast -v $(GINKGO_FLAGS) .
