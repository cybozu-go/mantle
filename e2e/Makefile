include ../versions.mk

BINDIR := $(shell pwd)/bin
TMPDIR := $(shell pwd)/tmp
CURL := curl -sSLf
GINKGO := $(BINDIR)/ginkgo-$(GINKGO_VERSION)
GINKGO_FLAGS :=
MINIKUBE := $(BINDIR)/minikube
KUBECTL := $(BINDIR)/kubectl-$(KUBERNETES_VERSION)
MINIKUBE_HOME = $(BINDIR)
NODE_NAME := minikube-worker
SUDO := sudo
DD := dd
LOSETUP := losetup
LOOP_FILE := $(TMPDIR)/loop.img
LOOP_DEV := $(shell loop_dev=$$($(LOSETUP) | grep $(LOOP_FILE) | cut -d ' ' -f 1); \
		if [ -z "$$loop_dev" ]; then \
			$(LOSETUP) -f; \
		else \
			echo $$loop_dev; \
		fi)
CNI_PLUGIN_INSTALL_DIR := /opt/cni/bin

export MINIKUBE_HOME

.PHONY: setup
setup:
	$(MAKE) $(GINKGO)
	$(MAKE) $(KUBECTL)
	$(MAKE) $(MINIKUBE)

.PHONY: test
test:
	$(MAKE) launch-minikube
	$(MAKE) common/setup-components
	$(MAKE) launch-rook-ceph
	$(MAKE) common/test

.PHONY: clean
clean:
	$(MAKE) delete-rook-ceph
	$(MAKE) delete-minikube

$(BINDIR):
	mkdir -p $@

$(TMPDIR):
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

.PHONY: launch-minikube
launch-minikube:
	# TODO: Is there any better way to verify whether k8s cluster is available or not?
	kubectl get pod >/dev/null 2>&1; \
	RET=$$? ; \
	if [ $$RET -eq 0 ] ; then exit; fi ; \
	$(SUDO) -E $(MINIKUBE) start \
		--kubernetes-version="v$(KUBERNETES_VERSION)" \
		--driver=none \
		--memory 6g \
		--cpus=2 \
		--addons ingress \
		--cni=calico \
		--extra-config=kubeadm.node-name=$(NODE_NAME) \
		--extra-config=kubelet.hostname-override=$(NODE_NAME)
	$(SUDO) chown -R $$USER $$HOME/.kube $(MINIKUBE_HOME)/.minikube
	$(SUDO) chmod -R a+r $$HOME/.kube $(MINIKUBE_HOME)/.minikube
	$(SUDO) find $(MINIKUBE_HOME)/.minikube -name id_rsa -exec chmod 600 {} ';'

.PHONY: delete-minikube
delete-minikube:
	$(SUDO) -E $(MINIKUBE) delete || true
	# Is it really OK to remove these files?
	rm -rf /tmp/juju-*

.PHONY: create-loop-dev
create-loop-dev: | $(TMPDIR)
	$(LOSETUP) | grep -q $(LOOP_FILE); \
	RET=$$? ; \
	if [ $$RET -ne 0 ]; then \
		$(DD) if=/dev/zero of=$(LOOP_FILE) bs=1G seek=32 count=0; \
		$(SUDO) $(LOSETUP) $(LOOP_DEV) $(LOOP_FILE); \
	fi

.PHONY: delete-loop-dev
delete-loop-dev: | $(TMPDIR)
	$(SUDO) $(LOSETUP) -d $(LOOP_DEV)
	$(SUDO) rm -f $(LOOP_FILE)

.PHONY: launch-rook-ceph
launch-rook-ceph: create-loop-dev
	$(KUBECTL) apply -f testdata/common.yaml
	$(KUBECTL) apply -f testdata/crds.yaml
	$(KUBECTL) apply -f testdata/operator.yaml
	sed -e "s%LOOP_DEV%$(LOOP_DEV)%" -e "s%NODE_NAME%$(NODE_NAME)%" testdata/cluster-template.yaml > testdata/cluster.yaml
	$(KUBECTL) apply -f testdata/cluster.yaml
	$(KUBECTL) apply -f testdata/toolbox.yaml

.PHONY: delete-rook-ceph
delete-rook-ceph:
	$(KUBECTL) delete -f testdata/toolbox.yaml
	$(KUBECTL) delete -f testdata/cluster.yaml
	$(SUDO) rm -rf /var/lib/rook
	ls /dev/mapper/ceph-* | xargs -I% -- $(SUDO) dmsetup remove %
	$(SUDO) rm -rf /dev/mapper/ceph-*
	$(KUBECTL) delete -f testdata/operator.yaml
	$(KUBECTL) delete -f testdata/crds.yaml
	$(KUBECTL) delete -f testdata/common.yaml
	$(MAKE) delete-loop-dev

.PHONY: common/setup-components
common/setup-components:
	$(KUBECTL) apply -f https://github.com/cert-manager/cert-manager/releases/download/$(CERT_MANAGER_VERSION)/cert-manager.yaml
	$(MAKE) -C .. docker-build
	# We can't use `minikube image load` due to a minikube's problem.
	# ref. https://github.com/kubernetes/minikube/issues/17785
	docker save controller:latest | $(SUDO) ctr -n k8s.io images import -

	$(MAKE) -C .. install
	$(MAKE) -C .. deploy

.PHONY: common/test
common/test: $(GINKGO)
	$(SUDO) -E env \
	PATH=${PATH} \
	E2ETEST=1 \
	KUBECTL=$(KUBECTL) \
	$(GINKGO) --fail-fast -v $(GINKGO_FLAGS) .
