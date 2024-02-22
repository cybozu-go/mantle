include ../versions.mk

BINDIR := $(shell pwd)/bin
TMPDIR := $(shell pwd)/tmp
CURL := curl -sSLf
GINKGO := $(BINDIR)/ginkgo-$(GINKGO_VERSION)
GINKGO_FLAGS :=
GO_FILES := $(shell find .. -path ../e2e -prune -o -name '*.go' -print)
MINIKUBE := $(BINDIR)/minikube
KUBECTL := $(BINDIR)/kubectl-$(KUBERNETES_VERSION)
MINIKUBE_HOME = $(BINDIR)
CLUSTER_NAME := minikube
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

export MINIKUBE_HOME

DOMAIN_NAME := cybozu.com

CNI_PLUGIN_VERSION := v1.4.0
CNI_PLUGIN_TAR := cni-plugins-linux-amd64-$(CNI_PLUGIN_VERSION).tgz
CNI_PLUGIN_INSTALL_DIR := /opt/cni/bin

.PHONY: setup-minikube-upstream-rook
setup-minikube-upstream-rook:
	echo aaaaa
	ls -l /home/runner/work/rbd-backup-system/rbd-backup-system/e2e/

	sudo apt update
	sudo apt install -y conntrack socat
	curl -LO https://storage.googleapis.com/minikube/releases/$(MINIKUBE_VERSION)/minikube_latest_amd64.deb
	sudo dpkg -i minikube_latest_amd64.deb
	rm -f minikube_latest_amd64.deb

	echo bbbbb
	ls -l /home/runner/work/rbd-backup-system/rbd-backup-system/e2e/

	curl -LO https://github.com/Mirantis/cri-dockerd/releases/download/v0.3.9/cri-dockerd_0.3.9.3-0.ubuntu-focal_amd64.deb
	sudo dpkg -i cri-dockerd_0.3.9.3-0.ubuntu-focal_amd64.deb
	rm -f cri-dockerd_0.3.9.3-0.ubuntu-focal_amd64.deb

	echo ccccc
	ls -l /home/runner/work/rbd-backup-system/rbd-backup-system/e2e/

	wget https://github.com/kubernetes-sigs/cri-tools/releases/download/$(CRICTL_VERSION)/crictl-$(CRICTL_VERSION)-linux-amd64.tar.gz
	sudo tar zxvf crictl-$(CRICTL_VERSION)-linux-amd64.tar.gz -C /usr/local/bin
	rm -f crictl-$(CRICTL_VERSION)-linux-amd64.tar.gz
	sudo sysctl fs.protected_regular=0

	echo ddddd
	ls -l /home/runner/work/rbd-backup-system/rbd-backup-system/e2e/

	curl -LO "https://github.com/containernetworking/plugins/releases/download/$(CNI_PLUGIN_VERSION)/$(CNI_PLUGIN_TAR)"
	sudo mkdir -p "$(CNI_PLUGIN_INSTALL_DIR)"
	sudo tar -xf "$(CNI_PLUGIN_TAR)" -C "$(CNI_PLUGIN_INSTALL_DIR)"
	rm "$(CNI_PLUGIN_TAR)"

	echo eeeee
	ls -l /home/runner/work/rbd-backup-system/rbd-backup-system/e2e/

	sudo -E minikube start --kubernetes-version="v$(KUBERNETES_VERSION)" --driver=none --memory 6g --cpus=2 --addons ingress --cni=calico

	$(MAKE) $(GINKGO)
	$(MAKE) $(KUBECTL)

.PHONY: test-upstream-rook
test-upstream-rook:
	$(MAKE) launch-rook-ceph
	$(MAKE) common/setup-components
	$(MAKE) common/test

.PHONY: test
test:
	$(MAKE) launch-minikube
	$(MAKE) launch-rook-ceph
	$(MAKE) common/setup-components
	$(MAKE) common/test

.PHONY: clean
clean:
	rm -f controller-container.tar
	$(MAKE) delete-rook-ceph
	$(MAKE) delete-minikube

.PHONY: setup
setup: setup-minikube
	$(MAKE) $(GINKGO)
	$(MAKE) $(MINIKUBE)
	$(MAKE) $(KUBECTL)

$(BINDIR):
	mkdir -p $@

$(TMPDIR):
	mkdir -p $@

$(GINKGO): | $(BINDIR)
	ls -l /home/runner/work/rbd-backup-system/rbd-backup-system/e2e/
	ls -l /home/runner/work/rbd-backup-system/rbd-backup-system/e2e/bin/
	GOBIN=$(BINDIR) go install github.com/onsi/ginkgo/v2/ginkgo@$(GINKGO_VERSION)
	mv $(BINDIR)/ginkgo $@

$(KUBECTL): | $(BINDIR)
	$(CURL) -o $@ https://storage.googleapis.com/kubernetes-release/release/v$(KUBERNETES_VERSION)/bin/linux/amd64/kubectl
	chmod a+x $@

.PHONY: setup-minikube
setup-minikube: | $(BINDIR)
	$(SUDO) apt-get update
	DEBIAN_FRONTEND=noninteractive $(SUDO) apt-get install -y --no-install-recommends conntrack

	$(CURL) -o $(BINDIR)/minikube https://github.com/kubernetes/minikube/releases/download/$(MINIKUBE_VERSION)/minikube-linux-amd64
	chmod a+x $(BINDIR)/minikube

	# These tools are required to use minikube for Kubernetes v1.24+
	# For CNI plugins, see https://github.com/Mirantis/cri-dockerd/blob/v0.2.6/README.md#important
	$(CURL) -o cni-plugins.tgz https://github.com/containernetworking/plugins/releases/download/$(CNI_PLUGINS_VERSION)/cni-plugins-linux-amd64-$(CNI_PLUGINS_VERSION).tgz
	$(SUDO) mkdir -p /opt/cni/bin
	$(SUDO) tar -C /opt/cni/bin -xf cni-plugins.tgz

	# Install cri-docker
	$(CURL) -o cri-dockerd.deb https://github.com/Mirantis/cri-dockerd/releases/download/$(CRI_DOCKERD_VERSION)/cri-dockerd_$(CRI_DOCKERD_VERSION:v%=%).3-0.ubuntu-focal_amd64.deb
	$(SUDO) dpkg -i cri-dockerd.deb

	$(CURL) -o crictl.tar.gz https://github.com/kubernetes-sigs/cri-tools/releases/download/$(CRICTL_VERSION)/crictl-$(CRICTL_VERSION)-linux-amd64.tar.gz
	$(SUDO) tar -C /usr/local/bin -xf crictl.tar.gz

.PHONY: launch-minikube
launch-minikube:
	# TODO: Is there any better way to verify whether k8s cluster is available or not?
	kubectl get pod >/dev/null 2>&1; \
	RET=$$? ; \
	if [ $$RET -eq 0 ] ; then exit; fi ; \
	$(SUDO) -E $(MINIKUBE) start \
		--driver=none \
		--container-runtime=containerd \
		--kubernetes-version=v$(KUBERNETES_VERSION) \
		--extra-config=kubelet.read-only-port=10255 \
		--extra-config=kubeadm.node-name=$(CLUSTER_NAME)-worker \
		--extra-config=kubelet.hostname-override=$(CLUSTER_NAME)-worker \
			--cni=calico
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
	sed "s%LOOP_DEV%$(LOOP_DEV)%" testdata/cluster-template.yaml > testdata/cluster.yaml
	$(KUBECTL) apply -f testdata/cluster.yaml
	$(KUBECTL) apply -f testdata/toolbox.yaml

.PHONY: delete-rook-ceph
delete-rook-ceph:
	$(KUBECTL) delete -f testdata/toolbox.yaml
	$(KUBECTL) delete -f testdata/cluster.yaml
	$(SUDO) rm -rf /var/lib/rook
	ls /dev/mapper/ceph-* | xargs -I% -- sudo dmsetup remove %
	$(SUDO) rm -rf /dev/mapper/ceph-*
	$(KUBECTL) delete -f testdata/operator.yaml
	$(KUBECTL) delete -f testdata/crds.yaml
	$(KUBECTL) delete -f testdata/common.yaml
	$(MAKE) delete-loop-dev

.PHONY: common/setup-components
common/setup-components:
	$(KUBECTL) apply -f https://github.com/cert-manager/cert-manager/releases/download/$(CERT_MANAGER_VERSION)/cert-manager.yaml
	$(MAKE) -C .. docker-build
	docker save controller:latest > controller-container.tar
	$(SUDO) ctr -n k8s.io images import controller-container.tar
	$(MAKE) -C .. install
	$(MAKE) -C .. deploy

.PHONY: common/test
common/test: $(GINKGO)
	$(SUDO) -E env \
	PATH=${PATH} \
	E2ETEST=1 \
	KUBECTL=$(KUBECTL) \
	$(GINKGO) --fail-fast -v $(GINKGO_FLAGS) .
