include ../versions.mk

BINDIR := $(shell pwd)/bin
CURL := curl -sSLf
GINKGO := $(BINDIR)/ginkgo-$(GINKGO_VERSION)
GINKGO_FLAGS :=
GO_FILES := $(shell find .. -path ../e2e -prune -o -name '*.go' -print)
MINIKUBE := bin/minikube
KUBECTL := $(BINDIR)/kubectl-$(KUBERNETES_VERSION)
MINIKUBE_HOME = $(BINDIR)
CLUSTER_NAME := minikube
SUDO := sudo

export MINIKUBE_HOME

DOMAIN_NAME := cybozu.com

.PHONY: test
test:
	$(MAKE) launch-minikube
	$(MAKE) launch-rook-ceph
	$(MAKE) common/test

.PHONY: clean
clean:
	$(MAKE) delete-minikube

.PHONY: setup
setup: setup-minikube
	$(MAKE) $(GINKGO)
	$(MAKE) $(MINIKUBE)
	$(MAKE) $(KUBECTL)

$(BINDIR):
	mkdir -p $@

$(GINKGO): | $(BINDIR)
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
	$(SUDO) -E $(BINDIR)/minikube start \
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
	$(SUDO) -E $(BINDIR)/minikube delete || true

.PHONY: launch-rook-ceph
launch-rook-ceph:
	$(KUBECTL) apply -f testdata/common.yaml
	$(KUBECTL) apply -f testdata/crds.yaml
	$(KUBECTL) apply -f testdata/operator.yaml

.PHONY: delete-rook-ceph
delete-rook-ceph:
	$(KUBECTL) delete -f testdata/operator.yaml
	$(KUBECTL) delete -f testdata/crds.yaml
	$(KUBECTL) delete -f testdata/common.yaml

.PHONY: common/test
common/test: $(GINKGO)
	$(SUDO) -E env \
	PATH=${PATH} \
	KUBECTL=$(KUBECTL) \
	$(GINKGO) --fail-fast -v $(GINKGO_FLAGS) .
