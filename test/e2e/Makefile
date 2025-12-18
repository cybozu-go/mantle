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
POLLING_INTERVAL := 5
TIMEOUT_SECS := 900
LOOP_DEV := /dev/loop0
LOOP_DEV2 := /dev/loop1
MINIKUBE_PROFILE_PRIMARY=profile1
MINIKUBE_PROFILE_SECONDARY=profile2
TMPDIR := tmp
BACKUP_TRANSFER_PART_SIZE := 3Mi # smaller than the PVC size (10Mi)
MAKEFILE_DIR := $(shell pwd)

export MINIKUBE_HOME
export MAKEFILE_DIR

# Retry a command up to 5 times with a 10 second delay between attempts.
# Usage: $(call retry,command)
define retry
	success=0; \
	for i in $$(seq 5); do \
		$(1) \
		&& success=1 && break || sleep 10; \
	done; \
	if [ $$success -ne 1 ]; then exit 1; fi
endef

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
	$(MAKE) install-cert-manager
	$(MAKE) install-mantle-cluster-wide
# set up a k8s cluster for primary mantle
	$(MAKE) launch-minikube MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_PRIMARY)
	$(MAKE) install-rook-ceph-operator
	$(MAKE) install-rook-ceph-cluster1
	$(MAKE) install-ceph-object-store-secret
	$(MAKE) install-cert-manager
	$(MAKE) install-mantle-cluster-wide
# prepare TLS certificates
	$(MAKE) install-tls-certificates NAMESPACE=$(CEPH_CLUSTER1_NAMESPACE)
# start mantle-controller in the secondary cluster
	$(MAKE) minikube-profile-secondary
	sed \
		-e "s%{OBJECT_STORAGE_BUCKET_NAME}%$$(cat $(TMPDIR)/cm-export-data.json | jq -r .data.BUCKET_NAME)%" \
		-e "s%{OBJECT_STORAGE_ENDPOINT}%http://$$(cat $(TMPDIR)/cm-export-data.json | jq -r .data.BUCKET_HOST)%" \
		testdata/values-mantle-secondary-template.yaml \
		> testdata/values-mantle-secondary.yaml
	$(MAKE) install-mantle \
		NAMESPACE=$(CEPH_CLUSTER1_NAMESPACE) \
		HELM_RELEASE=mantle \
		VALUES_YAML=testdata/values-mantle-secondary.yaml
# update the TLS certificates to include IP SAN of the secondary mantle-controller endpoint
	$(MAKE) install-tls-certificates NAMESPACE=$(CEPH_CLUSTER1_NAMESPACE)
	$(MINIKUBE) -p $(MINIKUBE_PROFILE_SECONDARY) kubectl -- rollout restart -n $(CEPH_CLUSTER1_NAMESPACE) deploy/mantle-controller
# start mantle-controller in the primary cluster
	$(MAKE) minikube-profile-primary
	sed \
		-e "s%{ENDPOINT}%$$($(MINIKUBE) service list -p $(MINIKUBE_PROFILE_SECONDARY) -o json | jq -r '.[] | select(.Name == "mantle-replication" and .Namespace == "rook-ceph") | .URLs | select(. | length > 0)[]' | head -1 | sed -r 's/^http:\/\///')%" \
		-e "s%{OBJECT_STORAGE_BUCKET_NAME}%$$(cat $(TMPDIR)/cm-export-data.json | jq -r .data.BUCKET_NAME)%" \
		-e "s%{OBJECT_STORAGE_ENDPOINT}%$$($(MINIKUBE) service list -p $(MINIKUBE_PROFILE_SECONDARY) -o json | jq -r '.[] | select(.Name == "rook-ceph-rgw-ceph-object-store-node-port" and .Namespace == "rook-ceph") | .URLs | select(. | length > 0)[]' | head -1)%" \
		-e "s%{BACKUP_TRANSFER_PART_SIZE}%$(BACKUP_TRANSFER_PART_SIZE)%" \
		testdata/values-mantle-primary-template.yaml \
		> testdata/values-mantle-primary.yaml
	$(MAKE) install-mantle \
		NAMESPACE=$(CEPH_CLUSTER1_NAMESPACE) \
		HELM_RELEASE=mantle \
		VALUES_YAML=testdata/values-mantle-primary.yaml
# start testing
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
	$(CURL) -o $@ https://dl.k8s.io/release/v$(KUBERNETES_VERSION)/bin/linux/amd64/kubectl
	chmod a+x $@

$(MINIKUBE): | $(BINDIR)
	$(CURL) -o $@ https://github.com/kubernetes/minikube/releases/download/$(MINIKUBE_VERSION)/minikube-linux-amd64
	chmod a+x $@

$(HELM): | $(BINDIR)
	$(CURL) https://get.helm.sh/helm-v$(HELM_VERSION)-linux-amd64.tar.gz \
		| tar xvz -C $(BINDIR) --strip-components 1 linux-amd64/helm

.PHONY: launch-minikube
launch-minikube:
	$(MAKE) do-minikube-start
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

.PHONY: do-minikube-start
do-minikube-start:
	# TODO: Is there any better way to verify whether k8s cluster is available or not?
	if $(MINIKUBE) profile $(MINIKUBE_PROFILE) |& grep "not found" > /dev/null; then \
		ok=0; \
		for i in $(shell seq 1 3) ; do \
			if [ "$$ok" -eq 0 ]; then \
				if [ "$$i" -ne 1 ]; then \
					$(MINIKUBE) delete -p $(MINIKUBE_PROFILE) || true; \
				fi; \
				$(MINIKUBE) start \
					--kubernetes-version="v$(KUBERNETES_VERSION)" \
					--driver=kvm2 \
					--memory 6g \
					--cpus=2 \
					--extra-config=kubeadm.node-name=$(NODE_NAME) \
					--extra-config=kubelet.hostname-override=$(NODE_NAME) \
					--network mantle-test \
					--disk-size=30g \
					-p $(MINIKUBE_PROFILE) ; \
				if [ "$$?" -eq 0 ]; then \
					ok=1; \
				fi; \
			fi; \
		done; \
		if [ $$ok -eq 0 ]; then \
			exit 1; \
		fi \
	fi

.PHONY: create-loop-dev
create-loop-dev:
	$(MINIKUBE) ssh -- $(DD) if=/dev/zero of=$(LOOP_FILE) bs=1G seek=32 count=0
	$(MINIKUBE) ssh -- $(SUDO) $(LOSETUP) $(LOOP_DEV) $(LOOP_FILE) || :
	$(MINIKUBE) ssh $(DD) if=/dev/zero of=$(LOOP_FILE2) bs=1G seek=32 count=0
	$(MINIKUBE) ssh -- $(SUDO) $(LOSETUP) $(LOOP_DEV2) $(LOOP_FILE2) || :
	$(MINIKUBE) ssh -- lsblk

.PHONY: install-rook-ceph-operator
install-rook-ceph-operator:
	$(call retry,$(HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
		--create-namespace --namespace $(CEPH_CLUSTER1_NAMESPACE) -f testdata/values.yaml --wait \
		rook-ceph rook-ceph)
	$(KUBECTL) wait --timeout=60s --for=condition=available -n $(CEPH_CLUSTER1_NAMESPACE) deployment/rook-ceph-operator

.PHONY: install-rook-ceph-cluster1
install-rook-ceph-cluster1:
	$(call retry,$(HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
		--namespace $(CEPH_CLUSTER1_NAMESPACE) -f testdata/values-cluster.yaml \
		--wait rook-ceph-cluster rook-ceph-cluster)
	$(KUBECTL) wait --timeout=300s --for=condition=Ready -n $(CEPH_CLUSTER1_NAMESPACE) cephclusters/rook-ceph

.PHONY: install-rook-ceph-cluster2
install-rook-ceph-cluster2:
	$(call retry,$(HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
		--create-namespace --namespace $(CEPH_CLUSTER2_NAMESPACE) -f testdata/values-cluster.yaml \
		--set cephClusterSpec.dataDirHostPath=/var/lib/rook2 \
		--wait rook-ceph-cluster2 rook-ceph-cluster)
	$(KUBECTL) wait --timeout=300s --for=condition=Ready -n $(CEPH_CLUSTER2_NAMESPACE) cephclusters/rook-ceph2

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

.PHONY: install-cert-manager
install-cert-manager:
	$(call retry,$(HELM) upgrade --install --version $(CERT_MANAGER_VERSION) --repo https://charts.jetstack.io \
		--create-namespace --namespace cert-manager --set crds.enabled=true --wait \
		cert-manager cert-manager)

.PHONY: install-tls-certificates
install-tls-certificates:
# Create a TLS certificate in the primary cluster.
	$(MAKE) minikube-profile-primary
	$(KUBECTL) apply -f testdata/cert-primary.yaml
# In order to get the regenerated Secret for sure, delete the existing secret if any.
	$(KUBECTL) delete secret -n $(NAMESPACE) cert-mantle-replication || true
# Wait until a Secret for the certificates is created.
	until $(KUBECTL) get secret -n $(NAMESPACE) cert-mantle-replication >/dev/null ; do sleep 1; done
# Dump ca.crt in the secret to import it to the secondary cluster.
	$(KUBECTL) get secret -n $(NAMESPACE) cert-mantle-replication -o json | jq -r '.data."ca.crt"' | base64 -d > $(TMPDIR)/primary-ca.crt
	$(MAKE) minikube-profile-secondary
	$(KUBECTL) create cm mantle-primary-ca-crt -n $(NAMESPACE) --from-file=ca.crt=$(TMPDIR)/primary-ca.crt \
		--output yaml --dry-run=client | $(KUBECTL) apply -f -
# Create a TLS certificate in the secondary cluster.
#
# Before creating a Certificate resource, we need to replace the {ENDPOINT} placeholder
# in the template file with the IP address of the secondary mantle-controller's Service endpoint.
# By doing so, the generated TLS certificate will have an IP SAN of that IP address,
# so the primary mantle-controller can access the secondary via TLS without any hostname.
# If the Service doesn't exist (i.e., the secondary mantle-controller is not started yet),
# just erase the placeholder.
	sed \
		-e "s%{ENDPOINT}%$$(\
			IPADDR=$$($(MINIKUBE) service list -p $(MINIKUBE_PROFILE_SECONDARY) -o json | jq -r '.[] | select(.Name == "mantle-replication" and .Namespace == "rook-ceph") | .URLs | select(. | length > 0)[]' | head -1 | sed -r 's/^http:\/\/([^:]+).*$$/\1/'); \
			if [ "$$IPADDR" = "" ]; then echo -n ""; else echo -n "\"$${IPADDR}\""; fi)%g" \
		testdata/cert-secondary-template.yaml \
		> $(TMPDIR)/cert-secondary.yaml
	$(KUBECTL) apply -f $(TMPDIR)/cert-secondary.yaml
# In order to get the regenerated Secret for sure, delete the existing secret if any.
	$(KUBECTL) delete secret -n $(NAMESPACE) cert-mantle-replication || true
# Wait until a Secret for the certificates is created.
	until $(KUBECTL) get secret -n $(NAMESPACE) cert-mantle-replication >/dev/null ; do sleep 1; done
# Dump ca.crt in the secret to import it to the primary cluster.
	$(KUBECTL) get secret -n $(NAMESPACE) cert-mantle-replication -o json | jq -r '.data."ca.crt"' | base64 -d > $(TMPDIR)/secondary-ca.crt
	$(MAKE) minikube-profile-primary
	$(KUBECTL) create cm mantle-secondary-ca-crt -n $(NAMESPACE) --from-file=ca.crt=$(TMPDIR)/secondary-ca.crt \
		--output yaml --dry-run=client | $(KUBECTL) apply -f -

.PHONY: minikube-profile-primary
minikube-profile-primary:
	$(MINIKUBE) profile $(MINIKUBE_PROFILE_PRIMARY)
.PHONY: minikube-profile-secondary
minikube-profile-secondary:
	$(MINIKUBE) profile $(MINIKUBE_PROFILE_SECONDARY)

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
	$(GINKGO) --fail-fast -v $(GINKGO_FLAGS) singlek8s; \
	if [ "$$?" -ne 0 ]; then \
		echo "Controller logs for $(CEPH_CLUSTER1_NAMESPACE) namespace:"; \
		$(KUBECTL) logs --tail 100 -n $(CEPH_CLUSTER1_NAMESPACE) -l app.kubernetes.io/name=mantle -c mantle; \
		echo ""; \
		echo "Controller logs for $(CEPH_CLUSTER2_NAMESPACE) namespace:"; \
		$(KUBECTL) logs --tail 100 -n $(CEPH_CLUSTER2_NAMESPACE) -l app.kubernetes.io/name=mantle -c mantle; \
		# Explicitly fail the test to mark the CI job as failed. \
		exit 1; \
	fi

.PHONY: do-test-multik8s
do-test-multik8s: $(GINKGO)
	export KUBECTL_PRIMARY="$(MINIKUBE) -p $(MINIKUBE_PROFILE_PRIMARY) kubectl -- "; \
	export KUBECTL_SECONDARY="$(MINIKUBE) -p $(MINIKUBE_PROFILE_SECONDARY) kubectl -- "; \
	env \
	PATH=${PATH} \
	E2ETEST=1 \
	$(GINKGO) --fail-fast -v $(GINKGO_FLAGS) multik8s; \
	if [ "$$?" -ne 0 ]; then \
		echo "Controller logs for $(MINIKUBE_PROFILE_PRIMARY):"; \
		$${KUBECTL_PRIMARY} logs --tail 1000 -n $(CEPH_CLUSTER1_NAMESPACE) -l app.kubernetes.io/name=mantle -c mantle; \
		echo ""; \
		echo "Controller logs for $(MINIKUBE_PROFILE_SECONDARY):"; \
		$${KUBECTL_SECONDARY}  logs --tail 1000 -n $(CEPH_CLUSTER1_NAMESPACE) -l app.kubernetes.io/name=mantle -c mantle; \
		echo ""; \
		echo "Primary MBs"; \
		$${KUBECTL_PRIMARY} get mantlebackups -A -o yaml; \
		echo ""; \
		echo "Secondary MBs"; \
		$${KUBECTL_SECONDARY} get mantlebackups -A -o yaml; \
		echo ""; \
		echo "Primary PVCs"; \
		$${KUBECTL_PRIMARY} get pvc -A -o yaml; \
		echo ""; \
		echo "Secondary PVCs"; \
		$${KUBECTL_SECONDARY} get pvc -A -o yaml; \
		echo ""; \
		echo "Primary PVs"; \
		$${KUBECTL_PRIMARY} get pv -o yaml; \
		echo ""; \
		echo "Secondary PVs"; \
		$${KUBECTL_SECONDARY} get pv -o yaml; \
		echo ""; \
		echo "Primary Jobs"; \
		$${KUBECTL_PRIMARY} get job -n $(CEPH_CLUSTER1_NAMESPACE) -l app.kubernetes.io/name=mantle -o yaml; \
		echo ""; \
		echo "Secondary Jobs"; \
		$${KUBECTL_SECONDARY} get job -n $(CEPH_CLUSTER1_NAMESPACE) -l app.kubernetes.io/name=mantle -o yaml; \
		echo ""; \
		echo "Job logs for $(MINIKUBE_PROFILE_PRIMARY):"; \
		$${KUBECTL_PRIMARY} get job -n $(CEPH_CLUSTER1_NAMESPACE) -l app.kubernetes.io/name=mantle -o name | \
			while read line; do echo $$line; $${KUBECTL_PRIMARY} logs -n $(CEPH_CLUSTER1_NAMESPACE) $$line; done; \
		echo ""; \
		echo "Job logs for $(MINIKUBE_PROFILE_SECONDARY):"; \
		$${KUBECTL_SECONDARY} get job -n $(CEPH_CLUSTER1_NAMESPACE) -l app.kubernetes.io/name=mantle -o name | \
			while read line; do echo $$line; $${KUBECTL_SECONDARY} logs -n $(CEPH_CLUSTER1_NAMESPACE) $$line; done; \
		# Explicitly fail the test to mark the CI job as failed. \
		exit 1; \
	fi
