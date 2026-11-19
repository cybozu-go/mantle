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
MINIKUBE_CPUS := 2
IMAGE := controller:latest
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
TOOLBOX_RESTART_INTERVAL := 15
LOOP_DEV := /dev/loop0
LOOP_DEV2 := /dev/loop1
MINIKUBE_PROFILE_PRIMARY=profile1
MINIKUBE_PROFILE_SECONDARY=profile2
# The minikube profile of the cluster that the targets to set up a cluster operate on.
# If it's empty, they use the current minikube profile and kubectl context. The targets
# that run in parallel must specify it, because both clusters share the current ones.
MINIKUBE_PROFILE :=
PROFILE_MINIKUBE = $(MINIKUBE)$(if $(MINIKUBE_PROFILE), -p $(MINIKUBE_PROFILE))
PROFILE_KUBECTL = $(KUBECTL)$(if $(MINIKUBE_PROFILE), --context=$(MINIKUBE_PROFILE))
PROFILE_HELM = $(HELM)$(if $(MINIKUBE_PROFILE), --kube-context=$(MINIKUBE_PROFILE))
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

# Run a command and add "[PREFIX] " to the head of each line of its output, so that
# the output of the targets running in parallel can be told apart.
# Usage: $(call run-with-prefix,PREFIX,command)
run-with-prefix = set -o pipefail; $(2) 2>&1 | sed -u 's/^/[$(1)] /'

.PHONY: setup
setup:
	$(MAKE) $(GINKGO)
	$(MAKE) $(KUBECTL)
	$(MAKE) $(MINIKUBE)
	$(MAKE) $(HELM)
	mkdir -p $(TMPDIR)

.PHONY: test
test:
	$(MAKE) -j 2 singlek8s-build-image singlek8s-start-vm
	$(MAKE) -j 2 singlek8s-launch singlek8s-build-test
	$(MAKE) do_test

.PHONY: launch-single-k8s-cluster
launch-single-k8s-cluster:
	$(MAKE) launch-minikube MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_PRIMARY)
	$(MINIKUBE) profile $(MINIKUBE_PROFILE_PRIMARY)
	$(MAKE) install-rook-ceph-operator
	$(MAKE) install-rook-ceph-cluster1
	$(MAKE) install-rook-ceph-cluster2
	$(MAKE) install-mantle-cluster-wide
	$(MAKE) install-mantle \
		NAMESPACE=$(CEPH_CLUSTER1_NAMESPACE) \
		VALUES_YAML=testdata/values-mantle1.yaml
	$(MAKE) install-mantle \
		NAMESPACE=$(CEPH_CLUSTER2_NAMESPACE) \
		VALUES_YAML=testdata/values-mantle2.yaml

.PHONY: singlek8s-build-image
singlek8s-build-image:
	$(call run-with-prefix,build-image,$(MAKE) docker-build)

.PHONY: singlek8s-start-vm
singlek8s-start-vm:
	$(call run-with-prefix,start-vm,$(MAKE) do-minikube-start MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_PRIMARY))

.PHONY: singlek8s-launch
singlek8s-launch:
	$(call run-with-prefix,$(MINIKUBE_PROFILE_PRIMARY),$(MAKE) launch-single-k8s-cluster)

.PHONY: singlek8s-build-test
singlek8s-build-test: $(GINKGO)
	$(call run-with-prefix,build-test,$(GINKGO) build singlek8s)

.PHONY: test-multiple-k8s-clusters
test-multiple-k8s-clusters:
	$(MAKE) launch-multiple-k8s-clusters
	$(MAKE) do-test-multik8s

.PHONY: launch-multiple-k8s-clusters
launch-multiple-k8s-clusters:
# set up k8s clusters for secondary and primary mantle in parallel
	$(MAKE) -j 5 multik8s-launch-secondary multik8s-launch-primary \
		multik8s-install-cert-manager-secondary multik8s-install-cert-manager-primary \
		multik8s-build-test
# copy secret from secondary
	$(MAKE) install-ceph-object-store-secret MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_PRIMARY)
# prepare TLS certificates
	$(MAKE) install-selfsigned-issuer MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_PRIMARY)
	$(MAKE) install-selfsigned-issuer MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_SECONDARY)
	$(MAKE) -j 2 multik8s-install-mantle1 multik8s-install-mantle2
	$(MAKE) minikube-profile-primary

# The following multik8s-* targets set up the two k8s clusters for
# launch-multiple-k8s-clusters, running in parallel as follows:
#
#   multik8s-build-image --+--> multik8s-launch-secondary
#   multik8s-start-vms ----+--> multik8s-launch-primary
#   multik8s-start-vms -------> multik8s-install-cert-manager-{secondary,primary}
#   multik8s-build-image -----> multik8s-build-test
#
# multik8s-start-vms starts the VMs one by one before setting up the clusters, because
# "minikube start" rewrites the kubeconfig file, which kubectl and helm for the other
# cluster may read at the same time. The do-multik8s-launch-* targets pass the given
# MINIKUBE_PROFILE to the sub-makes, so every command in them specifies the cluster.
.PHONY: multik8s-build-image
multik8s-build-image:
	$(call run-with-prefix,build-image,$(MAKE) docker-build)

.PHONY: multik8s-start-vms
multik8s-start-vms:
	$(call run-with-prefix,start-vms,$(MAKE) do-minikube-start MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_SECONDARY))
	$(call run-with-prefix,start-vms,$(MAKE) do-minikube-start MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_PRIMARY))

.PHONY: multik8s-launch-secondary
multik8s-launch-secondary: multik8s-build-image multik8s-start-vms
	$(call run-with-prefix,$(MINIKUBE_PROFILE_SECONDARY),$(MAKE) do-multik8s-launch-secondary MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_SECONDARY))

.PHONY: do-multik8s-launch-secondary
do-multik8s-launch-secondary:
	$(MAKE) launch-minikube
	$(MAKE) install-rook-ceph-operator
	$(MAKE) install-rook-ceph-cluster1
	$(MAKE) fake-rook-ceph-cluster2
	$(MAKE) install-ceph-object-store
	$(MAKE) install-ceph-object-store-secret
	$(MAKE) install-mantle-cluster-wide

.PHONY: multik8s-launch-primary
multik8s-launch-primary: multik8s-build-image multik8s-start-vms
	$(call run-with-prefix,$(MINIKUBE_PROFILE_PRIMARY),$(MAKE) do-multik8s-launch-primary MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_PRIMARY))

.PHONY: do-multik8s-launch-primary
do-multik8s-launch-primary:
	$(MAKE) launch-minikube
	$(MAKE) install-rook-ceph-operator
	$(MAKE) install-rook-ceph-cluster1
	$(MAKE) fake-rook-ceph-cluster2
	$(MAKE) install-mantle-cluster-wide

.PHONY: multik8s-install-cert-manager-secondary
multik8s-install-cert-manager-secondary: multik8s-start-vms
	$(call run-with-prefix,$(MINIKUBE_PROFILE_SECONDARY)-cert-manager,$(MAKE) install-cert-manager MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_SECONDARY))

.PHONY: multik8s-install-cert-manager-primary
multik8s-install-cert-manager-primary: multik8s-start-vms
	$(call run-with-prefix,$(MINIKUBE_PROFILE_PRIMARY)-cert-manager,$(MAKE) install-cert-manager MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_PRIMARY))

.PHONY: multik8s-build-test
multik8s-build-test: multik8s-build-image
	$(call run-with-prefix,build-test,$(MAKE) do-multik8s-build-test)

.PHONY: do-multik8s-build-test
do-multik8s-build-test: $(GINKGO)
	$(GINKGO) build multik8s

.PHONY: multik8s-install-mantle1
multik8s-install-mantle1:
	$(call run-with-prefix,$(CEPH_CLUSTER1_NAMESPACE),$(MAKE) do-multik8s-install-mantle NAMESPACE=$(CEPH_CLUSTER1_NAMESPACE) CEPH_CLUSTER_NO=1)

.PHONY: multik8s-install-mantle2
multik8s-install-mantle2:
	$(call run-with-prefix,$(CEPH_CLUSTER2_NAMESPACE),$(MAKE) do-multik8s-install-mantle NAMESPACE=$(CEPH_CLUSTER2_NAMESPACE) CEPH_CLUSTER_NO=2)

.PHONY: do-multik8s-install-mantle
do-multik8s-install-mantle:
	$(MAKE) install-tls-certificates
# install mantle-controller in the secondary cluster
	sed \
		-e "s%{OBJECT_STORAGE_BUCKET_NAME}%$$(cat $(TMPDIR)/cm-export-data-$(CEPH_CLUSTER_NO).json | jq -r .data.BUCKET_NAME)%" \
		-e "s%{OBJECT_STORAGE_ENDPOINT}%http://$$(cat $(TMPDIR)/cm-export-data-$(CEPH_CLUSTER_NO).json | jq -r .data.BUCKET_HOST)%" \
		testdata/values-mantle-secondary-$(CEPH_CLUSTER_NO)-template.yaml \
		> testdata/values-mantle-secondary-$(CEPH_CLUSTER_NO).yaml
	$(MAKE) install-mantle MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_SECONDARY) \
		VALUES_YAML=testdata/values-mantle-secondary-$(CEPH_CLUSTER_NO).yaml
# update the TLS certificates to include IP SAN of the secondary mantle-controller endpoint
	$(MAKE) install-tls-certificates
	$(MINIKUBE) -p $(MINIKUBE_PROFILE_SECONDARY) kubectl -- rollout restart -n $(NAMESPACE) deploy/mantle-controller
# install mantle-controller in the primary cluster
	sed \
		-e "s%{ENDPOINT}%$$($(MINIKUBE) service list -p $(MINIKUBE_PROFILE_SECONDARY) -o json \
		  | jq -r --arg NS $(NAMESPACE) \
		  '.[] | select(.Name == "mantle-replication" and .Namespace == $$NS) | .URLs | select(. | length > 0)[]' \
		  | head -1 | sed -r 's/^http:\/\///')%" \
		-e "s%{OBJECT_STORAGE_BUCKET_NAME}%$$(cat $(TMPDIR)/cm-export-data-$(CEPH_CLUSTER_NO).json | jq -r .data.BUCKET_NAME)%" \
		-e "s%{OBJECT_STORAGE_ENDPOINT}%$$($(MINIKUBE) service list -p $(MINIKUBE_PROFILE_SECONDARY) -o json \
		  | jq -r --arg NS $(CEPH_CLUSTER1_NAMESPACE) \
		  '.[] | select(.Name == "rook-ceph-rgw-ceph-object-store-node-port" and .Namespace == $$NS) | .URLs | select(. | length > 0)[]' \
		  | head -1)%" \
		-e "s%{BACKUP_TRANSFER_PART_SIZE}%$(BACKUP_TRANSFER_PART_SIZE)%" \
		testdata/values-mantle-primary-template.yaml \
		> testdata/values-mantle-primary-$(CEPH_CLUSTER_NO).yaml
	$(MAKE) install-mantle MINIKUBE_PROFILE=$(MINIKUBE_PROFILE_PRIMARY) \
		VALUES_YAML=testdata/values-mantle-primary-$(CEPH_CLUSTER_NO).yaml

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
	$(MAKE) image-build
	$(MAKE) create-loop-dev
	set -o pipefail; \
	sed \
		-e "s%{LOOP_DEV}%$(LOOP_DEV)%" \
		-e "s%{LOOP_DEV2}%$(LOOP_DEV2)%" \
		-e "s%{NODE_NAME}%$(NODE_NAME)%" \
		testdata/persistentvolumes-template.yaml \
		| $(PROFILE_KUBECTL) apply -f -

# Start the VM of MINIKUBE_PROFILE if the profile doesn't exist. Check it with
# "minikube profile list", because "minikube profile <name>" switches the current
# profile and context if the profile exists.
.PHONY: do-minikube-start
do-minikube-start:
	# TODO: Is there any better way to verify whether k8s cluster is available or not?
	if ! $(MINIKUBE) profile list -o json \
		| jq -e --arg profile $(MINIKUBE_PROFILE) '[.valid[]?, .invalid[]?] | any(.Name == $$profile)' > /dev/null; then \
		ok=0; \
		for i in $(shell seq 1 3) ; do \
			if [ "$$ok" -eq 0 ]; then \
				if [ "$$i" -ne 1 ]; then \
					$(MINIKUBE) delete -p $(MINIKUBE_PROFILE) || true; \
				fi; \
				$(MINIKUBE) start \
					--kubernetes-version="v$(KUBERNETES_VERSION)" \
					--driver=kvm2 \
					--container-runtime=docker \
					--memory 6g \
					--cpus=$(MINIKUBE_CPUS) \
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
	$(PROFILE_MINIKUBE) ssh -- $(DD) if=/dev/zero of=$(LOOP_FILE) bs=1G seek=32 count=0
	$(PROFILE_MINIKUBE) ssh -- $(SUDO) $(LOSETUP) $(LOOP_DEV) $(LOOP_FILE) || :
	$(PROFILE_MINIKUBE) ssh $(DD) if=/dev/zero of=$(LOOP_FILE2) bs=1G seek=32 count=0
	$(PROFILE_MINIKUBE) ssh -- $(SUDO) $(LOSETUP) $(LOOP_DEV2) $(LOOP_FILE2) || :
	$(PROFILE_MINIKUBE) ssh -- lsblk

.PHONY: install-rook-ceph-operator
install-rook-ceph-operator:
	$(call retry,$(PROFILE_HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
		--create-namespace --namespace $(CEPH_CLUSTER1_NAMESPACE) -f testdata/values.yaml --wait \
		rook-ceph rook-ceph)
	$(PROFILE_KUBECTL) wait --timeout=60s --for=condition=available -n $(CEPH_CLUSTER1_NAMESPACE) deployment/rook-ceph-operator

.PHONY: install-rook-ceph-cluster1
install-rook-ceph-cluster1:
	$(call retry,$(PROFILE_HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
		--namespace $(CEPH_CLUSTER1_NAMESPACE) -f testdata/values-cluster.yaml \
		--wait --force-conflicts rook-ceph-cluster rook-ceph-cluster)
	$(MAKE) wait-ceph-ready NAMESPACE=$(CEPH_CLUSTER1_NAMESPACE)

.PHONY: install-rook-ceph-cluster2
install-rook-ceph-cluster2:
	$(call retry,$(PROFILE_HELM) upgrade --install --version $(ROOK_CHART_VERSION) --repo https://charts.rook.io/release \
		--create-namespace --namespace $(CEPH_CLUSTER2_NAMESPACE) -f testdata/values-cluster.yaml \
		--set cephClusterSpec.dataDirHostPath=/var/lib/rook2 \
		--wait --force-conflicts rook-ceph-cluster2 rook-ceph-cluster)
	$(MAKE) wait-ceph-ready NAMESPACE=$(CEPH_CLUSTER2_NAMESPACE)

.PHONY: fake-rook-ceph-cluster2
fake-rook-ceph-cluster2:
	$(PROFILE_KUBECTL) get namespace $(CEPH_CLUSTER2_NAMESPACE) || $(PROFILE_KUBECTL) create namespace $(CEPH_CLUSTER2_NAMESPACE)
	$(PROFILE_KUBECTL) get configmap -n $(CEPH_CLUSTER1_NAMESPACE) rook-ceph-mon-endpoints -o yaml \
	  | yq '{"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "rook-ceph-mon-endpoints"}, "data": .data}' \
	  | $(PROFILE_KUBECTL) apply -n $(CEPH_CLUSTER2_NAMESPACE) -f -
	$(PROFILE_KUBECTL) get secret -n $(CEPH_CLUSTER1_NAMESPACE) rook-ceph-mon -o yaml \
	  | yq '{"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "rook-ceph-mon"}, "data": .data}' \
	  | $(PROFILE_KUBECTL) apply -n $(CEPH_CLUSTER2_NAMESPACE) -f -

.PHONY: wait-ceph-ready
wait-ceph-ready:
	is_ok="false"; \
	deadline=$$((SECONDS + $(TIMEOUT_SECS))); \
	last_restart=$$((SECONDS - $(TOOLBOX_RESTART_INTERVAL))); \
	while [ $$SECONDS -lt $$deadline ]; do \
		if osd_stat=$$($(PROFILE_KUBECTL) -n $(NAMESPACE) exec deploy/rook-ceph-tools -- ceph osd stat); then \
			echo "$${osd_stat}"; \
			if echo "$${osd_stat}" | grep -qE "[1-9][0-9]* up" && \
				$(PROFILE_KUBECTL) -n $(NAMESPACE) exec deploy/rook-ceph-tools -- ceph -s; then \
				echo "ceph -s succeeded in namespace $(NAMESPACE)"; \
				is_ok="true"; \
				break; \
			fi; \
		elif [ $$((SECONDS - last_restart)) -ge $(TOOLBOX_RESTART_INTERVAL) ]; then \
			$(PROFILE_KUBECTL) -n $(NAMESPACE) rollout restart deploy rook-ceph-tools; \
			$(PROFILE_KUBECTL) -n $(NAMESPACE) rollout status deploy rook-ceph-tools --timeout=1m; \
			last_restart=$$SECONDS; \
		fi; \
		echo "waiting for ceph -s to succeed in namespace $(NAMESPACE)" > /dev/stderr; \
		sleep $(POLLING_INTERVAL); \
	done; \
	if [ "$$is_ok" = "false" ]; then \
		echo "ceph -s failed to become ready in namespace $(NAMESPACE)" > /dev/stderr; \
		$(PROFILE_KUBECTL) -n $(NAMESPACE) exec deploy/rook-ceph-tools -- ceph -s ; \
		echo ; \
		echo "=== Pod ===" ; \
		$(PROFILE_KUBECTL) get -A pod ; \
		echo ; \
		echo "=== Job ===" ; \
		$(PROFILE_KUBECTL) get -A job ; \
		echo ; \
		echo "=== PVC ===" ; \
		$(PROFILE_KUBECTL) get -A pvc ; \
		echo ; \
		echo "=== PV ===" ; \
		$(PROFILE_KUBECTL) get pv ; \
		echo ; \
		echo "=== Node ===" ; \
		$(PROFILE_KUBECTL) get node ; \
		echo ; \
		echo "=== Service ===" ; \
		$(PROFILE_KUBECTL) get -A svc ; \
		exit 1; \
	fi

.PHONY: install-ceph-object-store
install-ceph-object-store:
	$(PROFILE_KUBECTL) apply -f testdata/objectstorage.yaml
	$(PROFILE_KUBECTL) wait -n rook-ceph cephobjectstore/ceph-object-store --for=jsonpath='{.status.phase}'=Ready --timeout=$(TIMEOUT_SECS)s
	$(PROFILE_KUBECTL) wait -n rook-ceph deploy/rook-ceph-rgw-ceph-object-store-a --for=condition=Available --timeout=$(TIMEOUT_SECS)s
	$(PROFILE_KUBECTL) apply -f testdata/objectbucketclaims.yaml
	until $(PROFILE_KUBECTL) get -n rook-ceph cm export-data-1 &> /dev/null && $(PROFILE_KUBECTL) get -n rook-ceph cm export-data-2 &> /dev/null; do sleep 3 ; done
	$(PROFILE_KUBECTL) get -n rook-ceph configmap export-data-1 -o json \
	  | jq -r --arg NS $(CEPH_CLUSTER1_NAMESPACE) \
	  '{ apiVersion: "v1", kind: "ConfigMap", metadata: { name: "export-data", namespace: $$NS }, data: .data }' \
	  > $(TMPDIR)/cm-export-data-1.json
	$(PROFILE_KUBECTL) get -n rook-ceph configmap export-data-2 -o json \
	  | jq -r --arg NS $(CEPH_CLUSTER2_NAMESPACE) \
	  '{ apiVersion: "v1", kind: "ConfigMap", metadata: { name: "export-data", namespace: $$NS }, data: .data }' \
	  > $(TMPDIR)/cm-export-data-2.json
	until $(PROFILE_KUBECTL) get -n rook-ceph secret export-data-1 &> /dev/null && $(PROFILE_KUBECTL) get -n rook-ceph secret export-data-2 &> /dev/null; do sleep 3 ; done
	$(PROFILE_KUBECTL) get -n rook-ceph secret export-data-1 -o json | jq -r --arg NS $(CEPH_CLUSTER1_NAMESPACE) \
	  '{ apiVersion: "v1", kind: "Secret", metadata: { name: "export-data", namespace: $$NS }, type: "Opaque", data: .data }' \
	  > $(TMPDIR)/secret-export-data-1.json
	$(PROFILE_KUBECTL) get -n rook-ceph secret export-data-2 -o json | jq -r --arg NS $(CEPH_CLUSTER2_NAMESPACE) \
	  '{ apiVersion: "v1", kind: "Secret", metadata: { name: "export-data", namespace: $$NS }, type: "Opaque", data: .data }' \
	  > $(TMPDIR)/secret-export-data-2.json

.PHONY: install-ceph-object-store-secret
install-ceph-object-store-secret:
	$(PROFILE_KUBECTL) apply -f $(TMPDIR)/secret-export-data-1.json
	$(PROFILE_KUBECTL) apply -f $(TMPDIR)/secret-export-data-2.json

.PHONY: install-cert-manager
install-cert-manager:
	$(call retry,$(PROFILE_HELM) upgrade --install --version $(CERT_MANAGER_VERSION) --repo https://charts.jetstack.io \
		--create-namespace --namespace cert-manager --set crds.enabled=true --wait \
		cert-manager cert-manager)

.PHONY: install-selfsigned-issuer
install-selfsigned-issuer:
	$(PROFILE_KUBECTL) apply -f testdata/selfsigned-issuer.yaml

.PHONY: install-tls-certificates
install-tls-certificates:
# Create a TLS certificate in the primary cluster.
	$(KUBECTL) --context=$(MINIKUBE_PROFILE_PRIMARY) apply -n $(NAMESPACE) -f testdata/cert-primary.yaml
# In order to get the regenerated Secret for sure, delete the existing secret if any.
	$(KUBECTL) --context=$(MINIKUBE_PROFILE_PRIMARY) delete secret -n $(NAMESPACE) cert-mantle-replication || true
# Wait until a Secret for the certificates is created.
	until $(KUBECTL) --context=$(MINIKUBE_PROFILE_PRIMARY) get secret -n $(NAMESPACE) cert-mantle-replication >/dev/null ; do sleep 1; done
# Dump ca.crt in the secret to import it to the secondary cluster.
	$(KUBECTL) --context=$(MINIKUBE_PROFILE_PRIMARY) get secret -n $(NAMESPACE) cert-mantle-replication -o json | jq -r '.data."ca.crt"' | base64 -d > $(TMPDIR)/primary-ca-$(NAMESPACE).crt
	$(KUBECTL) --context=$(MINIKUBE_PROFILE_SECONDARY) create cm mantle-primary-ca-crt -n $(NAMESPACE) --from-file=ca.crt=$(TMPDIR)/primary-ca-$(NAMESPACE).crt \
		--output yaml --dry-run=client | $(KUBECTL) --context=$(MINIKUBE_PROFILE_SECONDARY) apply -f -
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
			IPADDR=$$($(MINIKUBE) service list -p $(MINIKUBE_PROFILE_SECONDARY) -o json | jq -r --arg NS $(NAMESPACE) '.[] | select(.Name == "mantle-replication" and .Namespace == $$NS) | .URLs | select(. | length > 0)[]' | head -1 | sed -r 's/^http:\/\/([^:]+).*$$/\1/'); \
			if [ "$$IPADDR" = "" ]; then echo -n ""; else echo -n "\"$${IPADDR}\""; fi)%g" \
		testdata/cert-secondary-template.yaml \
		> $(TMPDIR)/cert-secondary-$(NAMESPACE).yaml
	$(KUBECTL) --context=$(MINIKUBE_PROFILE_SECONDARY) apply -n $(NAMESPACE) -f $(TMPDIR)/cert-secondary-$(NAMESPACE).yaml
# In order to get the regenerated Secret for sure, delete the existing secret if any.
	$(KUBECTL) --context=$(MINIKUBE_PROFILE_SECONDARY) delete secret -n $(NAMESPACE) cert-mantle-replication || true
# Wait until a Secret for the certificates is created.
	until $(KUBECTL) --context=$(MINIKUBE_PROFILE_SECONDARY) get secret -n $(NAMESPACE) cert-mantle-replication >/dev/null ; do sleep 1; done
# Dump ca.crt in the secret to import it to the primary cluster.
	$(KUBECTL) --context=$(MINIKUBE_PROFILE_SECONDARY) get secret -n $(NAMESPACE) cert-mantle-replication -o json | jq -r '.data."ca.crt"' | base64 -d > $(TMPDIR)/secondary-ca-$(NAMESPACE).crt
	$(KUBECTL) --context=$(MINIKUBE_PROFILE_PRIMARY) create cm mantle-secondary-ca-crt -n $(NAMESPACE) --from-file=ca.crt=$(TMPDIR)/secondary-ca-$(NAMESPACE).crt \
		--output yaml --dry-run=client | $(KUBECTL) --context=$(MINIKUBE_PROFILE_PRIMARY) apply -f -

.PHONY: minikube-profile-primary
minikube-profile-primary:
	$(MINIKUBE) profile $(MINIKUBE_PROFILE_PRIMARY)
.PHONY: minikube-profile-secondary
minikube-profile-secondary:
	$(MINIKUBE) profile $(MINIKUBE_PROFILE_SECONDARY)

.PHONY: docker-build
docker-build:
	$(MAKE) -C ../.. docker-build IMG=$(IMAGE)

# Build the image on the host and load it into the minikube VM.
.PHONY: image-build
image-build: docker-build
	set -o pipefail; \
	docker_env=$$($(PROFILE_MINIKUBE) docker-env) && \
	docker save $(IMAGE) | (eval "$${docker_env}" && docker load)
	$(PROFILE_MINIKUBE) ssh -- docker images

.PHONY: install-mantle-cluster-wide
install-mantle-cluster-wide:
	$(PROFILE_HELM) upgrade --install mantle-cluster-wide ../../charts/mantle-cluster-wide/ --wait

.PHONY: install-mantle
install-mantle:
	$(PROFILE_HELM) upgrade --install --namespace=$(NAMESPACE) mantle ../../charts/mantle/ --wait -f $(VALUES_YAML)
	$(PROFILE_KUBECTL) rollout restart -n $(NAMESPACE) deploy/mantle-controller

.PHONY: do_test
do_test: $(GINKGO) singlek8s-build-test
	env \
	PATH=${PATH} \
	E2ETEST=1 \
	KUBECTL=$(KUBECTL) \
	$(GINKGO) --fail-fast -v $(GINKGO_FLAGS) singlek8s/singlek8s.test; \
	if [ "$$?" -ne 0 ]; then \
		echo "Controller logs for $(CEPH_CLUSTER1_NAMESPACE) namespace:"; \
		$(KUBECTL) logs --tail 1000 -n $(CEPH_CLUSTER1_NAMESPACE) -l app.kubernetes.io/name=mantle -c mantle; \
		echo ""; \
		echo "Controller logs for $(CEPH_CLUSTER2_NAMESPACE) namespace:"; \
		$(KUBECTL) logs --tail 1000 -n $(CEPH_CLUSTER2_NAMESPACE) -l app.kubernetes.io/name=mantle -c mantle; \
		# Explicitly fail the test to mark the CI job as failed. \
		exit 1; \
	fi

.PHONY: do-test-multik8s
do-test-multik8s: do-multik8s-build-test
	export KUBECTL_PRIMARY="$(MINIKUBE) -p $(MINIKUBE_PROFILE_PRIMARY) kubectl -- "; \
	export KUBECTL_SECONDARY="$(MINIKUBE) -p $(MINIKUBE_PROFILE_SECONDARY) kubectl -- "; \
	env \
	PATH=${PATH} \
	E2ETEST=1 \
	$(GINKGO) --fail-fast -v $(GINKGO_FLAGS) multik8s/multik8s.test; \
	if [ "$$?" -ne 0 ]; then \
		echo "Controller logs for $(MINIKUBE_PROFILE_PRIMARY):"; \
		$${KUBECTL_PRIMARY} logs --tail 1000 -n $(CEPH_CLUSTER1_NAMESPACE) -l app.kubernetes.io/name=mantle,app.kubernetes.io/component=controller -c mantle; \
		echo ""; \
		echo "Controller logs for $(MINIKUBE_PROFILE_SECONDARY):"; \
		$${KUBECTL_SECONDARY} logs --tail 1000 -n $(CEPH_CLUSTER1_NAMESPACE) -l app.kubernetes.io/name=mantle,app.kubernetes.io/component=controller -c mantle; \
		echo ""; \
		echo "Primary MBs"; \
		$${KUBECTL_PRIMARY} get mantlebackups -A -o yaml; \
		echo ""; \
		echo "Secondary MBs"; \
		$${KUBECTL_SECONDARY} get mantlebackups -A -o yaml; \
		echo ""; \
		echo "Primary MRs"; \
		$${KUBECTL_PRIMARY} get mantlerestores -A -o yaml; \
		echo ""; \
		echo "Secondary MRs"; \
		$${KUBECTL_SECONDARY} get mantlerestores -A -o yaml; \
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
