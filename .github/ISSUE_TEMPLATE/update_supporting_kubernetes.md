---
name: Update supporting Kubernetes
about: Dependencies relating to Kubernetes version upgrades
title: 'Update supporting Kubernetes'
labels: 'update kubernetes'
assignees: ''

---

## Update Procedure

- Read [this document](https://github.com/cybozu-go/mantle/blob/main/docs/maintenance.md).

## Preconditions

### Update Dependencies

Must update Kubernetes with each new version of Kubernetes.

- [ ] minikube
  - https://github.com/kubernetes/minikube/releases
- [ ] kubebuilder
  - https://github.com/kubernetes-sigs/kubebuilder/releases

### Additional Checks

- [ ] Read the necessary release notes for Kubernetes.
- [ ] Ready to support our product.

## Completion Checks

- [ ] Finish implementation of the issue
- [ ] Test all functions
