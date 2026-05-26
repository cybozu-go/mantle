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

- [ ] sigs.k8s.io/controller-runtime
  - https://github.com/kubernetes-sigs/controller-runtime/releases
- [ ] sigs.k8s.io/controller-tools
  - https://github.com/kubernetes-sigs/controller-tools/releases
- [ ] minikube
  - https://github.com/kubernetes/minikube/releases
- [ ] rook
  - https://github.com/rook/rook/releases
  - Only need to confirm that upstream Rook has released a version that supports the target Kubernetes version.

### Additional Checks

- [ ] Read the necessary release notes for Kubernetes.
- [ ] Ready to support our product.

## Completion Checks

- [ ] Finish implementation of the issue
- [ ] Test all functions
