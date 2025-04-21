---
name: Update supporting Kubernetes
about: Dependencies relating to Kubernetes version upgrades
title: 'Update supporting Kubernetes'
labels: 'update kubernetes'
assignees: ''

---

## Update Procedure

- Read [this document](https://github.com/cybozu-go/mantle/blob/main/docs/maintenance.md).

## Before Check List

There is a checklist to confirm dependent libraries or tools are released. The release notes for Kubernetes should also be checked.

### Must Update Dependencies

Must update Kubernetes with each new version of Kubernetes.

- [ ] sigs.k8s.io/controller-runtime
  - https://github.com/kubernetes-sigs/controller-runtime/releases
- [ ] sigs.k8s.io/controller-tools
  - https://github.com/kubernetes-sigs/controller-tools/releases
- [ ] minikube
  - https://github.com/kubernetes/minikube/releases
- [ ] rook
  - https://github.com/rook/rook/releases

### Release notes check

- [ ] Read the necessary release notes for Kubernetes.

## Checklist

- [ ] Finish implementation of the issue
- [ ] Test all functions
