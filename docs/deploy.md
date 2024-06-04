# How to deploy Mantle to DCs

## Overview

Mantle is packaged as two separate helm charts:

- `mantle-cluster-wide` chart: It introduces cluster-wide resources such as CRDs and ClusterRoles.
- `mantle` chart: It introduces namespaced resources such as Deployments and ServiceAccounts.

Only one `mantle-cluster-wide` chart should be deployed in one k8s cluster, while `mantle` chart should be deployed for each required namespace.

## Installation instructions

First, add the Mantle's helm repository:

```
helm repo add mantle https://cybozu-go.github.io/mantle
helm repo update
```

> [!NOTE]
> The Mantle's Helm repository has **NOT** been published yet, so the instruction above doesn't run for now. We're planning to publish it soon.

Then, install `mantle-cluster-wide` chart:
```
helm install --wait mantle-cluster-wide mantle/mantle-cluster-wide
```

Next, install `mantle` chart. It should be installed for each required namespace. In this instruction, we assume that it is necessary to deploy Mantle to `rook-ceph` namespace only:

```
helm install --wait --namespace=rook-ceph mantle mantle/mantle
```

Finally, check that Mantle is running:

```
kubectl get pod -n rook-ceph
```

## See also

`e2e/Makefile` may help you install Mantle, because the e2e tests use the Helm charts to set up the testing environment. 
