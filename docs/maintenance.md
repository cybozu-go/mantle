# Maintenance Guide

## How to change the supported Kubernetes minor versions

Mantle depends on some Kubernetes repositories like `k8s.io/client-go` and supports two Kubernetes versions at a time.

Issues and PRs related the latest upgrade task also help you understand how to upgrade the supported versions, so checking them together with this guide is recommended when you do this task.

### Upgrade procedure

#### Kubernetes

When upgrading, add the new version and drop the oldest to keep supporting two consecutive minor versions.

Choose the appropriate versions and check the [release note](https://kubernetes.io/docs/setup/release/notes/).

Update the supported Kubernetes minor versions in `README.md`.

We should also update go.mod. According to [the Kubebuilder documentation](https://book.kubebuilder.io/versions_compatibility_supportability), we should use versions compatible with Kubebuilder, so refer to the samples in the testdata directory of the latest Kubebuilder release that supports the target Kubernetes minor (e.g., `https://github.com/kubernetes-sigs/kubebuilder/blob/<kubebuilder-release-tag>/testdata/project-v4/go.mod`) to see which versions should be used.

First, update `k8s.io/*` libraries. Please note that `k8s.io/api`, `k8s.io/apimachinery`, and `k8s.io/client-go` use `v0.x.x` tags (e.g., v1.35.4 → v0.35.4), while `k8s.io/kubernetes` uses `v1.x.x` tags directly.

```bash
$ VERSION=0.<minor>.<patch>  # e.g. 0.35.4 for Kubernetes 1.35.4
$ go get k8s.io/api@v${VERSION} k8s.io/apimachinery@v${VERSION} k8s.io/client-go@v${VERSION}
$ go get k8s.io/kubernetes@v1.<minor>.<patch>  # e.g. v1.35.4
```

Next, update controller-runtime to the version in the `go.mod` file in that Kubebuilder release's testdata directory. For example, see `https://github.com/kubernetes-sigs/kubebuilder/blob/<kubebuilder-release-tag>/testdata/project-v4/go.mod`. Before updating it, please read the [`controller-runtime`'s release note](https://github.com/kubernetes-sigs/controller-runtime/releases). If there are breaking changes, we should decide how to manage these changes.

```
$ VERSION=<upgrading controller-runtime version>
$ go get sigs.k8s.io/controller-runtime@v${VERSION}
```

Finally, update controller-tools to the version in the `Makefile` in that Kubebuilder release's testdata directory. For example, see `https://github.com/kubernetes-sigs/kubebuilder/blob/<kubebuilder-release-tag>/testdata/project-v4/Makefile`. Before updating it, please read the [`controller-tools`'s release note](https://github.com/kubernetes-sigs/controller-tools/releases). If there are breaking changes, we should decide how to manage these changes.
To change the version, edit `versions.mk`.

#### Go

Choose the version compatible with Kubebuilder (e.g., `https://github.com/kubernetes-sigs/kubebuilder/blob/<kubebuilder-release-tag>/testdata/project-v4/go.mod#L3`).

Edit the following files.

- `go.mod`
- `Dockerfile`
- `README.md`

#### Depending tools

Update all the tools in `versions.mk` to their latest versions, except Rook and Ceph.
To change their versions, edit `versions.mk`.
Rook and Ceph are updated separately. See [How to update Rook and Ceph](#how-to-update-rook-and-ceph).

For minikube, after choosing a Minikube version, check the Kubernetes versions it supports in `https://github.com/kubernetes/minikube/blob/<minikube-release-tag>/pkg/minikube/constants/constants_kubernetes_versions.go`. For each target Kubernetes minor, set its newest stable patch version in the `kubernetes-version` matrix in `.github/workflows/e2e.yaml` and `.github/workflows/e2e-multiple-k8s-clusters.yaml`. Also set `KUBERNETES_VERSION` in `versions.mk`.

Update the following versions in Dockerfile, if necessary, too:

- [s5cmd](https://github.com/peak/s5cmd/releases)

#### Depending modules

Read Kubernetes's `go.mod` at https://github.com/kubernetes/kubernetes/blob/main/go.mod (replace `main` with the target release branch, such as `release-1.36`) and update the `prometheus/*` modules to the versions listed there. Here is the example to update `prometheus/client_golang`.

```
$ VERSION=<upgrading prometheus-related libraries release version>
$ go get github.com/prometheus/client_golang@v${VERSION}
```

Update `k8s.io/kube-openapi` and `k8s.io/utils` to the versions in Kubernetes's `go.mod`.

```
$ go get k8s.io/kube-openapi@<kube-openapi version in Kubernetes go.mod> k8s.io/utils@<k8s.io/utils version in Kubernetes go.mod>
```

Then, please tidy up the dependencies.

```bash
$ go mod tidy
```

Regenerate manifests using new controller-tools.

```console
$ sudo rm -rf bin
$ make generate
```

#### Final check

`git grep <the kubernetes version which support will be dropped>`, `git grep image:`, `git grep -i VERSION` and looking `versions.mk` might help to avoid overlooking necessary changes.

## How to update Rook and Ceph

### Rook

Check the [Rook release notes](https://github.com/rook/rook/releases) and update `ROOK_CHART_VERSION` in `versions.mk`.
Update the minimum Rook version in `README.md` if Mantle no longer works with the version listed there.

### Ceph

Choose a Ceph version listed as supported in the Rook documentation for the target Rook version.

1. In one PR, set the Ceph image tag (`v<x.y.z>`) in the following files, and update `ceph/Dockerfile` and `ceph/export-diff.patch` by following "Procedure to update" in [ceph/README.md](../ceph/README.md). `make -C ceph test` uses all of them.
   - `test/e2e/testdata/values-cluster.yaml`
   - `ceph/manifests/toolbox.yaml`
   - `config/manager/manager.yaml`
2. After the PR is merged, release the custom `ceph rbd export-diff` command by following [Release](../ceph/README.md#release).
3. After the release, update `EXPORT_DIFF_VERSION` in `Dockerfile` to the released `ceph-export-diff` version (`<x.y.z.n>`) in another PR. Also update the `custom-ceph` stage if necessary by referring to the [Dockerfile in neco-containers](https://github.com/cybozu/neco-containers/blob/main/ceph/Dockerfile), and update the commit in the "cf." comment above the stage.
4. Run `git grep -F <the old Ceph version>` (`<old x.y.z>`) to make sure no old version remains.
