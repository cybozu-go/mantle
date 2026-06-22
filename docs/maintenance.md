# Maintenance Guide

## How to change the supported Kubernetes minor versions

Mantle depends on some Kubernetes repositories like `k8s.io/client-go` and supports two Kubernetes versions at a time.

Issues and PRs related the latest upgrade task also help you understand how to upgrade the supported versions, so checking them together with this guide is recommended when you do this task.

### Upgrade procedure

#### Kubernetes

When upgrading, add the new version and drop the oldest to keep supporting two consecutive minor versions.

Choose the appropriate versions and check the [release note](https://kubernetes.io/docs/setup/release/notes/).

To change the versions, edit the following files.

- `.github/workflows/e2e.yaml` (update the `kubernetes-version` entries in the matrix)
- `.github/workflows/e2e-multiple-k8s-clusters.yaml` (update the `kubernetes-version` entries in the matrix)
- `README.md`
- `versions.mk`

We should also update go.mod. According to [the Kubebuilder documentation](https://book.kubebuilder.io/versions_compatibility_supportability), we should use versions compatible with Kubebuilder, so refer to the samples in the latest Kubebuilder testdata directory (e.g., https://github.com/kubernetes-sigs/kubebuilder/blob/v4.1.1/testdata/project-v4/go.mod#L8-L11 and https://github.com/kubernetes-sigs/kubebuilder/blob/v4.1.1/testdata/project-v4/Makefile#L162) to see which versions should be used.

First, update `k8s.io/*` libraries. Please note that `k8s.io/api`, `k8s.io/apimachinery`, and `k8s.io/client-go` use `v0.x.x` tags (e.g., v1.35.4 → v0.35.4), while `k8s.io/kubernetes` uses `v1.x.x` tags directly.

```bash
$ VERSION=0.<minor>.<patch>  # e.g. 0.35.4 for Kubernetes 1.35.4
$ go get k8s.io/api@v${VERSION} k8s.io/apimachinery@v${VERSION} k8s.io/client-go@v${VERSION}
$ go get k8s.io/kubernetes@v1.<minor>.<patch>  # e.g. v1.35.4
```

Next, update controller-runtime by the following command. Before updating it, please read the [`controller-runtime`'s release note](https://github.com/kubernetes-sigs/controller-runtime/releases). If there are breaking changes, we should decide how to manage these changes.

```
$ VERSION=<upgrading controller-runtime version>
$ go get sigs.k8s.io/controller-runtime@v${VERSION}
```

Finally, update controller-tools. Before updating it, please read the [`controller-tools`'s release note](https://github.com/kubernetes-sigs/controller-tools/releases). If there are breaking changes, we should decide how to manage these changes.
To change the version, edit `versions.mk`.

#### Go

Choose the version compatible with Kubebuilder (e.g., https://github.com/kubernetes-sigs/kubebuilder/blob/v4.1.1/testdata/project-v4/go.mod#L3).

Edit the following files.

- `go.mod`
- `Dockerfile`
- `README.md`

#### Depending tools

The following tools don't depend on other software, so use the latest versions.
To change their versions, edit `versions.mk`.

- [helm](https://github.com/helm/helm/releases)
- [kustomize](https://github.com/kubernetes-sigs/kustomize/releases)
- [minikube](https://github.com/kubernetes/minikube/releases)
- [protoc](https://github.com/protocolbuffers/protobuf/releases)
- [Rook](https://github.com/rook/rook/releases)
- [golanci-lint](https://github.com/golangci/golangci-lint/releases)
- [cert-manager](https://github.com/cert-manager/cert-manager/releases)

Update the following versions in Dockerfile, if necessary, too:

- [s5cmd](https://github.com/peak/s5cmd/releases)
- custom rbd-export-diff

#### Depending modules

Read Kubernetes's `go.mod`(https://github.com/kubernetes/kubernetes/blob/<upgrading Kubernetes release version\>/go.mod), and update the `prometheus/*` modules. Here is the example to update `prometheus/client_golang`.

```
$ VERSION=<upgrading prometheus-related libraries release version>
$ go get github.com/prometheus/client_golang@v${VERSION}
```

The following modules don't depend on other softwares, so use their latest versions:

```
go get \
    github.com/aws/aws-sdk-go-v2@latest \
    github.com/aws/aws-sdk-go-v2/config@latest \
    github.com/aws/aws-sdk-go-v2/service/s3@latest \
    github.com/grpc-ecosystem/go-grpc-middleware/v2@latest \
    github.com/onsi/ginkgo/v2@latest \
    github.com/onsi/gomega@latest \
    github.com/pseudomuto/protoc-gen-doc@latest \
    github.com/spf13/cobra@latest \
    go.uber.org/mock@latest \
    golang.org/x/time@latest \
    google.golang.org/grpc@latest \
    google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest \
    google.golang.org/protobuf@latest \
    k8s.io/kube-openapi@latest \
    k8s.io/utils@latest \
    sigs.k8s.io/yaml@latest
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
