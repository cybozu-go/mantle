# mantle

The system for backup RBD PVC managed by Rook/Ceph. It can also copy the backup data to another Rook/Ceph cluster in another data center.

## Description

Users can manage backups by `MantleBackup` resources.

## Getting Started

If you want to try mantle on your local machine, see [e2e](e2e/) directory which provides scripts to run mantle on [minikube].

To use mantle on your real kubernetes cluster, read following sections.

### Prerequisites
- Deploy
  - Kubernetes cluster: v1.31.1+
  - Rook: v1.16.4+
- build
  - go: v1.23.0+
  - docker: 20.10+
  - kubectl:  v1.31.1+
  - kubebuilder: 3.14.0+ 

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/mantle:tag
```

**NOTE:** This image ought to be published in the personal registry you specified. 
And it is required to have access to pull the image from the working environment. 
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/mantle:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin 
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Contributing

Currently, we are not accepting outside contributions to this repository.


## License

This project is licensed under [Apache License 2.0](LICENSE).

[minikube]: https://minikube.sigs.k8s.io/
