# mantle

The system for backup RBD PVC managed by Rook/Ceph. It can also copy the backup data to another Rook/Ceph cluster in another data center.

## Description

Users can manage backups by `MantleBackup` resources.

## Getting Started

If you want to try mantle on your local machine, see [e2e](e2e/) directory which provides scripts to run mantle on [minikube].

To use mantle on your real kubernetes cluster, read following sections.

### Conformed environments

- Kubernetes cluster: v1.35, v1.36
- Rook: v1.17.2+

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

## Prometheus metrics

### `mantle_mantlebackupconfig_info`

`mantle_mantlebackupconfig_info` is a Gauge that indicates information about the backup configuration.

| Label                   | Description             |
| ----------------------- | ----------------------- |
| `persistentvolumeclaim` | The PVC name.           |
| `resource_namespace`    | The resource namespace. |

### `mantle_backup_duration_seconds`

`mantle_backup_duration_seconds` is a Histogram that indicates the time from the creationTimestamp to the completion of the backup.

| Label                   | Description             |
| ----------------------- | ----------------------- |
| `persistentvolumeclaim` | The PVC name.           |
| `resource_namespace`    | The resource namespace. |

### `mantle_backup_last_creation_timestamp_seconds`

`mantle_backup_last_creation_timestamp_seconds` is a Gauge that indicates the creationTimestamp of the newest MantleBackup. MantleBackups are counted only after the controller confirms their PVC is bound and resolves the snapshot target.

| Label                   | Description             |
| ----------------------- | ----------------------- |
| `persistentvolumeclaim` | The PVC name.           |
| `resource_namespace`    | The resource namespace. |

### `mantle_backup_last_snapshot_timestamp_seconds`

`mantle_backup_last_snapshot_timestamp_seconds` is a Gauge that indicates the latest snapshot capture time (`.status.createdAt`).

| Label                   | Description             |
| ----------------------- | ----------------------- |
| `persistentvolumeclaim` | The PVC name.           |
| `resource_namespace`    | The resource namespace. |

### `mantle_backup_oldest_unsynced_creation_timestamp_seconds`

`mantle_backup_oldest_unsynced_creation_timestamp_seconds` is a Gauge that indicates the creationTimestamp of the oldest MantleBackup whose snapshot has been captured but not synced yet. It is exported only by the primary.

| Label                   | Description             |
| ----------------------- | ----------------------- |
| `persistentvolumeclaim` | The PVC name.           |
| `resource_namespace`    | The resource namespace. |

These timestamp metrics are not exported by the secondary. A series is absent when no MantleBackup matches, and keeps its last value while the PVC is missing or re-created.

## Development

The following tools should be installed manually.

- go: v1.26.0+
- docker: 20.10+
- kubebuilder: v4.15.0+
  - required if you want to add new CRs or webhooks.

## Contributing

Currently, we are not accepting outside contributions to this repository.


## License

This project is licensed under [Apache License 2.0](LICENSE).

[minikube]: https://minikube.sigs.k8s.io/
