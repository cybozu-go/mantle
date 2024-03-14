# Design notes

## Motivation

We want to back up volumes, either by user operation or by periodic automatic processes. Also, we want to perform replication to and from another cluster.

## Goal

(Currently only has the ability to manually take backups.)

- Users can back up volumes for each PVC.

## Target

- Ceph Block Device (RBD)

## Architecture

```mermaid
%%{init:{'theme': 'default'}}%%

flowchart LR

    style Architecture fill:#FFFFFF
    subgraph Architecture

      classDef storageComponent fill:#FFFF00
      classDef component fill:#ADD8E6

      style PVCA fill:#90EE90
      style storage fill:#FFFFE0
      style Node fill-opacity:0

      CD[CSI Driver]:::storageComponent
      PVC[PersistentVolumeClaim]:::storageComponent
      SC[StorageClass]:::storageComponent
      PVCA[pvc-autoresizer]

      kubelet:::component
      Prometheus:::component
      kube-apiserver:::component

      subgraph Node
        kubelet
        storage
      end

      subgraph kube-apiserver
        SC --- PVC
      end

      CD -- watch PVC --> PVC
      CD -- expand volume --> storage[(Storage)]
      Prometheus -- get metrics --> kubelet

      PVCA -- 1. get target PVCs --> PVC
      PVCA -- 2. get SCs --> SC
      PVCA -- 3. get metrics of storage capacity --> Prometheus
      PVCA -- 4. expand storage capacity --> PVC

    end
```

### How rbd-backup-system works

TBD

### Details

TBD
