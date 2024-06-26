# Design notes

## Motivation

We want to backup and restore RBD PVCs managed by a Rook/Ceph cluster, either by user operation or by periodic automatic processes. Also, we want to copy backup data to another Rook/Ceph cluster in another data center.

## Goal

1. Backup arbitrary RBD PVCs.
2. Restore RBD PVCs from backups.
3. Backup arbitary RBD PVCs periodically.
4. Copy backup data to another cluster in another data center.

Currently, only 1 is implemented. Other goals will be achieved later.

## Architecture

```mermaid
%%{init:{'theme': 'default'}}%%

flowchart LR

    style Architecture fill:#FFFFFF
    subgraph Architecture

      USER([User])
      RBSC[mantle-controller]
      RPB[MantleBackup]
      PVC[PersistentVolumeClaim]
      PV[PersistentVolume]
      RI[RBD Image]
      RS[RBD Snapshot]

      subgraph Kubernetes Layer
        USER -- create/delete --> RPB
        RBSC -- watch --> RPB
        RPB -- point --> PVC
        PVC -- consume --> PV
      end

      subgraph Ceph Layer
        RBSC -- create/delete --> RS
        PV -- point --> RI
        RS -- point --> RI
        
      end


    end
```

mantle-controller exists for each Rook/Ceph cluster.

### Backup flow

To create/delete a backup, mantle works as follows:

1. Users create/delete `MantleBackup`.
2. rbd-backupsystem-controller (the controller) gets the target PVC from `MantleBackup`.
3. The controller gets the PV from the target PVC.
4. The controller gets the RBD image name and pool name from the PV.
5. The controller creates/deletes an RBD snapshot corresponding to the backup in the target RBD image.

### Definitions

The definition `MantleBackup` is as follows:

```yaml
apiVersion: mantle.cybozu.io/v1
kind: MantleBackup
metadata:
  name: <MantleBackup resource name>
spec:
  # The name of the backup target PVC
  pvc: <target PVC name>
status:
  conditions:
    # The corresponding backup data is ready to use if `status` is "True"
    - type: "ReadyToUse"
      status: "True"
```
