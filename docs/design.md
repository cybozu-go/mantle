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

      USER([User])
      RBSCM[rbd-backup-system-controller-manager]
      RPB[RBDPVCBackup]
      PVC[PersistentVolumeClaim]
      PV[PersistentVolume]
      RI[RBD Image]
      RS[RBD Snapshot]

      subgraph Kubernetes Layer
        RBSCM -- watch RBDPVCBackup --> RPB
        RPB -- specify PVC --> PVC
        PVC -- point PV --> PV
        PV -- point RBD image --> RI
      end

      subgraph Ceph Layer
        RS -- snapshot from RBD image --> RI
      end

      USER -- 1. create/delete RBDPVCBackup --> RPB
      RBSCM -- 2. take/delete snapshot --> RS

    end
```

### How rbd-backup-system works

TBD

### Details

TBD
