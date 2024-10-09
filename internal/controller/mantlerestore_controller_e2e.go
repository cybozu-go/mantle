package controller

import (
	"context"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/ceph"
)

// MantleRestoreReconcilerE2E is a wrapper of MantleRestoreReconciler
// this module is for testing cloneImageFromBackup and removeRBDImage in e2e tests
type MantleRestoreReconcilerE2E struct {
	MantleRestoreReconciler
}

func NewMantleRestoreReconcilerE2E(managedCephClusterID, toolsNamespace string) *MantleRestoreReconcilerE2E {
	return &MantleRestoreReconcilerE2E{
		MantleRestoreReconciler{
			managedCephClusterID: managedCephClusterID,
			ceph:                 ceph.NewCephCmdWithTools(toolsNamespace),
		},
	}
}

func (r *MantleRestoreReconcilerE2E) CloneImageFromBackup(ctx context.Context, restore *mantlev1.MantleRestore, backup *mantlev1.MantleBackup) error {
	return r.cloneImageFromBackup(ctx, restore, backup)
}

func (r *MantleRestoreReconcilerE2E) RemoveRBDImage(ctx context.Context, restore *mantlev1.MantleRestore) error {
	return r.removeRBDImage(ctx, restore)
}
