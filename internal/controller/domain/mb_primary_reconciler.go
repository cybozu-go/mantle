package domain //nolint:dupl,nolintlint // It differs from the standalone one once the replication moves here.

import (
	"context"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
)

// MBPrimaryReconciler reconciles MantleBackup resources on a primary cluster.
type MBPrimaryReconciler struct {
	Operations           *ReconcilerOperations
	managedCephClusterID string
	pvcClusterIDGetter   PVCClusterIDGetter
}

// NewMBPrimaryReconciler creates a new MBPrimaryReconciler that reconciles MantleBackups
// managed by the controller in charge of the Ceph cluster managedCephClusterID.
func NewMBPrimaryReconciler(
	managedCephClusterID string,
	pvcClusterIDGetter PVCClusterIDGetter,
) *MBPrimaryReconciler {
	return &MBPrimaryReconciler{
		Operations:           NewReconcilerOperations(),
		managedCephClusterID: managedCephClusterID,
		pvcClusterIDGetter:   pvcClusterIDGetter,
	}
}

// Provision handles the provisioning logic for a MantleBackup resource.
func (r *MBPrimaryReconciler) Provision(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) *reconcile.Result {
	if result := checkManagedBackup(ctx, backup, r.managedCephClusterID, r.pvcClusterIDGetter); result.ShouldReturn() {
		return result
	}

	if isCreatedWhenMantleControllerWasSecondary(backup) {
		return skipBackupCreatedAsSecondary(ctx)
	}

	if result := expire(ctx, backup, r.Operations); result.ShouldReturn() {
		return result.WrapIfError("failed to expire backup")
	}

	return reconcile.ContinueWithLegacyReconcile()
}

// Finalize handles the finalization logic for a MantleBackup resource.
func (r *MBPrimaryReconciler) Finalize(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) *reconcile.Result {
	if result := checkManagedBackup(ctx, backup, r.managedCephClusterID, r.pvcClusterIDGetter); result.ShouldReturn() {
		return result
	}

	if isCreatedWhenMantleControllerWasSecondary(backup) {
		return skipBackupCreatedAsSecondary(ctx)
	}

	return reconcile.ContinueWithLegacyReconcile()
}
