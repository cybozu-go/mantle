package domain

import (
	"context"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
)

// MBPrimaryReconciler reconciles MantleBackup resources on a primary cluster.
type MBPrimaryReconciler struct {
}

// NewMBPrimaryReconciler creates a new MBPrimaryReconciler.
func NewMBPrimaryReconciler() *MBPrimaryReconciler {
	return &MBPrimaryReconciler{}
}

// Provision handles the provisioning logic for a MantleBackup resource.
func (r *MBPrimaryReconciler) Provision(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) *reconcile.Result {
	if isCreatedWhenMantleControllerWasSecondary(backup) {
		return skipBackupCreatedByRemote(ctx)
	}

	return reconcile.ContinueWithLegacyReconcile()
}

// Finalize handles the finalization logic for a MantleBackup resource.
func (r *MBPrimaryReconciler) Finalize(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) *reconcile.Result {
	if isCreatedWhenMantleControllerWasSecondary(backup) {
		return skipBackupCreatedByRemote(ctx)
	}

	return reconcile.ContinueWithLegacyReconcile()
}
