package domain

import (
	"context"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
)

// MBSecondaryReconciler reconciles MantleBackup resources on a secondary cluster.
type MBSecondaryReconciler struct {
}

// NewMBSecondaryReconciler creates a new MBSecondaryReconciler.
func NewMBSecondaryReconciler() *MBSecondaryReconciler {
	return &MBSecondaryReconciler{}
}

// Provision handles the provisioning logic for a MantleBackup resource.
func (r *MBSecondaryReconciler) Provision(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) *reconcile.Result {
	if !isCreatedWhenMantleControllerWasSecondary(backup) {
		return skipBackupNotCreatedAsSecondary(ctx)
	}

	return reconcile.ContinueWithLegacyReconcile()
}

// Finalize handles the finalization logic for a MantleBackup resource.
func (r *MBSecondaryReconciler) Finalize(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) *reconcile.Result {
	if !isCreatedWhenMantleControllerWasSecondary(backup) {
		return skipBackupNotCreatedAsSecondary(ctx)
	}

	return reconcile.ContinueWithLegacyReconcile()
}
