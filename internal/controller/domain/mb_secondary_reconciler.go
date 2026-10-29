package domain

import (
	"context"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// MBSecondaryReconciler reconciles MantleBackup resources on a secondary cluster.
type MBSecondaryReconciler struct {
	Operations *ReconcilerOperations
}

// NewMBSecondaryReconciler creates a new MBSecondaryReconciler.
func NewMBSecondaryReconciler() *MBSecondaryReconciler {
	return &MBSecondaryReconciler{
		Operations: NewReconcilerOperations(),
	}
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

// skipBackupNotCreatedAsSecondary stops the reconciliation of a MantleBackup on a secondary
// cluster that was not created by this mantle-controller as a secondary.
func skipBackupNotCreatedAsSecondary(ctx context.Context) *reconcile.Result {
	log.FromContext(ctx).Info(
		"skipping to reconcile the MantleBackup created by a different mantle-controller to prevent accidental data loss",
	)

	return reconcile.Succeeded()
}
