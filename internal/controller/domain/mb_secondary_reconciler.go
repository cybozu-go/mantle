package domain

import (
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
	_ *mantlev1.MantleBackup,
) *reconcile.Result {
	return reconcile.ContinueWithLegacyReconcile()
}

// Finalize handles the finalization logic for a MantleBackup resource.
func (r *MBSecondaryReconciler) Finalize(
	_ *mantlev1.MantleBackup,
) *reconcile.Result {
	return reconcile.ContinueWithLegacyReconcile()
}
