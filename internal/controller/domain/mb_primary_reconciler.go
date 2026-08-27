package domain

import (
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
	_ *mantlev1.MantleBackup,
) *reconcile.Result {
	return reconcile.ContinueWithLegacyReconcile()
}

// Finalize handles the finalization logic for a MantleBackup resource.
func (r *MBPrimaryReconciler) Finalize(
	_ *mantlev1.MantleBackup,
) *reconcile.Result {
	return reconcile.ContinueWithLegacyReconcile()
}
