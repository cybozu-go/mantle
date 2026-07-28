package domain

import (
	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
)

// MBStandaloneReconciler reconciles MantleBackup resources on a standalone cluster.
type MBStandaloneReconciler struct {
}

// NewMBStandaloneReconciler creates a new MBStandaloneReconciler.
func NewMBStandaloneReconciler() *MBStandaloneReconciler {
	return &MBStandaloneReconciler{}
}

// Provision handles the provisioning logic for a MantleBackup resource.
func (r *MBStandaloneReconciler) Provision(
	_ *mantlev1.MantleBackup,
) *reconcile.Result {
	return reconcile.ContinueWithLegacyReconcile()
}

// Finalize handles the finalization logic for a MantleBackup resource.
func (r *MBStandaloneReconciler) Finalize(
	_ *mantlev1.MantleBackup,
) *reconcile.Result {
	return reconcile.ContinueWithLegacyReconcile()
}
