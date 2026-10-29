package domain //nolint:dupl,nolintlint // It differs from the primary one once the replication moves here.

import (
	"context"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// MBStandaloneReconciler reconciles MantleBackup resources on a standalone cluster.
type MBStandaloneReconciler struct {
	managedCephClusterID string
	k8sClient            MBKubernetesClient
}

// NewMBStandaloneReconciler creates a new MBStandaloneReconciler that reconciles MantleBackups
// managed by the controller in charge of the Ceph cluster managedCephClusterID.
func NewMBStandaloneReconciler(
	managedCephClusterID string,
	k8sClient MBKubernetesClient,
) *MBStandaloneReconciler {
	return &MBStandaloneReconciler{
		managedCephClusterID: managedCephClusterID,
		k8sClient:            k8sClient,
	}
}

// Provision handles the provisioning logic for a MantleBackup resource.
func (r *MBStandaloneReconciler) Provision(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) *reconcile.Result {
	if result := checkManagedBackup(ctx, backup, r.managedCephClusterID, r.k8sClient); result.ShouldReturn() {
		return result
	}

	log.FromContext(ctx).Info("starting reconciliation")

	if isCreatedWhenMantleControllerWasSecondary(backup) {
		return skipBackupCreatedAsSecondary(ctx)
	}

	return reconcile.ContinueWithLegacyReconcile()
}

// Finalize handles the finalization logic for a MantleBackup resource.
func (r *MBStandaloneReconciler) Finalize(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) *reconcile.Result {
	if result := checkManagedBackup(ctx, backup, r.managedCephClusterID, r.k8sClient); result.ShouldReturn() {
		return result
	}

	log.FromContext(ctx).Info("starting reconciliation")

	if isCreatedWhenMantleControllerWasSecondary(backup) {
		return skipBackupCreatedAsSecondary(ctx)
	}

	return reconcile.ContinueWithLegacyReconcile()
}
