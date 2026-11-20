package domain //nolint:dupl,nolintlint // It differs from the standalone one once the replication moves here.

import (
	"context"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// MBPrimaryReconciler reconciles MantleBackup resources on a primary cluster.
type MBPrimaryReconciler struct {
	managedCephClusterID string
	k8sClient            MBKubernetesClient
}

// NewMBPrimaryReconciler creates a new MBPrimaryReconciler that reconciles MantleBackups
// managed by the controller in charge of the Ceph cluster managedCephClusterID.
func NewMBPrimaryReconciler(
	managedCephClusterID string,
	k8sClient MBKubernetesClient,
) *MBPrimaryReconciler {
	return &MBPrimaryReconciler{
		managedCephClusterID: managedCephClusterID,
		k8sClient:            k8sClient,
	}
}

// Provision handles the provisioning logic for a MantleBackup resource.
func (r *MBPrimaryReconciler) Provision(
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
func (r *MBPrimaryReconciler) Finalize(
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
