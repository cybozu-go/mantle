package usecase

import (
	"context"

	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"k8s.io/apimachinery/pkg/types"
)

// ReconcileMBSecondary reconciles MantleBackup resources on a secondary cluster.
type ReconcileMBSecondary struct {
	reconciler           *domain.MBSecondaryReconciler
	k8sClient            KubernetesClient
	managedCephClusterID string
}

// NewReconcileMBSecondary creates a new ReconcileMBSecondary.
func NewReconcileMBSecondary(k8sClient KubernetesClient, managedCephClusterID string) *ReconcileMBSecondary {
	return &ReconcileMBSecondary{
		reconciler:           domain.NewMBSecondaryReconciler(),
		k8sClient:            k8sClient,
		managedCephClusterID: managedCephClusterID,
	}
}

// Run executes the reconciliation logic for a MantleBackup resource.
func (r *ReconcileMBSecondary) Run(
	ctx context.Context,
	backupNamespacedName types.NamespacedName,
) *reconcile.Result {
	return runMBReconciler(
		ctx, r.k8sClient, r.managedCephClusterID, backupNamespacedName, r.reconciler, r.reconciler.Operations,
	)
}
