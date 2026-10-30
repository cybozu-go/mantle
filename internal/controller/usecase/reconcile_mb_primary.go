package usecase

import (
	"context"

	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"k8s.io/apimachinery/pkg/types"
)

// ReconcileMBPrimary reconciles MantleBackup resources on a primary cluster.
type ReconcileMBPrimary struct {
	reconciler           *domain.MBPrimaryReconciler
	k8sClient            KubernetesClient
	managedCephClusterID string
}

// NewReconcileMBPrimary creates a new ReconcileMBPrimary.
func NewReconcileMBPrimary(k8sClient KubernetesClient, managedCephClusterID string) *ReconcileMBPrimary {
	return &ReconcileMBPrimary{
		reconciler:           domain.NewMBPrimaryReconciler(),
		k8sClient:            k8sClient,
		managedCephClusterID: managedCephClusterID,
	}
}

// Run executes the reconciliation logic for a MantleBackup resource.
func (r *ReconcileMBPrimary) Run(
	ctx context.Context,
	backupNamespacedName types.NamespacedName,
) *reconcile.Result {
	return runMBReconciler(
		ctx, r.k8sClient, r.managedCephClusterID, backupNamespacedName, r.reconciler, r.reconciler.Operations,
	)
}
