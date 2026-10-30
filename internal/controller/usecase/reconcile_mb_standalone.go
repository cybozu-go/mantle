package usecase

import (
	"context"

	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"k8s.io/apimachinery/pkg/types"
)

// ReconcileMBStandalone reconciles MantleBackup resources on a standalone cluster.
type ReconcileMBStandalone struct {
	reconciler           *domain.MBStandaloneReconciler
	k8sClient            KubernetesClient
	managedCephClusterID string
}

// NewReconcileMBStandalone creates a new ReconcileMBStandalone.
func NewReconcileMBStandalone(k8sClient KubernetesClient, managedCephClusterID string) *ReconcileMBStandalone {
	return &ReconcileMBStandalone{
		reconciler:           domain.NewMBStandaloneReconciler(),
		k8sClient:            k8sClient,
		managedCephClusterID: managedCephClusterID,
	}
}

// Run executes the reconciliation logic for a MantleBackup resource.
func (r *ReconcileMBStandalone) Run(
	ctx context.Context,
	backupNamespacedName types.NamespacedName,
) *reconcile.Result {
	return runMBReconciler(
		ctx, r.k8sClient, r.managedCephClusterID, backupNamespacedName, r.reconciler, r.reconciler.Operations,
	)
}
