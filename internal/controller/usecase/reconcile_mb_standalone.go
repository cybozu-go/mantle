package usecase

import (
	"context"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// ReconcileMBStandalone reconciles MantleBackup resources on a standalone cluster.
type ReconcileMBStandalone struct {
	reconciler *domain.MBStandaloneReconciler
	k8sClient  KubernetesClient
}

// NewReconcileMBStandalone creates a new ReconcileMBStandalone.
func NewReconcileMBStandalone(k8sClient KubernetesClient) *ReconcileMBStandalone {
	return &ReconcileMBStandalone{
		reconciler: domain.NewMBStandaloneReconciler(),
		k8sClient:  k8sClient,
	}
}

// Run executes the reconciliation logic for a MantleBackup resource.
func (r *ReconcileMBStandalone) Run(
	ctx context.Context,
	backupNamespacedName types.NamespacedName,
) *reconcile.Result {
	backup, err := getResource[mantlev1.MantleBackup](
		ctx, r.k8sClient, backupNamespacedName.Name, backupNamespacedName.Namespace,
	)
	if err != nil {
		if aerrors.IsNotFound(err) {
			return reconcile.Succeeded()
		}

		return reconcile.Failed("failed to get MantleBackup: %w", err)
	}

	if backup.DeletionTimestamp.IsZero() {
		return r.reconciler.Provision(backup)
	}

	return r.reconciler.Finalize(backup)
}
