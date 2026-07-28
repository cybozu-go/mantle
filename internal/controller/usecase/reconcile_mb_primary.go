package usecase

import (
	"context"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// ReconcileMBPrimary reconciles MantleBackup resources on a primary cluster.
type ReconcileMBPrimary struct {
	reconciler *domain.MBPrimaryReconciler
	k8sClient  KubernetesClient
}

// NewReconcileMBPrimary creates a new ReconcileMBPrimary.
func NewReconcileMBPrimary(k8sClient KubernetesClient) *ReconcileMBPrimary {
	return &ReconcileMBPrimary{
		reconciler: domain.NewMBPrimaryReconciler(),
		k8sClient:  k8sClient,
	}
}

// Run executes the reconciliation logic for a MantleBackup resource.
func (r *ReconcileMBPrimary) Run(
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
