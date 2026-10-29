package usecase

import (
	"context"
	"fmt"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// KubernetesClient is an interface for Kubernetes client operations used by reconcilers.
type KubernetesClient interface {
	client.Client
	domain.PVCClusterIDGetter
	ApplyReconcilerOperations(ctx context.Context, operations []domain.Operation) error
}

func getResource[
	T any,
	PT interface {
		*T
		client.Object
	},
](ctx context.Context, k8sClient client.Client, name string, namespace string) (*T, error) {
	obj := new(T)

	err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, PT(obj))
	if err != nil {
		return nil, fmt.Errorf("failed to get %T %s/%s: %w", obj, namespace, name, err)
	}

	return obj, nil
}

// mbReconciler is the domain reconciler of MantleBackup for a role.
type mbReconciler interface {
	Provision(ctx context.Context, backup *mantlev1.MantleBackup) *reconcile.Result
	Finalize(ctx context.Context, backup *mantlev1.MantleBackup) *reconcile.Result
}

// runMBReconciler runs the domain reconciler of MantleBackup. It fetches the
// MantleBackup, checks if it is managed, and calls Provision or Finalize based on
// the deletion timestamp.
func runMBReconciler(
	ctx context.Context,
	k8sClient KubernetesClient,
	managedCephClusterID string,
	backupNamespacedName types.NamespacedName,
	reconciler mbReconciler,
) *reconcile.Result {
	backup, err := getResource[mantlev1.MantleBackup](
		ctx, k8sClient, backupNamespacedName.Name, backupNamespacedName.Namespace,
	)
	if err != nil {
		if aerrors.IsNotFound(err) {
			return reconcile.Succeeded()
		}

		return reconcile.Failed("failed to get MantleBackup: %w", err)
	}

	if result := domain.CheckManagedBackup(ctx, backup, managedCephClusterID, k8sClient); result.ShouldReturn() {
		return result
	}

	log.FromContext(ctx).Info("starting reconciliation")

	if backup.DeletionTimestamp.IsZero() {
		return reconciler.Provision(ctx, backup)
	}

	return reconciler.Finalize(ctx, backup)
}
