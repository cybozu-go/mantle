package usecase

import (
	"context"
	"fmt"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	corev1 "k8s.io/api/core/v1"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// KubernetesClient is an interface for Kubernetes client operations used by reconcilers.
type KubernetesClient interface {
	client.Client
	ApplyReconcilerOperations(ctx context.Context, operations []domain.Operation) error
	// GetCephClusterIDFromPVC returns the Ceph cluster ID of the PVC, or an empty string
	// if the PVC is not managed by any Ceph cluster.
	GetCephClusterIDFromPVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (string, error)
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

// checkManagedBackup checks if the MantleBackup is managed by the controller in charge
// of the Ceph cluster managedCephClusterID. It returns nil if the MantleBackup is managed.
func checkManagedBackup(
	ctx context.Context,
	k8sClient KubernetesClient,
	managedCephClusterID string,
	backup *mantlev1.MantleBackup,
) *reconcile.Result {
	var pvcClusterID *string

	if domain.NeedsPVCClusterIDToCheckManagedBackup(backup) {
		pvc, err := getResource[corev1.PersistentVolumeClaim](ctx, k8sClient, backup.Spec.PVC, backup.Namespace)
		if err != nil && !aerrors.IsNotFound(err) {
			return reconcile.Failed("failed to get the PVC specified in the MantleBackup resource: %w", err)
		}

		if err == nil {
			clusterID, err := k8sClient.GetCephClusterIDFromPVC(ctx, pvc)
			if err != nil {
				return reconcile.Failed("failed to get the Ceph cluster ID from the PVC: %w", err)
			}

			pvcClusterID = &clusterID
		}
	}

	return domain.CheckManagedBackup(ctx, backup, managedCephClusterID, pvcClusterID)
}

// mbReconciler is the domain reconciler of MantleBackup for a role.
type mbReconciler interface {
	Provision(ctx context.Context, backup *mantlev1.MantleBackup) *reconcile.Result
	Finalize(ctx context.Context, backup *mantlev1.MantleBackup) *reconcile.Result
}

// runMBReconciler runs the domain reconciler of MantleBackup. It fetches the
// MantleBackup, checks if it is managed, calls Provision or Finalize based on the
// deletion timestamp, and applies the operations that the reconciler issued.
func runMBReconciler(
	ctx context.Context,
	k8sClient KubernetesClient,
	managedCephClusterID string,
	backupNamespacedName types.NamespacedName,
	reconciler mbReconciler,
	operations *domain.ReconcilerOperations,
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

	if result := checkManagedBackup(ctx, k8sClient, managedCephClusterID, backup); result.ShouldReturn() {
		return result
	}

	log.FromContext(ctx).Info("starting reconciliation")

	_ = operations.TakeAll()

	var result *reconcile.Result
	if backup.DeletionTimestamp.IsZero() {
		result = reconciler.Provision(ctx, backup)
	} else {
		result = reconciler.Finalize(ctx, backup)
	}

	err = k8sClient.ApplyReconcilerOperations(ctx, operations.TakeAll())
	if err != nil {
		return reconcile.Failed("failed to apply reconciler operations: %w", err)
	}

	return result
}
