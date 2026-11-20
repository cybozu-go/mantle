package domain

import (
	"context"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// PVCClusterIDGetter gets the Ceph cluster ID of a PVC.
type PVCClusterIDGetter interface { //nolint:iface // MBKubernetesClient embeds it and gets more methods later.
	// GetCephClusterIDOfPVC returns the Ceph cluster ID of the PVC namespace/name. The
	// cluster ID is an empty string if the PVC is not managed by any Ceph cluster.
	// found is false if the PVC does not exist.
	GetCephClusterIDOfPVC(ctx context.Context, namespace, name string) (clusterID string, found bool, err error)
}

// checkManagedBackup checks if the MantleBackup resource is managed by this controller.
func checkManagedBackup(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	managedCephClusterID string,
	pvcClusterIDGetter PVCClusterIDGetter,
) *reconcile.Result {
	// If the MantleBackup resource has a label indicating that it is managed by this controller.
	if clusterID, ok := backup.Labels[LabelClusterID]; ok {
		if clusterID == managedCephClusterID {
			return nil
		}

		// The MantleBackup resource is managed by a different controller, so we return a successful reconcile
		// result to stop further processing.
		return reconcile.Succeeded()
	}

	pvcClusterID, found, err := pvcClusterIDGetter.GetCephClusterIDOfPVC(ctx, backup.Namespace, backup.Spec.PVC)
	if err != nil {
		return reconcile.Failed("failed to get the Ceph cluster ID of the PVC specified in the MantleBackup: %w", err)
	}

	if !found {
		log.FromContext(ctx).Info("waiting for the PVC specified in the MantleBackup to be created", "pvc", backup.Spec.PVC)

		return reconcile.Requeue()
	}

	// If the cluster ID of the PVC does not match the managed Ceph cluster ID, it means that the PVC is not
	// managed by this controller.
	// In this case, we return a successful reconcile result to stop further processing.
	if pvcClusterID != managedCephClusterID {
		return reconcile.Succeeded()
	}

	return nil
}
