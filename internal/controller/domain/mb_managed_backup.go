package domain

import (
	"context"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// PVCClusterIDGetter gets the Ceph cluster ID of a PVC.
type PVCClusterIDGetter interface {
	// GetCephClusterIDOfPVC returns the Ceph cluster ID of the PVC namespace/name. The
	// cluster ID is an empty string if the PVC is not managed by any Ceph cluster.
	// found is false if the PVC does not exist.
	GetCephClusterIDOfPVC(ctx context.Context, namespace, name string) (clusterID string, found bool, err error)
}

// checkManagedBackup checks if the MantleBackup is managed by the controller in charge of
// the Ceph cluster managedCephClusterID. It returns nil and logs the start of the
// reconciliation if the MantleBackup is managed, and returns a successful result to stop
// further processing if it is not.
//
// If the MantleBackup doesn't have the cluster-id label, the Ceph cluster ID of the PVC
// specified in the MantleBackup decides it. pvcClusterIDGetter is called only in this case.
func checkManagedBackup(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	managedCephClusterID string,
	pvcClusterIDGetter PVCClusterIDGetter,
) *reconcile.Result {
	if clusterID, ok := backup.Labels[LabelClusterID]; ok {
		if clusterID == managedCephClusterID {
			return startReconciliation(ctx)
		}

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

	if pvcClusterID != managedCephClusterID {
		return reconcile.Succeeded()
	}

	return startReconciliation(ctx)
}

func startReconciliation(ctx context.Context) *reconcile.Result {
	log.FromContext(ctx).Info("starting reconciliation")

	return nil
}
