package domain

import (
	"context"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// NeedsPVCClusterIDToCheckManagedBackup reports whether CheckManagedBackup needs the
// Ceph cluster ID of the PVC specified in the MantleBackup.
func NeedsPVCClusterIDToCheckManagedBackup(backup *mantlev1.MantleBackup) bool {
	_, ok := backup.Labels[LabelClusterID]

	return !ok
}

// CheckManagedBackup checks if the MantleBackup is managed by the controller in charge of
// the Ceph cluster managedCephClusterID. It returns nil if the MantleBackup is managed,
// and a successful result to stop further processing if it is not.
//
// pvcClusterID is the Ceph cluster ID of the PVC specified in the MantleBackup, or nil if
// the PVC is not found. It is used only when NeedsPVCClusterIDToCheckManagedBackup
// returns true.
func CheckManagedBackup(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	managedCephClusterID string,
	pvcClusterID *string,
) *reconcile.Result {
	if !NeedsPVCClusterIDToCheckManagedBackup(backup) {
		if backup.Labels[LabelClusterID] == managedCephClusterID {
			return nil
		}

		return reconcile.Succeeded()
	}

	if pvcClusterID == nil {
		log.FromContext(ctx).Info("waiting for the PVC specified in the MantleBackup to be created", "pvc", backup.Spec.PVC)

		return reconcile.Requeue()
	}

	if *pvcClusterID != managedCephClusterID {
		return reconcile.Succeeded()
	}

	return nil
}
