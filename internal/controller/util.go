package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/cybozu-go/mantle/internal/ceph"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// createCloneByPV creates a clone RBD image from a snapshot of the volume bound to the given PersistentVolume (PV).
//
// This function checks if the clone image already exists in the Ceph pool:
//   - If the clone exists and is a clone of the specified snapshot, it does nothing and returns nil.
//   - If the clone exists but is not a clone of the specified snapshot, it returns an error.
//   - If the clone does not exist, it creates a new clone image from the snapshot using the specified image features.
//
// Parameters:
//
//	ctx         - context for logging and API calls
//	cephCmd     - Ceph command interface for RBD operations
//	pv          - PersistentVolume object containing CSI and image attributes
//	snapshotName- name of the snapshot to clone from
//	cloneName   - name for the new clone image
//
// Returns an error if required attributes are missing, if Ceph operations fail, or if a conflicting clone image exists.
func createCloneByPV(ctx context.Context, cephCmd ceph.CephCmd, pv *corev1.PersistentVolume, snapshotName, cloneName string) error {
	logger := log.FromContext(ctx)

	bkImage := pv.Spec.CSI.VolumeAttributes["imageName"]
	if len(bkImage) == 0 {
		return errors.New("imageName not found in PV manifest")
	}
	pool := pv.Spec.CSI.VolumeAttributes["pool"]
	if len(pool) == 0 {
		return errors.New("pool not found in PV manifest")
	}

	images, err := cephCmd.RBDLs(pool)
	if err != nil {
		return fmt.Errorf("failed to list RBD images: %w", err)
	}

	// check if the image already exists
	if slices.Contains(images, cloneName) {
		info, err := cephCmd.RBDInfo(pool, cloneName)
		if err != nil {
			return fmt.Errorf("failed to get RBD info: %w", err)
		}
		if info.Parent == nil {
			return errors.New("failed to get RBD info: parent field is empty")
		}

		if info.Parent.Pool == pool && info.Parent.Image == bkImage && info.Parent.Snapshot == snapshotName {
			logger.Info("image already exists", "image", cloneName)

			return nil
		}
		// If the clone image already exists but is not a clone of the snapshot, it returns an error.
		return fmt.Errorf("image already exists but not a clone of the backup: %s", cloneName)
	}

	features := pv.Spec.CSI.VolumeAttributes["imageFeatures"]
	if features == "" {
		features = "deep-flatten"
	} else {
		features += ",deep-flatten"
	}

	// create a clone image from the backup
	return cephCmd.RBDClone(pool, bkImage, snapshotName, cloneName, features)
}

func updateStatus(ctx context.Context, client client.Client, obj client.Object, mutator func() error) error {
	if err := client.Get(ctx, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}, obj); err != nil {
		return err
	}
	if err := mutator(); err != nil {
		return err
	}
	if err := client.Status().Update(ctx, obj); err != nil {
		return err
	}

	return nil
}

// IsJobConditionTrue returns true when the conditionType is present and set to
// `metav1.ConditionTrue`.  Otherwise, it returns false.  Note that we can't use
// meta.IsStatusConditionTrue because it doesn't accept []JobCondition.
func IsJobConditionTrue(conditions []batchv1.JobCondition, conditionType batchv1.JobConditionType) bool {
	for _, cond := range conditions {
		if cond.Type == conditionType && cond.Status == corev1.ConditionTrue {
			return true
		}
	}

	return false
}

func requeueReconciliation() ctrl.Result {
	return reconcile.RequeueAfter()
}

func deleteRBDImageAsynchronously(ceph ceph.CephCmd, pool, image string) error {
	images, err := ceph.RBDLs(pool)
	if err != nil {
		return fmt.Errorf("failed to list RBD images: %w", err)
	}

	if !slices.Contains(images, image) {
		return nil
	}

	imageInfo, err := ceph.RBDInfo(pool, image)
	if err != nil {
		return fmt.Errorf("failed to get info about the RBD image: %s/%s: %w", pool, image, err)
	}

	if err := ceph.RBDTrashMv(pool, image); err != nil {
		return fmt.Errorf("failed to move the RBD image to trash: %s/%s: %w", pool, image, err)
	}

	if err := ceph.CephRBDTaskAddTrashRemove(pool, imageInfo.ID); err != nil {
		return fmt.Errorf("failed to add task to remove the RBD image from trash: %s/%s: %w", pool, image, err)
	}

	return nil
}
