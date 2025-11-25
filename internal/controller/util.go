package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/cybozu-go/mantle/internal/ceph"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var errEmptyClusterID error = errors.New("cluster ID is empty")

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
		return fmt.Errorf("imageName not found in PV manifest")
	}
	pool := pv.Spec.CSI.VolumeAttributes["pool"]
	if len(pool) == 0 {
		return fmt.Errorf("pool not found in PV manifest")
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
			return fmt.Errorf("failed to get RBD info: parent field is empty")
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

func getCephClusterIDFromSCName(ctx context.Context, k8sClient client.Client, storageClassName string) (string, error) {
	var storageClass storagev1.StorageClass
	err := k8sClient.Get(ctx, types.NamespacedName{Name: storageClassName}, &storageClass)
	if err != nil {
		return "", fmt.Errorf("failed to get StorageClass: %s: %w", storageClassName, err)
	}

	// Check if the MantleBackup resource being reconciled is managed by the CephCluster we are in charge of.
	if !strings.HasSuffix(storageClass.Provisioner, ".rbd.csi.ceph.com") {
		return "", fmt.Errorf("SC is not managed by RBD: %s: %w", storageClassName, errEmptyClusterID)
	}
	clusterID, ok := storageClass.Parameters["clusterID"]
	if !ok {
		return "", fmt.Errorf("clusterID not found: %s: %w", storageClassName, errEmptyClusterID)
	}

	return clusterID, nil
}

func getCephClusterIDFromPVC(ctx context.Context, k8sClient client.Client, pvc *corev1.PersistentVolumeClaim) (string, error) {
	logger := log.FromContext(ctx)

	storageClassName := pvc.Spec.StorageClassName
	if storageClassName == nil {
		logger.Info("not managed storage class", "namespace", pvc.Namespace, "pvc", pvc.Name)
		return "", nil
	}

	clusterID, err := getCephClusterIDFromSCName(ctx, k8sClient, *storageClassName)
	if err != nil {
		logger.Info("failed to get ceph cluster ID from StorageClass name",
			"error", err, "namespace", pvc.Namespace, "pvc", pvc.Name, "storageClassName", *storageClassName)
		if errors.Is(err, errEmptyClusterID) {
			return "", nil
		}
		return "", err
	}

	return clusterID, nil
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
	requeueAfter := os.Getenv("REQUEUE_RECONCILIATION_AFTER")
	if requeueAfter != "" {
		duration, err := time.ParseDuration(requeueAfter)
		if err != nil {
			panic(fmt.Sprintf("set REQUEUE_RECONCILIATION_AFTER properly: %v", err))
		}
		return ctrl.Result{RequeueAfter: duration}
	}
	return ctrl.Result{Requeue: true}
}
