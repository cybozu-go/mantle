package infra

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var errEmptyClusterID = errors.New("cluster ID is empty")

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

// GetCephClusterIDFromPVC returns the Ceph cluster ID of the StorageClass of the PVC.
// It returns an empty string if the PVC is not managed by any Ceph cluster.
func GetCephClusterIDFromPVC(ctx context.Context, k8sClient client.Client, pvc *corev1.PersistentVolumeClaim) (string, error) {
	logger := log.FromContext(ctx)

	storageClassName := pvc.Spec.StorageClassName
	if storageClassName == nil {
		logger.Info("not managed storage class", "pvcNamespace", pvc.Namespace, "pvc", pvc.Name)

		return "", nil
	}

	clusterID, err := getCephClusterIDFromSCName(ctx, k8sClient, *storageClassName)
	if err != nil {
		logger.Info("failed to get ceph cluster ID from StorageClass name",
			"error", err, "pvcNamespace", pvc.Namespace, "pvc", pvc.Name, "storageClassName", *storageClassName)
		if errors.Is(err, errEmptyClusterID) {
			return "", nil
		}

		return "", err
	}

	return clusterID, nil
}
