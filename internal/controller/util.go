package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var errEmptyClusterID error = errors.New("cluster ID is empty")

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
