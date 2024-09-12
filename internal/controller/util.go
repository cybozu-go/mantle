package controller

import (
	"context"
	"log/slog"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func getCephClusterIDFromPVC(ctx context.Context, logger *slog.Logger, k8sClient client.Client, pvc *corev1.PersistentVolumeClaim) (string, error) {
	storageClassName := pvc.Spec.StorageClassName
	if storageClassName == nil {
		logger.Info("not managed storage class", "namespace", pvc.Namespace, "pvc", pvc.Name)
		return "", nil
	}
	var storageClass storagev1.StorageClass
	err := k8sClient.Get(ctx, types.NamespacedName{Name: *storageClassName}, &storageClass)
	if err != nil {
		return "", err
	}

	// Check if the MantleBackup resource being reconciled is managed by the CephCluster we are in charge of.
	if !strings.HasSuffix(storageClass.Provisioner, ".rbd.csi.ceph.com") {
		logger.Info("SC is not managed by RBD", "namespace", pvc.Namespace, "pvc", pvc.Name, "storageClassName", *storageClassName)
		return "", nil
	}
	clusterID, ok := storageClass.Parameters["clusterID"]
	if !ok {
		logger.Info("clusterID not found", "namespace", pvc.Namespace, "pvc", pvc.Name, "storageClassName", *storageClassName)
		return "", nil
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
