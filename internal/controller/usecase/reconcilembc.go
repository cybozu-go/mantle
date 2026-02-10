package usecase

import (
	"context"
	"fmt"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func getResource[
	T any,
	PT interface {
		*T
		client.Object
	},
](ctx context.Context, k8sClient client.Client, name string, namespace string) (PT, error) {
	obj := PT(new(T))
	if err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, obj); err != nil {
		return nil, fmt.Errorf("failed to get %T %s/%s: %w", obj, namespace, name, err)
	}
	return obj, nil
}

func ReconcileMBCInPrimary(
	ctx context.Context,
	reconciler *domain.MBCPrimaryReconciler,
	k8sClient client.Client,
	mbcName string,
	mbcNamespace string,
) error {
	mbc, err := getResource[mantlev1.MantleBackupConfig](ctx, k8sClient, mbcName, mbcNamespace)
	if err != nil {
		return err
	}
	pvc, err := getResource[corev1.PersistentVolumeClaim](ctx, k8sClient, mbc.Spec.PVC, mbcNamespace)
	if err != nil {
		return err
	}
	var sc *storagev1.StorageClass
	if pvc.Spec.StorageClassName != nil {
		sc, err = getResource[storagev1.StorageClass](ctx, k8sClient, *pvc.Spec.StorageClassName, "")
		if err != nil {
			return err
		}
	}

	return nil
}
