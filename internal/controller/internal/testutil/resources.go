package testutil

import (
	"context"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func CreateStorageClass(ctx context.Context, client client.Client, name, provisioner, cephClusterID string) error {
	sc := storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Provisioner: provisioner,
		Parameters: map[string]string{
			"clusterID": cephClusterID,
		},
	}
	return client.Create(ctx, &sc)
}
