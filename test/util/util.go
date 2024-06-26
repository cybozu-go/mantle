package util

import (
	"context"
	"fmt"
	"math/rand"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	usedResourceNames = make(map[string]bool)
)

func GetUniqueName(prefix string) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	buf := make([]byte, 8)
	for i := range buf {
		buf[i] = letters[rand.Intn(len(letters))]
	}
	name := fmt.Sprintf("%s%s", prefix, string(buf))
	if usedResourceNames[name] {
		return GetUniqueName(prefix)
	}
	usedResourceNames[name] = true
	return name
}

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
