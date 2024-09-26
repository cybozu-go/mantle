package testutil

import (
	"context"

	"github.com/cybozu-go/mantle/test/util"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	provisioner = "rook-ceph.rbd.csi.ceph.com"
)

type ResourceManager struct {
	client           client.Client
	ClusterID        string
	StorageClassName string
}

func NewResourceManager(client client.Client) *ResourceManager {
	return &ResourceManager{
		client:           client,
		StorageClassName: util.GetUniqueName("sc-"),
		ClusterID:        util.GetUniqueName("ceph-"),
	}
}

func (r *ResourceManager) CreateStorageClass(ctx context.Context) error {
	sc := storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: r.StorageClassName,
		},
		Provisioner: provisioner,
		Parameters: map[string]string{
			"clusterID": r.ClusterID,
		},
	}
	return r.client.Create(ctx, &sc)
}
