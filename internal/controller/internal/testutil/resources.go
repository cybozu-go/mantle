package testutil

import (
	"context"

	"github.com/cybozu-go/mantle/test/util"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	provisioner = "rook-ceph.rbd.csi.ceph.com"
)

type ResourceManager struct {
	client           client.Client
	ClusterID        string
	StorageClassName string
	poolName         string
}

func NewResourceManager(client client.Client) *ResourceManager {
	return &ResourceManager{
		client:           client,
		StorageClassName: util.GetUniqueName("sc-"),
		ClusterID:        util.GetUniqueName("ceph-"),
		poolName:         util.GetUniqueName("pool-"),
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

// CreateUniquePVAndPVC creates a unique named PV and PVC. The PV is bound to the PVC.
func (r *ResourceManager) CreateUniquePVAndPVC(ctx context.Context, ns string) (
	*corev1.PersistentVolume, *corev1.PersistentVolumeClaim, error) {
	return r.createPVAndPVC(ctx, ns, util.GetUniqueName("pv-"), util.GetUniqueName("pvc-"))
}

func (r *ResourceManager) createPVAndPVC(ctx context.Context, ns, pvName, pvcName string) (
	*corev1.PersistentVolume, *corev1.PersistentVolumeClaim, error) {
	accessModes := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	size := resource.MustParse("1Gi")
	volumeMode := corev1.PersistentVolumeFilesystem

	pv := corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: accessModes,
			Capacity:    corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("5Gi")},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver: "restore.rbd.csi.ceph.com",
					VolumeAttributes: map[string]string{
						"clusterID":                        r.ClusterID,
						"csi.storage.k8s.io/pv/name":       pvName,
						"csi.storage.k8s.io/pvc/name":      pvcName,
						"csi.storage.k8s.io/pvc/namespace": ns,
						"imageFeatures":                    "layering",
						"imageFormat":                      "2",
						"imageName":                        util.GetUniqueName("image-"),
						"journalPool":                      r.poolName,
						"pool":                             r.poolName,
						"storage.kubernetes.io/csiProvisionerIdentity": "dummy",
					},
					VolumeHandle: "dummy",
				},
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              r.StorageClassName,
			VolumeMode:                    &volumeMode,
		},
	}
	err := r.client.Create(ctx, &pv)
	if err != nil {
		return nil, nil, err
	}

	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: ns,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      accessModes,
			StorageClassName: &r.StorageClassName,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: size,
				},
			},
			VolumeMode: &volumeMode,
			VolumeName: pvName,
		},
	}
	err = r.client.Create(ctx, &pvc)
	if err != nil {
		return nil, nil, err
	}
	pvc.Status.Phase = corev1.ClaimBound
	err = r.client.Status().Update(ctx, &pvc)
	if err != nil {
		return nil, nil, err
	}

	err = r.client.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: ns}, &pvc)
	if err != nil {
		return nil, nil, err
	}

	pv.Spec.ClaimRef = &corev1.ObjectReference{
		Name:      pvcName,
		Namespace: pvc.Namespace,
		UID:       pvc.UID,
	}
	err = r.client.Update(ctx, &pv)
	if err != nil {
		return nil, nil, err
	}

	pv.Status.Phase = corev1.VolumeBound
	err = r.client.Status().Update(ctx, &pv)
	if err != nil {
		return nil, nil, err
	}

	err = r.client.Get(ctx, types.NamespacedName{Name: pvName}, &pv)
	if err != nil {
		return nil, nil, err
	}

	return &pv, &pvc, err
}
