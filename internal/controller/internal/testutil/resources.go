package testutil

import (
	"context"
	"fmt"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
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

// EnvTest cannot delete namespace. So, we have to use another new namespace.
func (r *ResourceManager) CreateNamespace() string {
	name := util.GetUniqueName("test-")
	ns := corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
	err := r.client.Create(context.Background(), &ns)
	Expect(err).NotTo(HaveOccurred())
	return name
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

func (r *ResourceManager) CreateUniqueBackupFor(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (
	*mantlev1.MantleBackup, error) {
	backup := &mantlev1.MantleBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GetUniqueName("backup-"),
			Namespace: pvc.Namespace,
		},
		Spec: mantlev1.MantleBackupSpec{
			PVC: pvc.Name,
		},
	}
	err := r.client.Create(ctx, backup)
	if err != nil {
		return nil, err
	}

	return backup, nil
}

func (r *ResourceManager) WaitForBackupReady(ctx context.Context, backup *mantlev1.MantleBackup) {
	EventuallyWithOffset(1, func(g Gomega, ctx context.Context) {
		err := r.client.Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace}, backup)
		g.Expect(err).NotTo(HaveOccurred())

		g.Expect(meta.IsStatusConditionTrue(backup.Status.Conditions, mantlev1.BackupConditionReadyToUse)).Should(BeTrue())
	}).WithContext(ctx).Should(Succeed())
}

// cf. https://go.googlesource.com/proposal/+/refs/heads/master/design/43651-type-parameters.md#pointer-method-example
type ObjectConstraint[T any] interface {
	client.Object
	*T
}

// These functions cannot belong to ResourceManager because they use generics.

func CheckCreatedEventually[T any, OC ObjectConstraint[T]](ctx context.Context, client client.Client, name, namespace string) {
	var obj T
	EventuallyWithOffset(1, func(g Gomega, ctx context.Context) {
		err := client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, OC(&obj))
		g.Expect(err).NotTo(HaveOccurred())
	}).WithContext(ctx).Should(Succeed())
}

func CheckDeletedEventually[T any, OC ObjectConstraint[T]](ctx context.Context, client client.Client, name, namespace string) {
	var obj T
	EventuallyWithOffset(1, func(g Gomega, ctx context.Context) {
		err := client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, OC(&obj))
		g.Expect(err).To(Satisfy(errors.IsNotFound), fmt.Sprintf(`"%s" is not deleted yet`, name))
	}).WithContext(ctx).Should(Succeed())
}