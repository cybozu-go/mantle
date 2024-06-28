package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/test/util"

	. "github.com/onsi/ginkgo/v2"

	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
)

func mockExecuteCommand(command []string, input io.Reader) ([]byte, error) {
	return nil, nil
}

var _ = Describe("MantleBackup controller", func() {
	ctx := context.Background()
	var mgrUtil util.ManagerUtil
	var reconciler *MantleBackupReconciler

	storageClassClusterID := dummyStorageClassClusterID
	storageClassName := dummyStorageClassName

	BeforeEach(func() {
		mgrUtil = util.NewManagerUtil(ctx, cfg, scheme.Scheme)

		reconciler = NewMantleBackupReconciler(k8sClient, mgrUtil.GetScheme(), storageClassClusterID)
		err := reconciler.SetupWithManager(mgrUtil.GetManager())
		Expect(err).NotTo(HaveOccurred())

		executeCommand = mockExecuteCommand

		mgrUtil.Start()
		time.Sleep(100 * time.Millisecond)
	})

	AfterEach(func() {
		err := mgrUtil.Stop()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should be ready to use", func() {
		ns := createNamespace()

		csiPVSource := corev1.CSIPersistentVolumeSource{
			Driver:       "driver",
			VolumeHandle: "handle",
			VolumeAttributes: map[string]string{
				"imageName": "imageName",
				"pool":      "pool",
			},
		}
		pv := corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pv",
			},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{
					"storage": resource.MustParse("5Gi"),
				},
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &csiPVSource,
				},
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
			},
		}
		err := k8sClient.Create(ctx, &pv)
		Expect(err).NotTo(HaveOccurred())

		pvc := corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pvc",
				Namespace: ns,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				VolumeName: pv.Name,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
					},
				},
				StorageClassName: &storageClassName,
			},
		}
		err = k8sClient.Create(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		pvc.Status.Phase = corev1.ClaimBound
		err = k8sClient.Status().Update(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		backup := mantlev1.MantleBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backup",
				Namespace: ns,
			},
			Spec: mantlev1.MantleBackupSpec{
				PVC: pvc.Name,
			},
		}
		err = k8sClient.Create(ctx, &backup)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() error {
			namespacedName := types.NamespacedName{
				Namespace: ns,
				Name:      backup.Name,
			}
			err = k8sClient.Get(ctx, namespacedName, &backup)
			if err != nil {
				return err
			}

			existFinalizer := false
			for _, f := range backup.Finalizers {
				if f == MantleBackupFinalizerName {
					existFinalizer = true
					break
				}
			}
			if !existFinalizer {
				return fmt.Errorf("finalizer does not set yet")
			}

			return nil
		}).Should(Succeed())

		Eventually(func() error {
			namespacedName := types.NamespacedName{
				Namespace: ns,
				Name:      backup.Name,
			}
			err = k8sClient.Get(ctx, namespacedName, &backup)
			if err != nil {
				return err
			}

			if meta.FindStatusCondition(backup.Status.Conditions, mantlev1.BackupConditionReadyToUse).Status != metav1.ConditionTrue {
				return fmt.Errorf("not ready to use yet")
			}

			return nil
		}).Should(Succeed())

		pvcJS := backup.Status.PVCManifest
		Expect(pvcJS).NotTo(BeEmpty())
		pvcStored := corev1.PersistentVolumeClaim{}
		err = json.Unmarshal([]byte(pvcJS), &pvcStored)
		Expect(err).NotTo(HaveOccurred())
		Expect(pvcStored.Name).To(Equal(pvc.Name))
		Expect(pvcStored.Namespace).To(Equal(pvc.Namespace))

		pvJS := backup.Status.PVManifest
		Expect(pvJS).NotTo(BeEmpty())
		pvStored := corev1.PersistentVolume{}
		err = json.Unmarshal([]byte(pvJS), &pvStored)
		Expect(err).NotTo(HaveOccurred())
		Expect(pvStored.Name).To(Equal(pv.Name))

		err = k8sClient.Delete(ctx, &backup)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() error {
			namespacedName := types.NamespacedName{
				Namespace: ns,
				Name:      backup.Name,
			}
			err = k8sClient.Get(ctx, namespacedName, &backup)
			if errors.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			return fmt.Errorf("\"%s\" is not deleted yet", backup.Name)
		}).Should(Succeed())
	})

	It("should still be ready to use even if the PVC lost", func() {
		ctx := context.Background()
		ns := createNamespace()

		csiPVSource := corev1.CSIPersistentVolumeSource{
			Driver:       "driver",
			VolumeHandle: "handle",
			VolumeAttributes: map[string]string{
				"imageName": "imageName",
				"pool":      "pool",
			},
		}
		pv := corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pv2",
			},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{
					"storage": resource.MustParse("5Gi"),
				},
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &csiPVSource,
				},
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
			},
		}
		err := k8sClient.Create(ctx, &pv)
		Expect(err).NotTo(HaveOccurred())

		pvc := corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pvc",
				Namespace: ns,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				VolumeName: pv.Name,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
					},
				},
				StorageClassName: &storageClassName,
			},
		}
		err = k8sClient.Create(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		pvc.Status.Phase = corev1.ClaimBound
		err = k8sClient.Status().Update(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		backup := mantlev1.MantleBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backup",
				Namespace: ns,
			},
			Spec: mantlev1.MantleBackupSpec{
				PVC: pvc.Name,
			},
		}
		err = k8sClient.Create(ctx, &backup)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() error {
			namespacedName := types.NamespacedName{
				Namespace: ns,
				Name:      backup.Name,
			}
			err = k8sClient.Get(ctx, namespacedName, &backup)
			if err != nil {
				return err
			}

			existFinalizer := false
			for _, f := range backup.Finalizers {
				if f == MantleBackupFinalizerName {
					existFinalizer = true
					break
				}
			}
			if !existFinalizer {
				return fmt.Errorf("finalizer does not set yet")
			}

			return nil
		}).Should(Succeed())

		Eventually(func() error {
			namespacedName := types.NamespacedName{
				Namespace: ns,
				Name:      backup.Name,
			}
			err = k8sClient.Get(ctx, namespacedName, &backup)
			if err != nil {
				return err
			}

			if meta.FindStatusCondition(backup.Status.Conditions, mantlev1.BackupConditionReadyToUse).Status != metav1.ConditionTrue {
				return fmt.Errorf("not ready to use yet")
			}

			return nil
		}).Should(Succeed())

		pvc.Status.Phase = corev1.ClaimLost // simulate lost PVC
		err = k8sClient.Status().Update(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() error {
			namespacedName := types.NamespacedName{
				Namespace: ns,
				Name:      backup.Name,
			}
			err = k8sClient.Get(ctx, namespacedName, &backup)
			if err != nil {
				return err
			}

			condition := meta.FindStatusCondition(backup.Status.Conditions, mantlev1.BackupConditionReadyToUse)
			if condition == nil {
				return fmt.Errorf("condition is not set")
			}

			if condition.Status != metav1.ConditionTrue {
				return fmt.Errorf("should keep ready to use")
			}

			return nil
		}).Should(Succeed())
	})

	It("should not be ready to use if the PVC is the lost state from the beginning", func() {
		ctx := context.Background()
		ns := createNamespace()

		csiPVSource := corev1.CSIPersistentVolumeSource{
			Driver:       "driver",
			VolumeHandle: "handle",
			VolumeAttributes: map[string]string{
				"imageName": "imageName",
				"pool":      "pool",
			},
		}
		pv := corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pv3",
			},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{
					"storage": resource.MustParse("5Gi"),
				},
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &csiPVSource,
				},
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
			},
		}
		err := k8sClient.Create(ctx, &pv)
		Expect(err).NotTo(HaveOccurred())

		pvc := corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pvc",
				Namespace: ns,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				VolumeName: pv.Name,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
					},
				},
				StorageClassName: &storageClassName,
			},
		}
		err = k8sClient.Create(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		pvc.Status.Phase = corev1.ClaimLost
		err = k8sClient.Status().Update(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		backup := mantlev1.MantleBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backup",
				Namespace: ns,
			},
			Spec: mantlev1.MantleBackupSpec{
				PVC: pvc.Name,
			},
		}
		err = k8sClient.Create(ctx, &backup)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() error {
			namespacedName := types.NamespacedName{
				Namespace: ns,
				Name:      backup.Name,
			}
			err = k8sClient.Get(ctx, namespacedName, &backup)
			if err != nil {
				return err
			}

			condition := meta.FindStatusCondition(backup.Status.Conditions, mantlev1.BackupConditionReadyToUse)
			if condition == nil {
				return fmt.Errorf("condition is not set")
			}
			if !(condition.Status == metav1.ConditionFalse && condition.Reason != mantlev1.BackupReasonNone) {
				return fmt.Errorf("should not be ready to use")
			}

			return nil
		}).Should(Succeed())
	})

	It("should not be ready to use if specified non-existent PVC name", func() {
		ctx := context.Background()
		ns := createNamespace()

		backup := mantlev1.MantleBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backup",
				Namespace: ns,
			},
			Spec: mantlev1.MantleBackupSpec{
				PVC: "non-existent-pvc",
			},
		}
		err := k8sClient.Create(ctx, &backup)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() error {
			namespacedName := types.NamespacedName{
				Namespace: ns,
				Name:      backup.Name,
			}
			err = k8sClient.Get(ctx, namespacedName, &backup)
			if err != nil {
				return err
			}

			condition := meta.FindStatusCondition(backup.Status.Conditions, mantlev1.BackupConditionReadyToUse)
			if condition == nil {
				return fmt.Errorf("condition is not set")
			}
			if !(condition.Status == metav1.ConditionFalse && condition.Reason != mantlev1.BackupReasonNone) {
				return fmt.Errorf("should not be ready to use")
			}

			return nil
		}).Should(Succeed())
	})

	It("should fail the resource creation the second time if the same MantleBackup is created twice", func() {
		ctx := context.Background()
		ns := createNamespace()

		csiPVSource := corev1.CSIPersistentVolumeSource{
			Driver:       "driver",
			VolumeHandle: "handle",
			VolumeAttributes: map[string]string{
				"imageName": "imageName",
				"pool":      "pool",
			},
		}
		pv := corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pv4",
			},
			Spec: corev1.PersistentVolumeSpec{
				Capacity: corev1.ResourceList{
					"storage": resource.MustParse("5Gi"),
				},
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &csiPVSource,
				},
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
			},
		}
		err := k8sClient.Create(ctx, &pv)
		Expect(err).NotTo(HaveOccurred())

		pvc := corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pvc",
				Namespace: ns,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				VolumeName: pv.Name,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
					},
				},
				StorageClassName: &storageClassName,
			},
		}
		err = k8sClient.Create(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		pvc.Status.Phase = corev1.ClaimBound
		err = k8sClient.Status().Update(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		backup := mantlev1.MantleBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backup",
				Namespace: ns,
			},
			Spec: mantlev1.MantleBackupSpec{
				PVC: pvc.Name,
			},
		}
		err = k8sClient.Create(ctx, &backup)
		Expect(err).NotTo(HaveOccurred())

		err = k8sClient.Create(ctx, &backup)
		Expect(err).To(HaveOccurred())
	})
})
