package controller

import (
	"context"
	"fmt"
	"io"
	"time"

	backupv1 "github.com/cybozu-go/rbd-backup-system/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

func mockExecuteCommand(command []string, input io.Reader) ([]byte, error) {
	return nil, nil
}

var _ = Describe("RBDPVCBackup controller", func() {
	ctx := context.Background()
	var stopFunc func()
	errCh := make(chan error)

	var reconciler *RBDPVCBackupReconciler
	scheme := runtime.NewScheme()

	BeforeEach(func() {
		err := backupv1.AddToScheme(scheme)
		Expect(err).NotTo(HaveOccurred())

		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme: scheme,
		})
		Expect(err).ToNot(HaveOccurred())

		reconciler = NewRBDPVCBackupReconciler(k8sClient, mgr.GetScheme())
		err = reconciler.SetupWithManager(mgr)
		Expect(err).NotTo(HaveOccurred())

		executeCommand = mockExecuteCommand

		ctx, cancel := context.WithCancel(ctx)
		stopFunc = cancel
		go func() {
			errCh <- mgr.Start(ctx)
		}()
		time.Sleep(100 * time.Millisecond)
	})

	AfterEach(func() {
		stopFunc()
		Expect(<-errCh).NotTo(HaveOccurred())
	})

	It("should be ready to use", func() {
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
			},
		}
		err = k8sClient.Create(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		pvc.Status.Phase = corev1.ClaimBound
		err = k8sClient.Status().Update(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		backup := backupv1.RBDPVCBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backup",
				Namespace: ns,
			},
			Spec: backupv1.RBDPVCBackupSpec{
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
				if f == RBDPVCBackupFinalizerName {
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

			if meta.FindStatusCondition(backup.Status.Conditions, backupv1.ConditionReadyToUse).Status != metav1.ConditionTrue {
				return fmt.Errorf("not ready to use yet")
			}

			return nil
		}).Should(Succeed())

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
			},
		}
		err = k8sClient.Create(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		pvc.Status.Phase = corev1.ClaimBound
		err = k8sClient.Status().Update(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		backup := backupv1.RBDPVCBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backup",
				Namespace: ns,
			},
			Spec: backupv1.RBDPVCBackupSpec{
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
				if f == RBDPVCBackupFinalizerName {
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

			if meta.FindStatusCondition(backup.Status.Conditions, backupv1.ConditionReadyToUse).Status != metav1.ConditionTrue {
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

			condition := meta.FindStatusCondition(backup.Status.Conditions, backupv1.ConditionReadyToUse)
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
			},
		}
		err = k8sClient.Create(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		pvc.Status.Phase = corev1.ClaimLost
		err = k8sClient.Status().Update(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		backup := backupv1.RBDPVCBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backup",
				Namespace: ns,
			},
			Spec: backupv1.RBDPVCBackupSpec{
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

			condition := meta.FindStatusCondition(backup.Status.Conditions, backupv1.ConditionReadyToUse)
			if condition == nil {
				return fmt.Errorf("condition is not set")
			}
			if !(condition.Status == metav1.ConditionFalse && condition.Reason != backupv1.ReasonNone) {
				return fmt.Errorf("should not be ready to use")
			}

			return nil
		}).Should(Succeed())
	})

	It("should not be ready to use if specified non-existent PVC name", func() {
		ctx := context.Background()
		ns := createNamespace()

		backup := backupv1.RBDPVCBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backup",
				Namespace: ns,
			},
			Spec: backupv1.RBDPVCBackupSpec{
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

			condition := meta.FindStatusCondition(backup.Status.Conditions, backupv1.ConditionReadyToUse)
			if condition == nil {
				return fmt.Errorf("condition is not set")
			}
			if !(condition.Status == metav1.ConditionFalse && condition.Reason != backupv1.ReasonNone) {
				return fmt.Errorf("should not be ready to use")
			}

			return nil
		}).Should(Succeed())
	})

	It("should fail the resource creation the second time if the same RBDPVCBackup is created twice", func() {
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
			},
		}
		err = k8sClient.Create(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		pvc.Status.Phase = corev1.ClaimBound
		err = k8sClient.Status().Update(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		backup := backupv1.RBDPVCBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backup",
				Namespace: ns,
			},
			Spec: backupv1.RBDPVCBackupSpec{
				PVC: pvc.Name,
			},
		}
		err = k8sClient.Create(ctx, &backup)
		Expect(err).NotTo(HaveOccurred())

		err = k8sClient.Create(ctx, &backup)
		Expect(err).To(HaveOccurred())
	})
})
