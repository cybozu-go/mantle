package controller

import (
	"context"
	"errors"
	"io"
	"time"

	backupv1 "github.com/cybozu-go/rbd-backup-system/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
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

	It("should set finalizer and \"Creating\" status on RBDPVCBackup resource creation", func() {
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
				return errors.New("finalizer does not set yet")
			}

			if backup.Status.Conditions != backupv1.RBDPVCBackupConditionsCreating {
				return errors.New("status.conditions does not set \"Creating\" yet")
			}

			return nil
		}).Should(Succeed())
	})
})
