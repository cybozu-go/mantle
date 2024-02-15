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

	It("should set finalizer on RBDPVCBackup resource creation", func() {
		ctx := context.Background()
		ns := createNamespace()

		pvSource := corev1.CSIPersistentVolumeSource{
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
					CSI: &pvSource,
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
				VolumeName: pv.Name,
			},
			Status: corev1.PersistentVolumeClaimStatus{
				Phase: "Bound",
			},
		}
		err = k8sClient.Create(ctx, &pvc)
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

		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Namespace: ns,
				Name:      backup.Name,
			},
		}
		result, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		Expect(result).Should(Equal(ctrl.Result{}))

		Eventually(func() error {
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

			return nil
		}).Should(Succeed())
	})
})
