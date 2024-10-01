package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var _ = Describe("MantleBackup controller", func() {
	var mgrUtil testutil.ManagerUtil
	var reconciler *MantleBackupReconciler
	var ns string

	// Not to access backup.Name before created, set the pointer to an empty object.
	backup := &mantlev1.MantleBackup{}

	waitForBackupNotReady := func(ctx context.Context, backup *mantlev1.MantleBackup) {
		EventuallyWithOffset(1, func(g Gomega, ctx context.Context) {
			namespacedName := types.NamespacedName{
				Namespace: backup.Namespace,
				Name:      backup.Name,
			}
			err := k8sClient.Get(ctx, namespacedName, backup)
			g.Expect(err).NotTo(HaveOccurred())
			condition := meta.FindStatusCondition(backup.Status.Conditions, mantlev1.BackupConditionReadyToUse)
			g.Expect(condition).NotTo(BeNil())
			g.Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(condition.Reason).NotTo(Equal(mantlev1.BackupReasonNone))
		}).WithContext(ctx).Should(Succeed())
	}

	waitForHavingFinalizer := func(ctx context.Context, backup *mantlev1.MantleBackup) {
		EventuallyWithOffset(1, func(g Gomega, ctx context.Context) {
			namespacedName := types.NamespacedName{
				Namespace: backup.Namespace,
				Name:      backup.Name,
			}
			err := k8sClient.Get(ctx, namespacedName, backup)
			g.Expect(err).NotTo(HaveOccurred())

			g.Expect(controllerutil.ContainsFinalizer(backup, MantleBackupFinalizerName)).To(BeTrue(), "finalizer does not set yet")
		}).WithContext(ctx).Should(Succeed())
	}

	BeforeEach(func() {
		mgrUtil = testutil.NewManagerUtil(context.Background(), cfg, scheme.Scheme)

		reconciler = NewMantleBackupReconciler(
			mgrUtil.GetManager().GetClient(),
			mgrUtil.GetManager().GetScheme(),
			resMgr.ClusterID,
			RoleStandalone,
			nil,
		)
		err := reconciler.SetupWithManager(mgrUtil.GetManager())
		Expect(err).NotTo(HaveOccurred())

		executeCommand = func(_ context.Context, command []string, _ io.Reader) ([]byte, error) {
			if command[0] == "rbd" && command[1] == "snap" && command[2] == "ls" {
				return []byte(fmt.Sprintf("[{\"id\":1000,\"name\":\"%s\","+
					"\"timestamp\":\"Mon Sep  2 00:42:00 2024\"}]", backup.Name)), nil
			}
			return nil, nil
		}

		mgrUtil.Start()
		time.Sleep(100 * time.Millisecond)

		ns = resMgr.CreateNamespace()
	})

	AfterEach(func() {
		err := mgrUtil.Stop()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should be ready to use", func(ctx SpecContext) {
		pv, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())

		backup, err = resMgr.CreateUniqueBackupFor(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())

		waitForHavingFinalizer(ctx, backup)
		resMgr.WaitForBackupReady(ctx, backup)

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

		snapID := backup.Status.SnapID
		Expect(*snapID).To(Equal(1000))

		err = k8sClient.Delete(ctx, backup)
		Expect(err).NotTo(HaveOccurred())

		testutil.CheckDeletedEventually[mantlev1.MantleBackup](ctx, k8sClient, backup.Name, backup.Namespace)
	})

	It("should still be ready to use even if the PVC lost", func(ctx SpecContext) {
		_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())

		backup, err = resMgr.CreateUniqueBackupFor(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())

		waitForHavingFinalizer(ctx, backup)
		resMgr.WaitForBackupReady(ctx, backup)

		pvc.Status.Phase = corev1.ClaimLost // simulate lost PVC
		err = k8sClient.Status().Update(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())

		resMgr.WaitForBackupReady(ctx, backup)
	})

	It("should not be ready to use if the PVC is the lost state from the beginning", func(ctx SpecContext) {
		pv, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())
		pv.Status.Phase = corev1.VolumeAvailable
		err = k8sClient.Status().Update(ctx, pv)
		Expect(err).NotTo(HaveOccurred())
		pvc.Status.Phase = corev1.ClaimLost
		err = k8sClient.Status().Update(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())

		backup, err = resMgr.CreateUniqueBackupFor(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())

		waitForBackupNotReady(ctx, backup)
	})

	It("should not be ready to use if specified non-existent PVC name", func(ctx SpecContext) {
		var err error
		backup, err = resMgr.CreateUniqueBackupFor(ctx, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "non-existent-pvc",
				Namespace: ns,
			},
		})
		Expect(err).NotTo(HaveOccurred())

		waitForBackupNotReady(ctx, backup)
	})

	It("should fail the resource creation the second time if the same MantleBackup is created twice", func(ctx SpecContext) {
		_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())

		backup, err = resMgr.CreateUniqueBackupFor(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())

		err = k8sClient.Create(ctx, backup)
		Expect(err).To(HaveOccurred())
	})
})
