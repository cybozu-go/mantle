package controller

import (
	"context"
	"encoding/json"
	"sync"
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
	"k8s.io/kube-openapi/pkg/validation/strfmt"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

var _ = Describe("MantleBackup controller", func() {
	var mgrUtil testutil.ManagerUtil
	var reconciler *MantleBackupReconciler
	var ns string
	var lastExpireQueuedBackups sync.Map

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

	simulateExpire := func(ctx context.Context, backup *mantlev1.MantleBackup, offset time.Duration) time.Time {
		err := k8sClient.Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace}, backup)
		Expect(err).NotTo(HaveOccurred())

		// set the creation time to expire the backup after the offset from now
		expire, err := strfmt.ParseDuration(backup.Spec.Expire)
		Expect(err).NotTo(HaveOccurred())
		// trim nsec because kubernetes seems not to save nsec.
		now := time.Unix(time.Now().Unix(), 0).UTC()
		newCreatedAt := now.Add(-expire).Add(offset)
		backup.Status.CreatedAt = metav1.NewTime(newCreatedAt)
		err = k8sClient.Status().Update(ctx, backup)
		Expect(err).NotTo(HaveOccurred())

		return newCreatedAt
	}

	// sniffing the expire queue to check the expiration is deferred.
	setupExpireQueueSniffer := func() {
		origCh := reconciler.expireQueueCh
		newCh := make(chan event.GenericEvent)
		reconciler.expireQueueCh = newCh
		go func() {
			for event := range newCh {
				lastExpireQueuedBackups.Store(
					types.NamespacedName{
						Namespace: event.Object.GetNamespace(),
						Name:      event.Object.GetName(),
					},
					event.Object.DeepCopyObject())
				origCh <- event
			}
		}()
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
		reconciler.ceph = testutil.NewFakeRBD()
		err := reconciler.SetupWithManager(mgrUtil.GetManager())
		Expect(err).NotTo(HaveOccurred())

		setupExpireQueueSniffer()

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

		backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
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

		snaps, err := reconciler.ceph.RBDSnapLs(resMgr.PoolName, pv.Spec.CSI.VolumeAttributes["imageName"])
		Expect(err).NotTo(HaveOccurred())
		Expect(snaps).To(HaveLen(1))
		snapID := backup.Status.SnapID
		Expect(snapID).To(Equal(&snaps[0].Id))

		err = k8sClient.Delete(ctx, backup)
		Expect(err).NotTo(HaveOccurred())

		testutil.CheckDeletedEventually[mantlev1.MantleBackup](ctx, k8sClient, backup.Name, backup.Namespace)
	})

	It("should still be ready to use even if the PVC lost", func(ctx SpecContext) {
		_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())

		backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())

		waitForHavingFinalizer(ctx, backup)
		resMgr.WaitForBackupReady(ctx, backup)

		pvc.Status.Phase = corev1.ClaimLost // simulate lost PVC
		err = k8sClient.Status().Update(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())

		resMgr.WaitForBackupReady(ctx, backup)
	})

	DescribeTable("MantleBackup with correct expiration",
		func(ctx SpecContext, expire string) {
			_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())

			_, err = resMgr.CreateUniqueBackupFor(ctx, pvc, func(backup *mantlev1.MantleBackup) {
				backup.Spec.Expire = expire
			})
			Expect(err).NotTo(HaveOccurred())
		},
		Entry("min expire", "1d"),
		Entry("max expire", "15d"),
		Entry("complex expire", "1w2d3h4m5s"),
	)

	DescribeTable("MantleBackup with incorrect expiration",
		func(ctx SpecContext, expire string) {
			_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())

			_, err = resMgr.CreateUniqueBackupFor(ctx, pvc, func(backup *mantlev1.MantleBackup) {
				backup.Spec.Expire = expire
			})
			Expect(err).To(Or(
				MatchError(ContainSubstring("expire must be")),
				MatchError(ContainSubstring("body must be of type duration")),
			))
		},
		Entry("invalid short expire", "23h"),
		Entry("invalid long expire", "15d1s"),
		Entry("invalid duration", "foo"),
	)

	It("Should reject updating the expire field", func(ctx SpecContext) {
		_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())

		backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())

		expire, err := strfmt.ParseDuration(backup.Spec.Expire)
		Expect(err).NotTo(HaveOccurred())
		expire += time.Hour
		backup.Spec.Expire = expire.String()
		err = k8sClient.Update(ctx, backup)
		Expect(err).To(MatchError(ContainSubstring("spec.expire is immutable")))
	})

	DescribeTable("MantleBackup expiration",
		func(ctx SpecContext, offset time.Duration) {
			_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())

			backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for the backup to be ready")
			resMgr.WaitForBackupReady(ctx, backup)

			expectCreatedAt := backup.Status.CreatedAt.Time

			By("simulate backup expiration")
			newCreatedAt := simulateExpire(ctx, backup, offset)

			By("wait for the backup to be deleted")
			testutil.CheckDeletedEventually[mantlev1.MantleBackup](ctx, k8sClient, backup.Name, backup.Namespace)

			By("check the queued backup has the correct createdAt")
			// If expiration is deferred, the backup with the new createdAt is queued.
			// Otherwise, the backup is not queued after updating the createdAt, so the backup has the original createdAt.
			if offset > 0 {
				expectCreatedAt = newCreatedAt
			}
			v, ok := lastExpireQueuedBackups.Load(types.NamespacedName{Namespace: backup.Namespace, Name: backup.Name})
			Expect(ok).To(BeTrue())
			createdAt := v.(*mantlev1.MantleBackup).Status.CreatedAt.Time
			Expect(createdAt).To(BeTemporally("==", expectCreatedAt))
		},
		Entry("an already expired backup should be deleted immediately", -time.Hour),
		Entry("a near expiring backup should be deleted after expiration", 10*time.Second),
	)

	It("should not be ready to use if the PVC is the lost state from the beginning", func(ctx SpecContext) {
		pv, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())
		pv.Status.Phase = corev1.VolumeAvailable
		err = k8sClient.Status().Update(ctx, pv)
		Expect(err).NotTo(HaveOccurred())
		pvc.Status.Phase = corev1.ClaimLost
		err = k8sClient.Status().Update(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())

		backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())

		waitForBackupNotReady(ctx, backup)
	})

	It("should not be ready to use if specified non-existent PVC name", func(ctx SpecContext) {
		var err error
		backup, err := resMgr.CreateUniqueBackupFor(ctx, &corev1.PersistentVolumeClaim{
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

		backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())

		err = k8sClient.Create(ctx, backup)
		Expect(err).To(HaveOccurred())
	})
})
