package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/testutil"
	"github.com/cybozu-go/mantle/pkg/controller/proto"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/kube-openapi/pkg/validation/strfmt"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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

	AfterEach(func() {
		err := mgrUtil.Stop()
		Expect(err).NotTo(HaveOccurred())
	})

	Context("when the role is `standalone`", func() {
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

		It("should retain the backup if it has the retain-if-expired annotation", func(ctx SpecContext) {
			_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())

			backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for the backup to be ready")
			resMgr.WaitForBackupReady(ctx, backup)

			By("simulate backup expiration")
			simulateExpire(ctx, backup, -time.Hour)

			By("checking the backup is not deleted")
			err = k8sClient.Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace}, backup)
			Expect(err).NotTo(HaveOccurred())
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

	Context("when the role is `primary`", func() {
		var mockCtrl *gomock.Controller
		var grpcClient *proto.MockMantleServiceClient
		BeforeEach(func() {
			mgrUtil = testutil.NewManagerUtil(context.Background(), cfg, scheme.Scheme)

			var t reporter
			mockCtrl = gomock.NewController(t)
			grpcClient = proto.NewMockMantleServiceClient(mockCtrl)
			reconciler = NewMantleBackupReconciler(
				mgrUtil.GetManager().GetClient(),
				mgrUtil.GetManager().GetScheme(),
				resMgr.ClusterID,
				RolePrimary,
				&PrimarySettings{
					Client: grpcClient,
				},
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
			if mockCtrl != nil {
				mockCtrl.Finish()
			}
		})

		It("should be synced to remote", func(ctx SpecContext) {
			grpcClient.EXPECT().CreateOrUpdatePVC(gomock.Any(), gomock.Any()).
				MinTimes(1).Return(
				&proto.CreateOrUpdatePVCResponse{
					Uid: "a7c9d5e2-4b8f-4e2a-9d3f-1b6a7c8e9f2b",
				}, nil)
			targetBackups := []*mantlev1.MantleBackup{}
			grpcClient.EXPECT().CreateOrUpdateMantleBackup(gomock.Any(), gomock.Any()).
				MinTimes(1).
				DoAndReturn(
					func(
						ctx context.Context,
						req *proto.CreateOrUpdateMantleBackupRequest,
						opts ...grpc.CallOption,
					) (*proto.CreateOrUpdateMantleBackupResponse, error) {
						var targetBackup mantlev1.MantleBackup
						err := json.Unmarshal(req.GetMantleBackup(), &targetBackup)
						if err != nil {
							panic(err)
						}
						targetBackups = append(targetBackups, &targetBackup)
						return &proto.CreateOrUpdateMantleBackupResponse{}, nil
					})
			grpcClient.EXPECT().ListMantleBackup(gomock.Any(), gomock.Any()).
				MinTimes(1).
				DoAndReturn(
					func(
						ctx context.Context,
						req *proto.ListMantleBackupRequest,
						opts ...grpc.CallOption,
					) (*proto.ListMantleBackupResponse, error) {
						data, err := json.Marshal(targetBackups)
						if err != nil {
							panic(err)
						}
						return &proto.ListMantleBackupResponse{
							MantleBackupList: data,
						}, nil
					})
			grpcClient.EXPECT().SetSynchronizing(gomock.Any(), gomock.Any()).
				MinTimes(1).
				DoAndReturn(
					func(
						ctx context.Context,
						req *proto.SetSynchronizingRequest,
						opts ...grpc.CallOption,
					) (*proto.SetSynchronizingResponse, error) {
						return &proto.SetSynchronizingResponse{}, nil
					})

			pv, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())

			backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())

			waitForHavingFinalizer(ctx, backup)
			resMgr.WaitForBackupReady(ctx, backup)
			resMgr.WaitForBackupSyncedToRemote(ctx, backup)

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

			// Make sure export() correctly annotates the MantleBackup resource.
			syncMode, ok := backup.GetAnnotations()[annotSyncMode]
			Expect(ok).To(BeTrue())
			Expect(syncMode).To(Equal(syncModeFull))

			// Make the all existing MantleBackups in the (mocked) secondary Mantle
			// ReadyToUse=True.
			for _, backup := range targetBackups {
				meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
					Type:   mantlev1.BackupConditionReadyToUse,
					Status: metav1.ConditionTrue,
					Reason: mantlev1.BackupReasonNone,
				})
			}

			// Create another MantleBackup (backup2) to make sure it should become a incremental backup.
			backup2, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())
			waitForHavingFinalizer(ctx, backup2)
			resMgr.WaitForBackupReady(ctx, backup2)
			resMgr.WaitForBackupSyncedToRemote(ctx, backup2)
			syncMode2, ok := backup2.GetAnnotations()[annotSyncMode]
			Expect(ok).To(BeTrue())
			Expect(syncMode2).To(Equal(syncModeIncremental))
			err = k8sClient.Get(ctx, types.NamespacedName{Name: backup.GetName(), Namespace: backup.GetNamespace()}, backup)
			Expect(err).NotTo(HaveOccurred())
			diffTo, ok := backup.GetAnnotations()[annotDiffTo]
			Expect(ok).To(BeTrue())
			Expect(diffTo).To(Equal(backup2.GetName()))

			// remove diffTo annotation of backup here to allow it to be deleted.
			// FIXME: this process is for testing purposes only and should be removed in the near future.
			_, err = ctrl.CreateOrUpdate(ctx, k8sClient, backup, func() error {
				delete(backup.Annotations, annotDiffTo)
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.Delete(ctx, backup)
			Expect(err).NotTo(HaveOccurred())
			err = k8sClient.Delete(ctx, backup2)
			Expect(err).NotTo(HaveOccurred())

			testutil.CheckDeletedEventually[mantlev1.MantleBackup](ctx, k8sClient, backup.Name, backup.Namespace)
		})
	})
})

var _ = Describe("searchDiffOriginMantleBackup", func() {
	testMantleBackup := newMantleBackup("test0", "test-ns", nil, nil, false,
		5, metav1.ConditionTrue, metav1.ConditionFalse)

	basePrimaryBackups := []mantlev1.MantleBackup{
		*newMantleBackup("test1", "test-ns", nil, nil, false,
			1, metav1.ConditionTrue, metav1.ConditionTrue),
		*newMantleBackup("test2", "test-ns", nil, nil, false,
			6, metav1.ConditionTrue, metav1.ConditionFalse),
		*newMantleBackup("test3", "test-ns", nil, nil, false,
			3, metav1.ConditionTrue, metav1.ConditionTrue),
		*newMantleBackup("test4", "test-ns", nil, nil, false,
			4, metav1.ConditionTrue, metav1.ConditionTrue),
	}
	// Note that slices.Clone() does the shallow copy.
	// ref. https://pkg.go.dev/slices#Clone
	primaryBackupsWithConditionFalse := slices.Clone(basePrimaryBackups)
	primaryBackupsWithConditionFalse[2] = *basePrimaryBackups[2].DeepCopy()
	meta.SetStatusCondition(&primaryBackupsWithConditionFalse[2].Status.Conditions,
		metav1.Condition{
			Type:   mantlev1.BackupConditionReadyToUse,
			Status: metav1.ConditionFalse,
		})
	primaryBackupsWithDeletionTimestamp := slices.Clone(basePrimaryBackups)
	primaryBackupsWithDeletionTimestamp[2] = *basePrimaryBackups[2].DeepCopy()
	now := metav1.Now()
	primaryBackupsWithDeletionTimestamp[2].SetDeletionTimestamp(&now)

	testSecondaryMantleBackups := map[string]*mantlev1.MantleBackup{
		"test1": basePrimaryBackups[0].DeepCopy(),
		// "test2" cannot exist on the secondary cluster
		// because it has a higher snapID than "test0".
		"test3": basePrimaryBackups[2].DeepCopy(),
		// "test4" is intentionally omitted.
	}

	DescribeTable("Search for the MantleBackup which is used for the diff origin",
		func(backup *mantlev1.MantleBackup,
			primaryBackups []mantlev1.MantleBackup,
			secondaryBackupMap map[string]*mantlev1.MantleBackup,
			shouldFindBackup bool,
			expectedBackupName string) {
			foundBackup := searchForDiffOriginMantleBackup(backup, primaryBackups, secondaryBackupMap)
			if shouldFindBackup {
				Expect(foundBackup).NotTo(BeNil())
				Expect(foundBackup.GetName()).To(Equal(expectedBackupName))
			} else {
				Expect(foundBackup).To(BeNil())
			}
		},
		Entry("should return nil when no MantleBackup found on the secondary cluster",
			testMantleBackup, basePrimaryBackups, make(map[string]*mantlev1.MantleBackup),
			false, ""),
		Entry("should find the correct MantleBackup",
			testMantleBackup, basePrimaryBackups, testSecondaryMantleBackups,
			true, "test3"),
		Entry("should skip the not-ready MantleBackup",
			testMantleBackup, primaryBackupsWithConditionFalse, testSecondaryMantleBackups,
			true, "test1"),
		Entry("should skip the MantleBackup with the deletion timestamp",
			testMantleBackup, primaryBackupsWithDeletionTimestamp, testSecondaryMantleBackups,
			true, "test1"),
	)
})

type reporter struct{}

func (g reporter) Errorf(format string, args ...any) {
	Fail(fmt.Sprintf(format, args...))
}

func (g reporter) Fatalf(format string, args ...any) {
	Fail(fmt.Sprintf(format, args...))
}

func newMantleBackup(
	name string,
	namespace string,
	annotations map[string]string,
	labels map[string]string,
	withDelTimestamp bool,
	snapID int,
	readyToUse metav1.ConditionStatus,
	syncedToRemote metav1.ConditionStatus,
) *mantlev1.MantleBackup {
	newMB := &mantlev1.MantleBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: annotations,
			Labels:      labels,
		},
		Status: mantlev1.MantleBackupStatus{
			SnapID: &snapID,
		},
	}
	if withDelTimestamp {
		now := metav1.Now()
		newMB.SetDeletionTimestamp(&now)
	}
	meta.SetStatusCondition(&newMB.Status.Conditions, metav1.Condition{
		Type:   mantlev1.BackupConditionReadyToUse,
		Status: readyToUse,
	})
	meta.SetStatusCondition(&newMB.Status.Conditions, metav1.Condition{
		Type:   mantlev1.BackupConditionSyncedToRemote,
		Status: syncedToRemote,
	})
	return newMB
}

var _ = Describe("prepareForDataSynchronization", func() {
	testPVCUID := "d3b07384-d9a7-4e6b-8a3b-1f4b7b7b7b7b"
	testName := "testSnap5"
	testNamespace := "test-ns"
	primaryLabels := map[string]string{
		labelLocalBackupTargetPVCUID: testPVCUID,
	}
	secondaryLabels := map[string]string{
		labelRemoteBackupTargetPVCUID: testPVCUID,
	}
	testMantleBackup := newMantleBackup(testName, testNamespace, nil, primaryLabels,
		false, 5, metav1.ConditionTrue, metav1.ConditionFalse)

	doTest := func(
		backup *mantlev1.MantleBackup,
		primaryBackupsWithoutTarget []*mantlev1.MantleBackup,
		secondaryMantleBackups []*mantlev1.MantleBackup,
		isIncremental bool,
		isSecondaryMantleBackupReadyToUse bool,
		diffFrom *mantlev1.MantleBackup,
	) {
		var t reporter
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		grpcClient := proto.NewMockMantleServiceClient(mockCtrl)

		data, err := json.Marshal(secondaryMantleBackups)
		Expect(err).NotTo(HaveOccurred())
		grpcClient.EXPECT().ListMantleBackup(gomock.Any(),
			&proto.ListMantleBackupRequest{
				PvcUID:    testPVCUID,
				Namespace: testNamespace,
			}).Times(1).Return(
			&proto.ListMantleBackupResponse{
				MantleBackupList: data,
			}, nil)

		ctrlClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
		Expect(ctrlClient).NotTo(BeNil())
		primaryBackups := append(primaryBackupsWithoutTarget, backup.DeepCopy())
		for _, backup := range primaryBackups {
			shouldBeDeleted := false
			if !backup.DeletionTimestamp.IsZero() {
				shouldBeDeleted = true
			}

			err := ctrlClient.Create(context.Background(), backup)
			Expect(err).NotTo(HaveOccurred())
			if shouldBeDeleted {
				// Add a finalizer to prevent the immediate deletion.
				_ = controllerutil.AddFinalizer(backup, MantleBackupFinalizerName)
				err = ctrlClient.Update(context.Background(), backup)
				Expect(err).NotTo(HaveOccurred())
				err := ctrlClient.Delete(context.Background(), backup)
				Expect(err).NotTo(HaveOccurred())
				// Check if the deletion is blocked by the finalizer.
				err = ctrlClient.Get(context.Background(), types.NamespacedName{
					Namespace: backup.Namespace,
					Name:      backup.Name,
				}, backup)
				Expect(err).NotTo(HaveOccurred())
				Expect(controllerutil.ContainsFinalizer(backup, MantleBackupFinalizerName)).To(BeTrue())
			}
		}

		mbr := NewMantleBackupReconciler(ctrlClient,
			ctrlClient.Scheme(), "test", RolePrimary, nil)

		ret, err := mbr.prepareForDataSynchronization(context.Background(),
			backup, grpcClient)
		Expect(err).NotTo(HaveOccurred())
		Expect(ret.isIncremental).To(Equal(isIncremental))
		Expect(ret.isSecondaryMantleBackupReadyToUse).To(Equal(isSecondaryMantleBackupReadyToUse))
		if isIncremental {
			Expect(ret.diffFrom).NotTo(BeNil())
			Expect(ret.diffFrom.GetName()).To(Equal(diffFrom.GetName()))
		} else {
			Expect(ret.diffFrom).To(BeNil())
		}
	}

	DescribeTable("MantleBackup without annotations", doTest,
		Entry("No synced MantleBackup exists",
			testMantleBackup,
			[]*mantlev1.MantleBackup{},
			[]*mantlev1.MantleBackup{
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, false, false, nil),
		Entry("Synced but not reflected to the condition",
			testMantleBackup,
			[]*mantlev1.MantleBackup{},
			[]*mantlev1.MantleBackup{
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionTrue, metav1.ConditionUnknown),
			}, false, true, nil),
		Entry("Synced MantleBackup exists but deletionTimestamp is set on both clusters",
			testMantleBackup,
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, primaryLabels, true,
					3, metav1.ConditionTrue, metav1.ConditionTrue),
			},
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, secondaryLabels, true,
					3, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, false, false, nil),
		Entry("Synced MantleBackup exists but deletionTimestamp is set on the primary cluster",
			testMantleBackup,
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, primaryLabels, true,
					3, metav1.ConditionTrue, metav1.ConditionTrue),
			},
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, secondaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, false, false, nil),
		Entry("Synced MantleBackup exists but deletionTimestamp is set on the secondary cluster",
			testMantleBackup,
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, primaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionTrue),
			},
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, secondaryLabels, true,
					3, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, false, false, nil),
		Entry("The candidate for the diff origin MantleBackup does not exist on the primary cluster",
			testMantleBackup,
			[]*mantlev1.MantleBackup{},
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, secondaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, false, false, nil),
		Entry("The candidate for the diff origin MantleBackup does not exist on the secondary cluster",
			testMantleBackup,
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, primaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionTrue),
			},
			[]*mantlev1.MantleBackup{
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, false, false, nil),
		Entry("Incremental backup",
			testMantleBackup,
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap1", testNamespace, nil, primaryLabels, false,
					1, metav1.ConditionTrue, metav1.ConditionTrue),
				newMantleBackup("testSnap3", testNamespace, nil, primaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionTrue),
			},
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap1", testNamespace, nil, secondaryLabels, false,
					1, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup("testSnap3", testNamespace, nil, secondaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, true, false, newMantleBackup("testSnap3", testNamespace, nil, primaryLabels,
				false, 3, metav1.ConditionTrue, metav1.ConditionTrue),
		),
		Entry("Incremental backup but skips the MantleBackup which only exists on the primary cluster",
			testMantleBackup,
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap1", testNamespace, nil, primaryLabels, false,
					1, metav1.ConditionTrue, metav1.ConditionTrue),
				newMantleBackup("testSnap3", testNamespace, nil, primaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionTrue),
			},
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap1", testNamespace, nil, secondaryLabels, false,
					1, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, true, false, newMantleBackup("testSnap1", testNamespace, nil, primaryLabels,
				false, 1, metav1.ConditionTrue, metav1.ConditionTrue),
		),
	)

	syncModeFullAnnotation := map[string]string{
		annotSyncMode: syncModeFull,
	}
	syncModeIncAnnotation := map[string]string{
		annotSyncMode: syncModeIncremental,
		annotDiffFrom: "testSnap3",
	}
	DescribeTable("MantleBackup with annotations", doTest,
		Entry(`Skip search because the MantleBackup already has the "sync-mode: full" annotation.`,
			newMantleBackup(testName, testNamespace, syncModeFullAnnotation, primaryLabels,
				false, 5, metav1.ConditionTrue, metav1.ConditionFalse),
			[]*mantlev1.MantleBackup{},
			[]*mantlev1.MantleBackup{
				newMantleBackup(testName, testNamespace, syncModeFullAnnotation, secondaryLabels,
					false, 5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, false, false, nil),
		Entry(`Skip search because the MantleBackup already has the "sync-mode: incremental" annotation.`,
			newMantleBackup(testName, testNamespace, syncModeIncAnnotation, primaryLabels,
				false, 5, metav1.ConditionTrue, metav1.ConditionFalse),
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap1", testNamespace, nil, primaryLabels, false,
					1, metav1.ConditionTrue, metav1.ConditionTrue),
				newMantleBackup("testSnap3", testNamespace, nil, primaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionTrue),
			},
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap1", testNamespace, nil, secondaryLabels, false,
					1, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup("testSnap3", testNamespace, nil, secondaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup(testName, testNamespace, syncModeIncAnnotation, secondaryLabels,
					false, 5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, true, false, newMantleBackup("testSnap3", testNamespace, nil, primaryLabels,
				false, 3, metav1.ConditionTrue, metav1.ConditionTrue),
		),
	)
})
