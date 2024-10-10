package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/testutil"
	"github.com/cybozu-go/mantle/pkg/controller/proto"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
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
	})

	AfterEach(func() {
		err := mgrUtil.Stop()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should be ready to use", func(ctx SpecContext) {
		ns := resMgr.CreateNamespace()

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
		ns := resMgr.CreateNamespace()

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
		ns := resMgr.CreateNamespace()

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
		ns := resMgr.CreateNamespace()

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
		ns := resMgr.CreateNamespace()

		_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())

		backup, err = resMgr.CreateUniqueBackupFor(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())

		err = k8sClient.Create(ctx, backup)
		Expect(err).To(HaveOccurred())
	})
})

type mockGRPCClient struct {
	backup mantlev1.MantleBackup
}

var _ proto.MantleServiceClient = (*mockGRPCClient)(nil)

func (m *mockGRPCClient) CreateOrUpdatePVC(
	ctx context.Context,
	req *proto.CreateOrUpdatePVCRequest,
	opts ...grpc.CallOption,
) (*proto.CreateOrUpdatePVCResponse, error) {
	return &proto.CreateOrUpdatePVCResponse{}, nil
}

func (m *mockGRPCClient) CreateOrUpdateMantleBackup(
	ctx context.Context,
	req *proto.CreateOrUpdateMantleBackupRequest,
	opts ...grpc.CallOption,
) (*proto.CreateOrUpdateMantleBackupResponse, error) {
	err := json.Unmarshal([]byte(req.MantleBackup), &m.backup)
	if err != nil {
		return nil, err
	}
	return &proto.CreateOrUpdateMantleBackupResponse{}, nil
}

func (m *mockGRPCClient) ListMantleBackup(
	ctx context.Context,
	req *proto.ListMantleBackupRequest,
	opts ...grpc.CallOption,
) (*proto.ListMantleBackupResponse, error) {
	backups := []mantlev1.MantleBackup{m.backup}
	data, err := json.Marshal(backups)
	if err != nil {
		return nil, err
	}
	return &proto.ListMantleBackupResponse{
		MantleBackupList: data,
	}, nil
}

var _ = Describe("MantleBackup controller in primary role", func() {
	var mgrUtil testutil.ManagerUtil
	var reconciler *MantleBackupReconciler

	// Not to access backup.Name before created, set the pointer to an empty object.
	backup := &mantlev1.MantleBackup{}

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
			RolePrimary,
			&PrimarySettings{
				Client: &mockGRPCClient{},
			},
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
	})

	AfterEach(func() {
		err := mgrUtil.Stop()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should be synced to remote", func(ctx SpecContext) {
		ns := resMgr.CreateNamespace()

		pv, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())

		backup, err = resMgr.CreateUniqueBackupFor(ctx, pvc)
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

		snapID := backup.Status.SnapID
		Expect(*snapID).To(Equal(1000))

		// TODO: Currently, there is no way to check if the annotations are set correctly.
		// After implementing export() function, the annotations check should be added
		// for various conditions.

		err = k8sClient.Delete(ctx, backup)
		Expect(err).NotTo(HaveOccurred())

		testutil.CheckDeletedEventually[mantlev1.MantleBackup](ctx, k8sClient, backup.Name, backup.Namespace)
	})

})

func int2Ptr(i int) *int {
	return &i
}

var _ = Describe("searchDiffOriginMantleBackup", func() {
	testMantleBackup := mantlev1.MantleBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test0",
		},
		Status: mantlev1.MantleBackupStatus{
			SnapID: int2Ptr(5),
		},
	}

	basePrimaryBackups := []mantlev1.MantleBackup{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test1",
			},
			Status: mantlev1.MantleBackupStatus{
				Conditions: []metav1.Condition{
					{
						Type:   mantlev1.BackupConditionReadyToUse,
						Status: metav1.ConditionTrue,
					},
				},
				SnapID: int2Ptr(1),
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test2",
			},
			Status: mantlev1.MantleBackupStatus{
				Conditions: []metav1.Condition{
					{
						Type:   mantlev1.BackupConditionReadyToUse,
						Status: metav1.ConditionTrue,
					},
				},
				SnapID: int2Ptr(6),
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test3",
			},
			Status: mantlev1.MantleBackupStatus{
				Conditions: []metav1.Condition{
					{
						Type:   mantlev1.BackupConditionReadyToUse,
						Status: metav1.ConditionTrue,
					},
				},
				SnapID: int2Ptr(3),
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test4",
			},
			Status: mantlev1.MantleBackupStatus{
				Conditions: []metav1.Condition{
					{
						Type:   mantlev1.BackupConditionReadyToUse,
						Status: metav1.ConditionTrue,
					},
				},
				SnapID: int2Ptr(4),
			},
		},
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
		"test2": basePrimaryBackups[1].DeepCopy(),
		"test3": basePrimaryBackups[2].DeepCopy(),
		// "test4" is intentionally omitted.
	}

	DescribeTable("Search for the MantleBackup which is used for the diff origin",
		func(backup *mantlev1.MantleBackup,
			primaryBackups []mantlev1.MantleBackup,
			secondaryBackupSet map[string]*mantlev1.MantleBackup,
			shouldFindBackup bool,
			expectedBackupName string) {
			foundBackup := searchForDiffOriginMantleBackup(backup, primaryBackups, secondaryBackupSet)
			if shouldFindBackup {
				Expect(foundBackup).NotTo(BeNil())
				Expect(foundBackup.GetName()).To(Equal(expectedBackupName))
			} else {
				Expect(foundBackup).To(BeNil())
			}
		},
		Entry("should return nil when no MantleBackup found on the secondary cluster",
			&testMantleBackup, basePrimaryBackups, make(map[string]*mantlev1.MantleBackup),
			false, ""),
		Entry("should find the correct MantleBackup",
			&testMantleBackup, basePrimaryBackups, testSecondaryMantleBackups,
			true, "test3"),
		Entry("should skip the not-ready MantleBackup",
			&testMantleBackup, primaryBackupsWithConditionFalse, testSecondaryMantleBackups,
			true, "test1"),
		Entry("should skip the MantleBackup with the deletion timestamp",
			&testMantleBackup, primaryBackupsWithDeletionTimestamp, testSecondaryMantleBackups,
			true, "test1"),
	)
})
