package controller

import (
	"context"
	"fmt"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func setMockedGetRunningPod(namespace string) {
	getRunningPod = func(ctx context.Context, client client.Client) (*corev1.Pod, error) {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "controller",
				Namespace: namespace,
			},
			Spec: corev1.PodSpec{
				ServiceAccountName: "controller-sa",
				Containers: []corev1.Container{
					{
						Image: "controller-image",
					},
				},
			},
		}, nil
	}
}

func createMBC(ctx context.Context, mbcName, mbcNamespace, pvcName, schedule, expire string, suspend bool) error {
	mbc := mantlev1.MantleBackupConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mbcName,
			Namespace: mbcNamespace,
		},
		Spec: mantlev1.MantleBackupConfigSpec{
			PVC:      pvcName,
			Schedule: schedule,
			Expire:   expire,
			Suspend:  suspend,
		},
	}
	if err := k8sClient.Create(ctx, &mbc); err != nil {
		return err
	}
	return nil
}

var _ = Describe("MantleBackupConfig controller", func() {
	var mgrUtil testutil.ManagerUtil
	var reconciler *MantleBackupConfigReconciler

	BeforeEach(func() {
		mgrUtil = testutil.NewManagerUtil(context.Background(), cfg, scheme.Scheme)

		reconciler = NewMantleBackupConfigReconciler(
			mgrUtil.GetManager().GetClient(),
			mgrUtil.GetManager().GetScheme(),
			resMgr.ClusterID,
			"",
			RoleStandalone,
		)
		err := reconciler.SetupWithManager(mgrUtil.GetManager())
		Expect(err).NotTo(HaveOccurred())

		mgrUtil.Start()
		time.Sleep(100 * time.Millisecond)
	})

	AfterEach(func() {
		err := mgrUtil.Stop()
		Expect(err).NotTo(HaveOccurred())
	})

	DescribeTable("MantleBackupConfigs with correct fields",
		func(ctx SpecContext, schedule, expire string) {
			ns := resMgr.CreateNamespace()
			_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())

			mbc := mantlev1.MantleBackupConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      util.GetUniqueName("mbc-"),
					Namespace: ns,
				},
				Spec: mantlev1.MantleBackupConfigSpec{
					PVC:      pvc.Name,
					Schedule: schedule,
					Expire:   expire,
					Suspend:  false,
				},
			}
			err = k8sClient.Create(ctx, &mbc)
			Expect(err).NotTo(HaveOccurred())
		},
		Entry("various expires 1", "0 0 * * *", "24h"),
		Entry("various expires 2", "0 0 * * *", "1d"),
		Entry("various expires 3", "0 0 * * *", "15d"),
		Entry("various expires 4", "0 0 * * *", "1w"),
		Entry("various expires 5", "0 0 * * *", "2w"),
		Entry("various expires 6", "0 0 * * *", "1w2d3h4m5s"),
		Entry("unusual spacing", "  0   0 *   * *     ", "2w"),
	)

	It("should accept MantleBackupConfigs with all possible schedules", func(ctx SpecContext) {
		ns := resMgr.CreateNamespace()

		_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())

		schedules := []string{}
		for m := 0; m < 60; m++ {
			for h := 0; h < 24; h++ {
				schedules = append(schedules, fmt.Sprintf("%d %d * * *", m, h))
				if m < 10 {
					schedules = append(schedules, fmt.Sprintf("%02d %d * * *", m, h))
				}
				if h < 10 {
					schedules = append(schedules, fmt.Sprintf("%d %02d * * *", m, h))
				}
				if m < 10 && h < 10 {
					schedules = append(schedules, fmt.Sprintf("%02d %02d * * *", m, h))
				}
			}
		}
		for _, schedule := range schedules {
			mbc := mantlev1.MantleBackupConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      util.GetUniqueName("mbc-"),
					Namespace: ns,
				},
				Spec: mantlev1.MantleBackupConfigSpec{
					PVC:      pvc.Name,
					Schedule: schedule,
					Expire:   "2w",
					Suspend:  false,
				},
			}
			err := k8sClient.Create(ctx, &mbc)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	DescribeTable("MantleBackupConfigs with incorrect fields",
		func(ctx SpecContext, schedule, expire string) {
			ns := resMgr.CreateNamespace()

			_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())

			mbc := mantlev1.MantleBackupConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      util.GetUniqueName("mbc-"),
					Namespace: ns,
				},
				Spec: mantlev1.MantleBackupConfigSpec{
					PVC:      pvc.Name,
					Schedule: schedule,
					Expire:   expire,
					Suspend:  false,
				},
			}
			err = k8sClient.Create(ctx, &mbc)
			Expect(err).To(HaveOccurred())
		},
		Entry("incorrectly formatted schedules 1", "0 0 * *", "2w"),
		Entry("incorrectly formatted schedules 2", "0 0 0 0 0", "2w"),
		Entry("incorrectly formatted schedules 3", "0 0 0 * *", "2w"),
		Entry("incorrectly formatted schedules 4", "0 0 0 0 *", "2w"),
		Entry("incorrectly formatted schedules 5", "0 0 0 0 0", "2w"),
		Entry("incorrectly formatted schedules 6", "0 24 0 0 0", "2w"),
		Entry("incorrectly formatted schedules 7", "60 0 0 0 0", "2w"),
		Entry("incorrectly formatted expires 1", "0 0 * * *", "foo"),
		Entry("too short expires 1", "0 0 * * *", "23h59m59s"),
		Entry("too long expires 1", "0 0 * * *", "15d1s"),
	)

	It("should accept MantleBackupConfigs with modified mutable fields", func(ctx SpecContext) {
		ns := resMgr.CreateNamespace()

		oldSchedule := "0 0 * * *"
		newSchedule := "0 10 * * *"
		oldSuspend := false
		newSuspend := true

		_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())

		mbc := mantlev1.MantleBackupConfig{
			ObjectMeta: metav1.ObjectMeta{
				Name:      util.GetUniqueName("mbc-"),
				Namespace: ns,
			},
			Spec: mantlev1.MantleBackupConfigSpec{
				PVC:      pvc.Name,
				Schedule: oldSchedule,
				Expire:   "2w",
				Suspend:  oldSuspend,
			},
		}
		err = k8sClient.Create(ctx, &mbc)
		Expect(err).NotTo(HaveOccurred())

		// schedule and suspend fields are mutable.
		mbc.Spec.Schedule = newSchedule
		mbc.Spec.Suspend = newSuspend
		err = k8sClient.Update(ctx, &mbc)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should reject MantleBackupConfigs with modified immutable fields", func(ctx SpecContext) {
		ns := resMgr.CreateNamespace()

		oldExpire := "2w"
		newExpire := "1w"

		_, oldPVC, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())
		_, newPVC, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())

		mbc := mantlev1.MantleBackupConfig{
			ObjectMeta: metav1.ObjectMeta{
				Name:      util.GetUniqueName("mbc-"),
				Namespace: ns,
			},
			Spec: mantlev1.MantleBackupConfigSpec{
				PVC:      oldPVC.Name,
				Schedule: "0 0 * * *",
				Expire:   oldExpire,
				Suspend:  false,
			},
		}
		err = k8sClient.Create(ctx, &mbc)
		Expect(err).NotTo(HaveOccurred())

		mbc1 := mbc.DeepCopy()
		mbc1.Spec.PVC = newPVC.Name
		err = k8sClient.Update(ctx, mbc1)
		Expect(err).To(HaveOccurred())

		mbc2 := mbc.DeepCopy()
		mbc2.Spec.Expire = newExpire
		err = k8sClient.Update(ctx, mbc2)
		Expect(err).To(HaveOccurred())
	})

	It("should create a CronJob for a valid MantleBackupConfig resource and delete the CronJob when the MantleBackupConfig is deleted", func(ctx SpecContext) {
		ns := resMgr.CreateNamespace()
		mbcNamespace := ns
		controllerNs := resMgr.CreateNamespace()
		setMockedGetRunningPod(controllerNs)
		mbcName := util.GetUniqueName("mbc-")
		schedule := "0 0 * * *"
		suspend := false

		_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())

		err = createMBC(ctx, mbcName, mbcNamespace, pvc.Name, schedule, "2w", suspend)
		Expect(err).NotTo(HaveOccurred())

		var mbc mantlev1.MantleBackupConfig
		err = k8sClient.Get(ctx, types.NamespacedName{Name: mbcName, Namespace: mbcNamespace}, &mbc)
		Expect(err).NotTo(HaveOccurred())

		cronJobName := "mbc-" + string(mbc.UID)
		testutil.CheckCreatedEventually[batchv1.CronJob](ctx, k8sClient, cronJobName, controllerNs)

		var cronJob batchv1.CronJob
		err = k8sClient.Get(ctx, types.NamespacedName{Name: cronJobName, Namespace: controllerNs}, &cronJob)
		Expect(err).NotTo(HaveOccurred())
		Expect(cronJob.Spec.Schedule).To(Equal(schedule))
		Expect(*cronJob.Spec.Suspend).To(Equal(suspend))
		Expect(cronJob.Spec.ConcurrencyPolicy).To(Equal(batchv1.ForbidConcurrent))
		var expectedStartingDeadlineSeconds int64 = 3600
		Expect(cronJob.Spec.StartingDeadlineSeconds).To(Equal(&expectedStartingDeadlineSeconds))

		err = k8sClient.Delete(ctx, &mbc)
		Expect(err).NotTo(HaveOccurred())

		testutil.CheckDeletedEventually[batchv1.CronJob](ctx, k8sClient, cronJobName, controllerNs)
		testutil.CheckDeletedEventually[mantlev1.MantleBackupConfig](ctx, k8sClient, mbcName, mbcNamespace)
	})

	It("should re-create the CronJob when someone deleted it", func(ctx SpecContext) {
		ns := resMgr.CreateNamespace()
		mbcNamespace := ns
		controllerNs := resMgr.CreateNamespace()
		setMockedGetRunningPod(controllerNs)
		mbcName := util.GetUniqueName("mbc-")

		_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())

		err = createMBC(ctx, mbcName, mbcNamespace, pvc.Name, "59 23 * * *", "2w", false)
		Expect(err).NotTo(HaveOccurred())

		var mbc mantlev1.MantleBackupConfig
		err = k8sClient.Get(ctx, types.NamespacedName{Name: mbcName, Namespace: mbcNamespace}, &mbc)
		Expect(err).NotTo(HaveOccurred())

		cronJobName := "mbc-" + string(mbc.UID)
		testutil.CheckCreatedEventually[batchv1.CronJob](ctx, k8sClient, cronJobName, controllerNs)

		var cronJob batchv1.CronJob
		err = k8sClient.Get(ctx, types.NamespacedName{Name: cronJobName, Namespace: controllerNs}, &cronJob)
		Expect(err).NotTo(HaveOccurred())
		err = k8sClient.Delete(ctx, &cronJob)
		Expect(err).NotTo(HaveOccurred())
		testutil.CheckDeletedEventually[batchv1.CronJob](ctx, k8sClient, cronJobName, controllerNs)
		testutil.CheckCreatedEventually[batchv1.CronJob](ctx, k8sClient, cronJobName, controllerNs)
	})
})
