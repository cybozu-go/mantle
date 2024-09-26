package controller

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
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

func createBoundPVC(ctx context.Context, ns, pvName, pvcName, storageClassName string) error {
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
			Name: pvName,
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
	if err := k8sClient.Create(ctx, &pv); err != nil {
		return err
	}

	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
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
	if err := k8sClient.Create(ctx, &pvc); err != nil {
		return err
	}

	pvc.Status.Phase = corev1.ClaimBound
	if err := k8sClient.Status().Update(ctx, &pvc); err != nil {
		return err
	}

	return nil
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

// cf. https://go.googlesource.com/proposal/+/refs/heads/master/design/43651-type-parameters.md#pointer-method-example
type ObjectConstraint[T any] interface {
	client.Object
	*T
}

func checkCreatedEventually[T any, OC ObjectConstraint[T]](ctx context.Context, name, namespace string) {
	var obj T
	Eventually(func() error {
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, OC(&obj))
		if err != nil {
			return err
		}
		return nil
	}).Should(Succeed())
}

func checkDeletedEventually[T any, OC ObjectConstraint[T]](ctx context.Context, name, namespace string) {
	var obj T
	Eventually(func() error {
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, OC(&obj))
		if errors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("\"%s\" is not deleted yet", name)
	}).Should(Succeed())
}

var _ = Describe("MantleBackupConfig controller", func() {
	ctx := context.Background()

	var mgrUtil testutil.ManagerUtil
	var reconciler *MantleBackupConfigReconciler

	storageClassClusterID := dummyStorageClassClusterID
	storageClassName := dummyStorageClassName

	BeforeEach(func() {
		mgrUtil = testutil.NewManagerUtil(ctx, cfg, scheme.Scheme)

		reconciler = NewMantleBackupConfigReconciler(k8sClient, mgrUtil.GetScheme(), storageClassClusterID, "0s", "", RoleStandalone)
		err := reconciler.SetupWithManager(mgrUtil.GetManager())
		Expect(err).NotTo(HaveOccurred())

		executeCommand = func(_ *slog.Logger, _ []string, _ io.Reader) ([]byte, error) {
			return nil, nil
		}

		mgrUtil.Start()
		time.Sleep(100 * time.Millisecond)
	})

	AfterEach(func() {
		err := mgrUtil.Stop()
		Expect(err).NotTo(HaveOccurred())
	})

	DescribeTable("MantleBackupConfigs with correct fields",
		func(schedule, expire string) {
			ns := createNamespace()
			pvName := util.GetUniqueName("pv-")
			pvcName := util.GetUniqueName("pvc-")

			err := createBoundPVC(ctx, ns, pvName, pvcName, storageClassName)
			Expect(err).NotTo(HaveOccurred())

			mbc := mantlev1.MantleBackupConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      util.GetUniqueName("mbc-"),
					Namespace: ns,
				},
				Spec: mantlev1.MantleBackupConfigSpec{
					PVC:      pvcName,
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

	It("should accept MantleBackupConfigs with all possible schedules", func() {
		ns := createNamespace()
		pvName := util.GetUniqueName("pv-")
		pvcName := util.GetUniqueName("pvc-")

		err := createBoundPVC(ctx, ns, pvName, pvcName, storageClassName)
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
					PVC:      pvcName,
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
		func(schedule, expire string) {
			pvName := util.GetUniqueName("pv-")
			pvcName := util.GetUniqueName("pvc-")
			ns := createNamespace()

			err := createBoundPVC(ctx, ns, pvName, pvcName, storageClassName)
			Expect(err).NotTo(HaveOccurred())

			mbc := mantlev1.MantleBackupConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      util.GetUniqueName("mbc-"),
					Namespace: ns,
				},
				Spec: mantlev1.MantleBackupConfigSpec{
					PVC:      pvcName,
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

	It("should accept MantleBackupConfigs with modified mutable fields", func() {
		ns := createNamespace()

		pvName := util.GetUniqueName("pv-")
		pvcName := util.GetUniqueName("pvc-")

		oldSchedule := "0 0 * * *"
		newSchedule := "0 10 * * *"
		oldSuspend := false
		newSuspend := true

		err := createBoundPVC(ctx, ns, pvName, pvcName, storageClassName)
		Expect(err).NotTo(HaveOccurred())

		mbc := mantlev1.MantleBackupConfig{
			ObjectMeta: metav1.ObjectMeta{
				Name:      util.GetUniqueName("mbc-"),
				Namespace: ns,
			},
			Spec: mantlev1.MantleBackupConfigSpec{
				PVC:      pvcName,
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

	It("should reject MantleBackupConfigs with modified immutable fields", func() {
		ns := createNamespace()

		pvName1 := util.GetUniqueName("pv-")
		pvName2 := util.GetUniqueName("pv-")
		oldPVC := util.GetUniqueName("pvc-")
		newPVC := util.GetUniqueName("pvc-")
		oldExpire := "2w"
		newExpire := "1w"

		err := createBoundPVC(ctx, ns, pvName1, oldPVC, storageClassName)
		Expect(err).NotTo(HaveOccurred())
		err = createBoundPVC(ctx, ns, pvName2, newPVC, storageClassName)
		Expect(err).NotTo(HaveOccurred())

		mbc := mantlev1.MantleBackupConfig{
			ObjectMeta: metav1.ObjectMeta{
				Name:      util.GetUniqueName("mbc-"),
				Namespace: ns,
			},
			Spec: mantlev1.MantleBackupConfigSpec{
				PVC:      oldPVC,
				Schedule: "0 0 * * *",
				Expire:   oldExpire,
				Suspend:  false,
			},
		}
		err = k8sClient.Create(ctx, &mbc)
		Expect(err).NotTo(HaveOccurred())

		mbc1 := mbc.DeepCopy()
		mbc1.Spec.PVC = newPVC
		err = k8sClient.Update(ctx, mbc1)
		Expect(err).To(HaveOccurred())

		mbc2 := mbc.DeepCopy()
		mbc2.Spec.Expire = newExpire
		err = k8sClient.Update(ctx, mbc2)
		Expect(err).To(HaveOccurred())
	})

	It("should create a CronJob for a valid MantleBackupConfig resource and delete the CronJob when the MantleBackupConfig is deleted", func() {
		ctx := context.Background()
		ns := createNamespace()
		mbcNamespace := ns
		controllerNs := createNamespace()
		setMockedGetRunningPod(controllerNs)
		pvName := util.GetUniqueName("pv-")
		pvcName := util.GetUniqueName("pvc-")
		mbcName := util.GetUniqueName("mbc-")
		schedule := "0 0 * * *"
		suspend := false

		err := createBoundPVC(ctx, ns, pvName, pvcName, storageClassName)
		Expect(err).NotTo(HaveOccurred())

		err = createMBC(ctx, mbcName, mbcNamespace, pvcName, schedule, "2w", suspend)
		Expect(err).NotTo(HaveOccurred())

		var mbc mantlev1.MantleBackupConfig
		err = k8sClient.Get(ctx, types.NamespacedName{Name: mbcName, Namespace: mbcNamespace}, &mbc)
		Expect(err).NotTo(HaveOccurred())

		cronJobName := "mbc-" + string(mbc.UID)
		checkCreatedEventually[batchv1.CronJob](ctx, cronJobName, controllerNs)

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

		checkDeletedEventually[batchv1.CronJob](ctx, cronJobName, controllerNs)
		checkDeletedEventually[mantlev1.MantleBackupConfig](ctx, mbcName, mbcNamespace)
	})

	It("should re-create the CronJob when someone deleted it", func() {
		ctx := context.Background()
		ns := createNamespace()
		mbcNamespace := ns
		controllerNs := createNamespace()
		setMockedGetRunningPod(controllerNs)
		pvName := util.GetUniqueName("pv-")
		pvcName := util.GetUniqueName("pvc-")
		mbcName := util.GetUniqueName("mbc-")

		err := createBoundPVC(ctx, ns, pvName, pvcName, storageClassName)
		Expect(err).NotTo(HaveOccurred())

		err = createMBC(ctx, mbcName, mbcNamespace, pvcName, "59 23 * * *", "2w", false)
		Expect(err).NotTo(HaveOccurred())

		var mbc mantlev1.MantleBackupConfig
		err = k8sClient.Get(ctx, types.NamespacedName{Name: mbcName, Namespace: mbcNamespace}, &mbc)
		Expect(err).NotTo(HaveOccurred())

		cronJobName := "mbc-" + string(mbc.UID)
		checkCreatedEventually[batchv1.CronJob](ctx, cronJobName, controllerNs)

		var cronJob batchv1.CronJob
		err = k8sClient.Get(ctx, types.NamespacedName{Name: cronJobName, Namespace: controllerNs}, &cronJob)
		Expect(err).NotTo(HaveOccurred())
		err = k8sClient.Delete(ctx, &cronJob)
		Expect(err).NotTo(HaveOccurred())
		checkDeletedEventually[batchv1.CronJob](ctx, cronJobName, controllerNs)
		checkCreatedEventually[batchv1.CronJob](ctx, cronJobName, controllerNs)
	})
})
