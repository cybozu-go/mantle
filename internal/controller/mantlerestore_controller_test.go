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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type mantleRestoreControllerUnitTest struct {
	reconciler      *MantleRestoreReconciler
	mgrUtil         testutil.ManagerUtil
	tenantNamespace string
	srcPVC          *corev1.PersistentVolumeClaim
	srcPV           *corev1.PersistentVolume
	backup          *mantlev1.MantleBackup
}

var _ = Describe("MantleRestoreReconciler unit test", func() {
	var test mantleRestoreControllerUnitTest

	Describe("setup environment", test.setupEnv)
	Describe("test imageName", test.testImageName)
	Describe("test pvName", test.testPvName)
	Describe("test createRestoringPV", test.testCreateRestoringPV)
	Describe("test createRestoringPVC", test.testCreateRestoringPVC)
	Describe("test deleteRestoringPVC", test.testDeleteRestoringPVC)
	Describe("test deleteRestoringPV", test.testDeleteRestoringPV)
	Describe("tearDown environment", test.tearDownEnv)
})

func (test *mantleRestoreControllerUnitTest) setupEnv() {
	It("creating namespace for test", func() {
		test.tenantNamespace = resMgr.CreateNamespace()
	})

	It("prepare reconcilers", func() {
		By("prepare MantleBackup reconciler")
		test.mgrUtil = testutil.NewManagerUtil(context.Background(), cfg, scheme.Scheme)
		backupReconciler := NewMantleBackupReconciler(
			test.mgrUtil.GetManager().GetClient(),
			test.mgrUtil.GetManager().GetScheme(),
			resMgr.ClusterID,
			RoleStandalone,
			nil,
			"dummy image",
			"",
			nil,
			nil,
			resource.MustParse("1Gi"),
		)
		backupReconciler.ceph = testutil.NewFakeRBD()
		err := backupReconciler.SetupWithManager(test.mgrUtil.GetManager())
		Expect(err).NotTo(HaveOccurred())

		By("prepare MantleRestore reconciler")
		// just allocate the reconciler, and does not start it.
		test.reconciler = NewMantleRestoreReconciler(
			test.mgrUtil.GetManager().GetClient(),
			test.mgrUtil.GetManager().GetScheme(),
			resMgr.ClusterID,
			RoleStandalone,
		)

		test.mgrUtil.Start()
		time.Sleep(100 * time.Millisecond)
	})

	It("create backup resources", func(ctx SpecContext) {
		var err error
		test.srcPV, test.srcPVC, err = resMgr.CreateUniquePVAndPVC(ctx, test.tenantNamespace)
		Expect(err).NotTo(HaveOccurred())
		test.backup, err = resMgr.CreateUniqueBackupFor(ctx, test.srcPVC)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the backup to be captured")
		resMgr.WaitForBackupSnapshotCaptured(ctx, test.backup)
	})
}

func (test *mantleRestoreControllerUnitTest) tearDownEnv() {
	It("delete backup resources", func(ctx SpecContext) {
		_ = k8sClient.Delete(ctx, test.backup)
		_ = k8sClient.Delete(ctx, test.srcPVC)
		_ = k8sClient.Delete(ctx, test.srcPV)
	})

	It("stop backup controller", func() {
		test.mgrUtil.Stop()
	})

	It("deleting resources", func(ctx SpecContext) {
		_ = k8sClient.Delete(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: test.tenantNamespace,
			},
		})
	})
}

func (test *mantleRestoreControllerUnitTest) testImageName() {
	mr := &mantlev1.MantleRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-name",
			Namespace: "test-namespace",
		},
	}

	It("should return the correct image name", func() {
		Expect(test.reconciler.restoringRBDImageName(mr)).To(Equal("mantle-test-namespace-test-name"))
	})
}

func (test *mantleRestoreControllerUnitTest) testPvName() {
	mr := &mantlev1.MantleRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-name",
			Namespace: "test-namespace",
		},
	}

	It("should return the correct PV name", func() {
		Expect(test.reconciler.restoringPVName(mr)).To(Equal("mr-test-namespace-test-name"))
	})
}

func (test *mantleRestoreControllerUnitTest) testCreateRestoringPV() {
	var pv1 corev1.PersistentVolume
	var restore *mantlev1.MantleRestore

	It("should prepare restore resource", func() {
		restore = test.restoreResource()
	})

	It("should create a correct PV", func(ctx SpecContext) {
		err := test.reconciler.createOrUpdateRestoringPV(ctx, restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		err = k8sClient.Get(ctx, client.ObjectKey{Name: fmt.Sprintf("mr-%s-%s", test.tenantNamespace, restore.Name)}, &pv1)
		Expect(err).NotTo(HaveOccurred())

		Expect(pv1.UID).NotTo(BeEmpty())
		Expect(pv1.Annotations[PVAnnotationRestoredBy]).To(Equal(string(restore.UID)))
		Expect(pv1.Spec.AccessModes).To(Equal(test.srcPV.Spec.AccessModes))
		// CSATEST-1490
		pvCapacity, ok := pv1.Spec.Capacity.Storage().AsInt64()
		Expect(ok).To(BeTrue())
		Expect(pvCapacity).To(Equal(int64(testutil.FakeRBDSnapshotSize)))
		Expect(pv1.Spec.ClaimRef).To(BeNil())
		Expect(pv1.Spec.PersistentVolumeSource.CSI.Driver).To(Equal(test.srcPV.Spec.CSI.Driver))
		Expect(pv1.Spec.PersistentVolumeSource.CSI.VolumeAttributes).To(HaveLen(4))
		Expect(pv1.Spec.PersistentVolumeSource.CSI.VolumeAttributes["clusterID"]).To(Equal(test.srcPV.Spec.CSI.VolumeAttributes["clusterID"]))
		Expect(pv1.Spec.PersistentVolumeSource.CSI.VolumeAttributes["imageFeatures"]).To(Equal(test.srcPV.Spec.CSI.VolumeAttributes["imageFeatures"] + ",deep-flatten"))
		Expect(pv1.Spec.PersistentVolumeSource.CSI.VolumeAttributes["pool"]).To(Equal(test.srcPV.Spec.CSI.VolumeAttributes["pool"]))
		Expect(pv1.Spec.PersistentVolumeSource.CSI.VolumeAttributes["staticVolume"]).To(Equal("true"))
		Expect(pv1.Spec.PersistentVolumeSource.CSI.VolumeHandle).To(Equal(fmt.Sprintf("mantle-%s-%s", restore.Namespace, restore.Name)))
		Expect(pv1.Spec.StorageClassName).To(Equal(test.srcPV.Spec.StorageClassName))
		Expect(pv1.Spec.VolumeMode).To(Equal(test.srcPV.Spec.VolumeMode))
		Expect(pv1.Spec.PersistentVolumeReclaimPolicy).To(Equal(corev1.PersistentVolumeReclaimRetain))
	})

	It("should skip creating a PV if it already exists", func(ctx SpecContext) {
		err := test.reconciler.createOrUpdateRestoringPV(ctx, restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		By("PV should not be updated")
		var pv2 corev1.PersistentVolume
		err = k8sClient.Get(ctx, client.ObjectKey{Name: fmt.Sprintf("mr-%s-%s", test.tenantNamespace, restore.Name)}, &pv2)
		Expect(err).NotTo(HaveOccurred())

		Expect(pv1).To(Equal(pv2))
	})

	It("should return an error, if the PV already exists and having different RestoredBy annotation", func(ctx SpecContext) {
		restoreDifferent := restore.DeepCopy()
		restoreDifferent.UID = types.UID(util.GetUniqueName("uid-"))

		err := test.reconciler.createOrUpdateRestoringPV(ctx, restoreDifferent, test.backup)
		Expect(err).To(HaveOccurred())

		By("PV should not be updated")
		var pv3 corev1.PersistentVolume
		err = k8sClient.Get(ctx, client.ObjectKey{Name: fmt.Sprintf("mr-%s-%s", test.tenantNamespace, restore.Name)}, &pv3)
		Expect(err).NotTo(HaveOccurred())

		Expect(pv1).To(Equal(pv3))
	})
}

func (test *mantleRestoreControllerUnitTest) testCreateRestoringPVC() {
	var pvc1 corev1.PersistentVolumeClaim
	var restore *mantlev1.MantleRestore

	It("should prepare restore resource", func() {
		restore = test.restoreResource()
	})

	It("should return an error if the PVC already exists and has no RestoredBy annotation", func(ctx SpecContext) {
		err := k8sClient.Create(ctx, &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: restore.Name, Namespace: test.tenantNamespace},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: test.srcPVC.Spec.Resources.Requests,
				},
				StorageClassName: test.srcPVC.Spec.StorageClassName,

				// We intentionally leave volumeName unset here. Setting it
				// would cause createOrUpdate to fail since volumeName is
				// immutable. Our goal is to test that createOrUpdate isn't
				// applied to this PVC, so we keep it empty.
			},
		})
		Expect(err).NotTo(HaveOccurred())

		var pvc corev1.PersistentVolumeClaim
		err = k8sClient.Get(ctx, client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &pvc)
		Expect(err).NotTo(HaveOccurred())

		err = test.reconciler.createOrUpdateRestoringPVC(ctx, restore, test.backup)
		Expect(err).To(HaveOccurred())

		By("PVC should not be updated")
		var updatedPVC corev1.PersistentVolumeClaim
		err = k8sClient.Get(ctx, client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &updatedPVC)
		Expect(err).NotTo(HaveOccurred())

		Expect(pvc).To(Equal(updatedPVC))

		// delete pvc0 for the following tests
		pvc.Finalizers = []string{}
		err = k8sClient.Update(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())
		err = k8sClient.Delete(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())
		err = k8sClient.Get(ctx, client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &updatedPVC)
		Expect(errors.IsNotFound(err)).To(BeTrue())
	})

	It("should create a correct PVC", func(ctx SpecContext) {
		err := test.reconciler.createOrUpdateRestoringPVC(ctx, restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		err = k8sClient.Get(ctx, client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &pvc1)
		Expect(err).NotTo(HaveOccurred())

		Expect(pvc1.UID).NotTo(BeEmpty())
		Expect(pvc1.Annotations[PVCAnnotationRestoredBy]).To(Equal(string(restore.UID)))
		Expect(pvc1.Spec.AccessModes).To(Equal(test.srcPVC.Spec.AccessModes))
		// CSATEST-1489
		pvcCapacity, ok := pvc1.Spec.Resources.Requests.Storage().AsInt64()
		Expect(ok).To(BeTrue())
		Expect(pvcCapacity).To(Equal(int64(testutil.FakeRBDSnapshotSize)))
		Expect(pvc1.Spec.StorageClassName).To(Equal(test.srcPVC.Spec.StorageClassName))
		Expect(pvc1.Spec.VolumeMode).To(Equal(test.srcPVC.Spec.VolumeMode))
		Expect(pvc1.Spec.VolumeName).To(Equal(fmt.Sprintf("mr-%s-%s", test.tenantNamespace, restore.Name)))
		Expect(controllerutil.HasControllerReference(&pvc1)).To(BeTrue())
		Expect(*pvc1.GetObjectMeta().GetOwnerReferences()[0].Controller).To(BeTrue())
		Expect(pvc1.GetObjectMeta().GetOwnerReferences()[0].Kind).To(Equal("MantleRestore"))
		Expect(pvc1.GetObjectMeta().GetOwnerReferences()[0].UID).To(Equal(restore.GetUID()))
	})

	It("should skip creating a PVC if it already exists", func(ctx SpecContext) {
		err := test.reconciler.createOrUpdateRestoringPVC(ctx, restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		By("PVC should not be updated")
		var pvc2 corev1.PersistentVolumeClaim
		err = k8sClient.Get(ctx, client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &pvc2)
		Expect(err).NotTo(HaveOccurred())

		Expect(pvc1).To(Equal(pvc2))
	})

	It("should return an error, if the PVC already exists and having different RestoredBy annotation", func(ctx SpecContext) {
		restoreDifferent := restore.DeepCopy()
		restoreDifferent.UID = types.UID(util.GetUniqueName("uid-"))

		err := test.reconciler.createOrUpdateRestoringPVC(ctx, restoreDifferent, test.backup)
		Expect(err).To(HaveOccurred())

		By("PVC should not be updated")
		var pvc3 corev1.PersistentVolumeClaim
		err = k8sClient.Get(ctx, client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &pvc3)
		Expect(err).NotTo(HaveOccurred())

		Expect(pvc1).To(Equal(pvc3))
	})
}

func (test *mantleRestoreControllerUnitTest) testDeleteRestoringPVC() {
	var restore *mantlev1.MantleRestore

	It("should prepare restore resource", func() {
		restore = test.restoreResource()
	})

	It("should delete the PVC", func(ctx SpecContext) {
		var pvc corev1.PersistentVolumeClaim

		err := test.reconciler.createOrUpdateRestoringPVC(ctx, restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		Eventually(ctx, func(g Gomega) {
			err = k8sClient.Get(ctx, client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &pvc)
			g.Expect(err).NotTo(HaveOccurred())
		}).Should(Succeed())

		err = test.reconciler.deleteRestoringPVC(ctx, restore)
		Expect(err).NotTo(HaveOccurred())

		// remove finalizers from PVC to allow deletion
		err = k8sClient.Get(ctx, client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &pvc)
		Expect(err).NotTo(HaveOccurred())
		pvc.Finalizers = nil
		err = k8sClient.Update(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		Eventually(ctx, func(g Gomega) {
			err = k8sClient.Get(ctx, client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &pvc)
			g.Expect(err).To(HaveOccurred())
			g.Expect(errors.IsNotFound(err)).To(BeTrue())
		}).Should(Succeed())
	})

	It("should skip deleting the PVC if it does not exist", func(ctx SpecContext) {
		err := test.reconciler.deleteRestoringPVC(ctx, restore)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should an error, if the PVC having different RestoredBy annotation", func(ctx SpecContext) {
		var pvc corev1.PersistentVolumeClaim
		restoreDifferent := restore.DeepCopy()
		restoreDifferent.UID = types.UID(util.GetUniqueName("uid-"))

		err := test.reconciler.createOrUpdateRestoringPVC(ctx, restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		err = test.reconciler.deleteRestoringPVC(ctx, restoreDifferent)
		Expect(err).To(HaveOccurred())

		// remove finalizers from PVC to allow deletion
		err = k8sClient.Get(ctx, client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &pvc)
		Expect(err).NotTo(HaveOccurred())
		pvc.Finalizers = nil
		err = k8sClient.Update(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())

		// cleanup
		err = test.reconciler.deleteRestoringPVC(ctx, restore)
		Expect(err).NotTo(HaveOccurred())
	})
}

func (test *mantleRestoreControllerUnitTest) testDeleteRestoringPV() {
	var restore *mantlev1.MantleRestore

	It("should prepare restore resource", func() {
		restore = test.restoreResource()
	})

	It("should delete the PV", func(ctx SpecContext) {
		var pv corev1.PersistentVolume

		err := test.reconciler.createOrUpdateRestoringPV(ctx, restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		Eventually(ctx, func(g Gomega) {
			err = k8sClient.Get(ctx, client.ObjectKey{Name: test.reconciler.restoringPVName(restore)}, &pv)
			g.Expect(err).NotTo(HaveOccurred())
		}).Should(Succeed())

		err = test.reconciler.deleteRestoringPV(ctx, restore)
		Expect(err).NotTo(HaveOccurred())

		// remove finalizers from PV to allow deletion
		err = k8sClient.Get(ctx, client.ObjectKey{Name: test.reconciler.restoringPVName(restore)}, &pv)
		Expect(err).NotTo(HaveOccurred())
		pv.Finalizers = nil
		err = k8sClient.Update(ctx, &pv)
		Expect(err).NotTo(HaveOccurred())

		Eventually(ctx, func(g Gomega) {
			err = k8sClient.Get(ctx, client.ObjectKey{Name: test.reconciler.restoringPVName(restore)}, &pv)
			g.Expect(err).To(HaveOccurred())
			g.Expect(errors.IsNotFound(err)).To(BeTrue())
		}).Should(Succeed())
	})

	It("should skip deleting the PV if it does not exist", func(ctx SpecContext) {
		err := test.reconciler.deleteRestoringPV(ctx, restore)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should an error, if the PV having different RestoredBy annotation", func(ctx SpecContext) {
		var pv corev1.PersistentVolume
		restoreDifferent := restore.DeepCopy()
		restoreDifferent.UID = types.UID(util.GetUniqueName("uid-"))

		err := test.reconciler.createOrUpdateRestoringPV(ctx, restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		// Make sure the client cache stores the restoring PV.
		Eventually(func(g Gomega) {
			err = k8sClient.Get(ctx, client.ObjectKey{Name: test.reconciler.restoringPVName(restore)}, &pv)
			g.Expect(err).NotTo(HaveOccurred())
		}).Should(Succeed())

		err = test.reconciler.deleteRestoringPV(ctx, restoreDifferent)
		Expect(err).To(HaveOccurred())

		// remove finalizers from PV to allow deletion
		err = k8sClient.Get(ctx, client.ObjectKey{Name: test.reconciler.restoringPVName(restore)}, &pv)
		Expect(err).NotTo(HaveOccurred())
		pv.Finalizers = nil
		err = k8sClient.Update(ctx, &pv)
		Expect(err).NotTo(HaveOccurred())

		// cleanup
		err = test.reconciler.deleteRestoringPV(ctx, restore)
		Expect(err).NotTo(HaveOccurred())
	})
}

// helper function to get MantleRestore object.
func (test *mantleRestoreControllerUnitTest) restoreResource() *mantlev1.MantleRestore {
	return &mantlev1.MantleRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GetUniqueName("restore-"),
			Namespace: test.tenantNamespace,
			UID:       types.UID(util.GetUniqueName("uid-")),
		},
		Spec: mantlev1.MantleRestoreSpec{
			Backup: test.backup.Name,
		},
	}
}
