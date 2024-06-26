package controller

import (
	"context"
	"fmt"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	testutil "github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type mantleRestoreControllerUnitTest struct {
	reconciler      *MantleRestoreReconciler
	mgrUtil         testutil.ManagerUtil
	tenantNamespace string
	cephClusterID   string
	poolName        string
	scName          string
	srcPVC          *corev1.PersistentVolumeClaim
	srcPV           *corev1.PersistentVolume
	backupName      string
	backup          *mantlev1.MantleBackup
}

var _ = Describe("MantleRestoreReconciler unit test", func() {
	test := &mantleRestoreControllerUnitTest{
		tenantNamespace: testutil.GetUniqueName("ns-"),
		cephClusterID:   testutil.GetUniqueName("ceph-"),
		poolName:        testutil.GetUniqueName("pool-"),
		scName:          testutil.GetUniqueName("sc-"),
		backupName:      testutil.GetUniqueName("backup-"),
	}

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
	ctx := context.Background()

	It("creating namespace for test", func() {
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: test.tenantNamespace,
			},
		}

		err := k8sClient.Create(ctx, ns)
		Expect(err).NotTo(HaveOccurred())
	})

	It("prepare reconcilers", func() {
		By("prepare MantleBackup reconciler")
		executeCommand = mockExecuteCommand
		test.mgrUtil = testutil.NewManagerUtil(ctx, cfg, scheme.Scheme)
		backupReconciler := NewMantleBackupReconciler(k8sClient, test.mgrUtil.GetScheme(), test.cephClusterID)
		err := backupReconciler.SetupWithManager(test.mgrUtil.GetManager())
		Expect(err).NotTo(HaveOccurred())

		By("prepare MantleRestore reconciler")
		// just allocate the reconciler, and does not start it.
		test.reconciler = NewMantleRestoreReconciler(k8sClient, test.mgrUtil.GetScheme(), test.cephClusterID)

		test.mgrUtil.Start()
		time.Sleep(100 * time.Millisecond)
	})

	It("create backup resources", func() {
		err := testutil.CreateStorageClass(ctx, k8sClient, test.scName, "rook-ceph.rbd.csi.ceph.com", test.cephClusterID)
		Expect(err).NotTo(HaveOccurred())

		test.srcPV, test.srcPVC = test.createPVAndPVC(test.scName, testutil.GetUniqueName("pv-"), testutil.GetUniqueName("pvc-"), testutil.GetUniqueName("image-"))
		test.backup = test.createBackup(test.srcPVC, test.backupName)

		By("waiting for the backup to be ready")

		Eventually(func() bool {
			err := k8sClient.Get(context.Background(), types.NamespacedName{Name: test.backup.Name, Namespace: test.backup.Namespace}, test.backup)
			Expect(err).NotTo(HaveOccurred())

			cond := meta.FindStatusCondition(test.backup.Status.Conditions, mantlev1.BackupConditionReadyToUse)
			return cond != nil && cond.Status == metav1.ConditionTrue
		}, 10*time.Second, 1*time.Second).Should(BeTrue())
	})
}

func (test *mantleRestoreControllerUnitTest) tearDownEnv() {
	It("delete backup resources", func() {
		_ = k8sClient.Delete(context.Background(), test.backup)
		_ = k8sClient.Delete(context.Background(), test.srcPVC)
		_ = k8sClient.Delete(context.Background(), test.srcPV)
	})

	It("stop backup controller", func() {
		err := test.mgrUtil.Stop()
		Expect(err).NotTo(HaveOccurred())
	})

	It("deleting resources", func() {
		_ = k8sClient.Delete(context.Background(), &corev1.Namespace{
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
	restore := &mantlev1.MantleRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testutil.GetUniqueName("restore-"),
			Namespace: test.tenantNamespace,
			UID:       types.UID(testutil.GetUniqueName("uid-")),
		},
		Spec: mantlev1.MantleRestoreSpec{
			Backup: test.backupName,
		},
	}

	It("should create a correct PV", func() {
		err := test.reconciler.createRestoringPV(context.Background(), restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		err = k8sClient.Get(context.Background(), client.ObjectKey{Name: fmt.Sprintf("mr-%s-%s", test.tenantNamespace, restore.Name)}, &pv1)
		Expect(err).NotTo(HaveOccurred())

		Expect(pv1.UID).NotTo(BeEmpty())
		Expect(pv1.Annotations[PVAnnotationRestoredBy]).To(Equal(string(restore.UID)))
		Expect(pv1.Spec.AccessModes).To(Equal(test.srcPV.Spec.AccessModes))
		Expect(pv1.Spec.Capacity).To(Equal(test.srcPV.Spec.Capacity))
		Expect(pv1.Spec.ClaimRef).To(BeNil())
		Expect(pv1.Spec.PersistentVolumeSource.CSI.Driver).To(Equal(test.srcPV.Spec.PersistentVolumeSource.CSI.Driver))
		Expect(pv1.Spec.PersistentVolumeSource.CSI.VolumeAttributes).To(HaveLen(4))
		Expect(pv1.Spec.PersistentVolumeSource.CSI.VolumeAttributes["clusterID"]).To(Equal(test.srcPV.Spec.PersistentVolumeSource.CSI.VolumeAttributes["clusterID"]))
		Expect(pv1.Spec.PersistentVolumeSource.CSI.VolumeAttributes["imageFeatures"]).To(Equal(test.srcPV.Spec.PersistentVolumeSource.CSI.VolumeAttributes["imageFeatures"] + ",deep-flatten"))
		Expect(pv1.Spec.PersistentVolumeSource.CSI.VolumeAttributes["pool"]).To(Equal(test.srcPV.Spec.PersistentVolumeSource.CSI.VolumeAttributes["pool"]))
		Expect(pv1.Spec.PersistentVolumeSource.CSI.VolumeAttributes["staticVolume"]).To(Equal("true"))
		Expect(pv1.Spec.PersistentVolumeSource.CSI.VolumeHandle).To(Equal(fmt.Sprintf("mantle-%s-%s", restore.Namespace, restore.Name)))
		Expect(pv1.Spec.StorageClassName).To(Equal(test.srcPV.Spec.StorageClassName))
		Expect(pv1.Spec.VolumeMode).To(Equal(test.srcPV.Spec.VolumeMode))
		Expect(pv1.Spec.PersistentVolumeReclaimPolicy).To(Equal(corev1.PersistentVolumeReclaimRetain))
	})

	It("should skip creating a PV if it already exists", func() {
		err := test.reconciler.createRestoringPV(context.Background(), restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		By("PV should not be updated")
		var pv2 corev1.PersistentVolume
		err = k8sClient.Get(context.Background(), client.ObjectKey{Name: fmt.Sprintf("mr-%s-%s", test.tenantNamespace, restore.Name)}, &pv2)
		Expect(err).NotTo(HaveOccurred())

		pv1Bin, err := pv1.Marshal()
		Expect(err).NotTo(HaveOccurred())
		pv2Bin, err := pv2.Marshal()
		Expect(err).NotTo(HaveOccurred())
		Expect(pv1Bin).To(Equal(pv2Bin))
	})

	It("should return an error, if the PV already exists and having different RestoredBy annotation", func() {
		restoreDifferent := restore.DeepCopy()
		restoreDifferent.UID = types.UID(testutil.GetUniqueName("uid-"))

		err := test.reconciler.createRestoringPV(context.Background(), restoreDifferent, test.backup)
		Expect(err).To(HaveOccurred())

		By("PV should not be updated")
		var pv3 corev1.PersistentVolume
		err = k8sClient.Get(context.Background(), client.ObjectKey{Name: fmt.Sprintf("mr-%s-%s", test.tenantNamespace, restore.Name)}, &pv3)
		Expect(err).NotTo(HaveOccurred())

		pv1Bin, err := pv1.Marshal()
		Expect(err).NotTo(HaveOccurred())
		pv3Bin, err := pv3.Marshal()
		Expect(err).NotTo(HaveOccurred())
		Expect(pv1Bin).To(Equal(pv3Bin))
	})
}

func (test *mantleRestoreControllerUnitTest) testCreateRestoringPVC() {
	var pvc1 corev1.PersistentVolumeClaim
	restore := &mantlev1.MantleRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testutil.GetUniqueName("restore-"),
			Namespace: test.tenantNamespace,
			UID:       types.UID(testutil.GetUniqueName("uid-")),
		},
		Spec: mantlev1.MantleRestoreSpec{
			Backup: test.backupName,
		},
	}

	It("should create a correct PVC", func() {
		err := test.reconciler.createRestoringPVC(context.Background(), restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		err = k8sClient.Get(context.Background(), client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &pvc1)
		Expect(err).NotTo(HaveOccurred())

		Expect(pvc1.UID).NotTo(BeEmpty())
		Expect(pvc1.Annotations[PVCAnnotationRestoredBy]).To(Equal(string(restore.UID)))
		Expect(pvc1.Spec.AccessModes).To(Equal(test.srcPVC.Spec.AccessModes))
		Expect(pvc1.Spec.Resources.Requests).To(Equal(test.srcPVC.Spec.Resources.Requests))
		Expect(pvc1.Spec.StorageClassName).To(Equal(test.srcPVC.Spec.StorageClassName))
		Expect(pvc1.Spec.VolumeMode).To(Equal(test.srcPVC.Spec.VolumeMode))
		Expect(pvc1.Spec.VolumeName).To(Equal(fmt.Sprintf("mr-%s-%s", test.tenantNamespace, restore.Name)))
	})

	It("should skip creating a PVC if it already exists", func() {
		err := test.reconciler.createRestoringPVC(context.Background(), restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		By("PVC should not be updated")
		var pvc2 corev1.PersistentVolumeClaim
		err = k8sClient.Get(context.Background(), client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &pvc2)
		Expect(err).NotTo(HaveOccurred())

		pvc1Bin, err := pvc1.Marshal()
		Expect(err).NotTo(HaveOccurred())
		pvc2Bin, err := pvc2.Marshal()
		Expect(err).NotTo(HaveOccurred())
		Expect(pvc1Bin).To(Equal(pvc2Bin))
	})

	It("should return an error, if the PVC already exists and having different RestoredBy annotation", func() {
		restoreDifferent := restore.DeepCopy()
		restoreDifferent.UID = types.UID(testutil.GetUniqueName("uid-"))

		err := test.reconciler.createRestoringPVC(context.Background(), restoreDifferent, test.backup)
		Expect(err).To(HaveOccurred())

		By("PVC should not be updated")
		var pvc3 corev1.PersistentVolumeClaim
		err = k8sClient.Get(context.Background(), client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &pvc3)
		Expect(err).NotTo(HaveOccurred())

		pvc1Bin, err := pvc1.Marshal()
		Expect(err).NotTo(HaveOccurred())
		pvc3Bin, err := pvc3.Marshal()
		Expect(err).NotTo(HaveOccurred())
		Expect(pvc1Bin).To(Equal(pvc3Bin))
	})
}

func (test *mantleRestoreControllerUnitTest) testDeleteRestoringPVC() {
	restore := &mantlev1.MantleRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testutil.GetUniqueName("restore-"),
			Namespace: test.tenantNamespace,
			UID:       types.UID(testutil.GetUniqueName("uid-")),
		},
		Spec: mantlev1.MantleRestoreSpec{
			Backup: test.backupName,
		},
	}

	It("should delete the PVC", func() {
		var pvc corev1.PersistentVolumeClaim

		err := test.reconciler.createRestoringPVC(context.Background(), restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		// remove pvc-protection finalizer from PVC to allow deletion
		err = k8sClient.Get(context.Background(), client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &pvc)
		Expect(err).NotTo(HaveOccurred())
		pvc.Finalizers = nil
		err = k8sClient.Update(context.Background(), &pvc)
		Expect(err).NotTo(HaveOccurred())

		err = test.reconciler.deleteRestoringPVC(context.Background(), restore)
		Expect(err).NotTo(HaveOccurred())

		err = k8sClient.Get(context.Background(), client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &pvc)
		Expect(err).To(HaveOccurred())
		Expect(errors.IsNotFound(err)).To(BeTrue())
	})

	It("should skip deleting the PVC if it does not exist", func() {
		err := test.reconciler.deleteRestoringPVC(context.Background(), restore)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should an error, if the PVC having different RestoredBy annotation", func() {
		var pvc corev1.PersistentVolumeClaim
		restoreDifferent := restore.DeepCopy()
		restoreDifferent.UID = types.UID(testutil.GetUniqueName("uid-"))

		err := test.reconciler.createRestoringPVC(context.Background(), restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		// remove pvc-protection finalizer from PVC to allow deletion
		err = k8sClient.Get(context.Background(), client.ObjectKey{Name: restore.Name, Namespace: test.tenantNamespace}, &pvc)
		Expect(err).NotTo(HaveOccurred())
		pvc.Finalizers = nil
		err = k8sClient.Update(context.Background(), &pvc)
		Expect(err).NotTo(HaveOccurred())

		err = test.reconciler.deleteRestoringPVC(context.Background(), restoreDifferent)
		Expect(err).To(HaveOccurred())

		// cleanup
		err = test.reconciler.deleteRestoringPVC(context.Background(), restore)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should return an error, if the PVC having finalizer", func() {
		err := test.reconciler.createRestoringPVC(context.Background(), restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		err = test.reconciler.deleteRestoringPVC(context.Background(), restore)
		Expect(err).To(HaveOccurred())
	})
}

func (test *mantleRestoreControllerUnitTest) testDeleteRestoringPV() {
	restore := &mantlev1.MantleRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testutil.GetUniqueName("restore-"),
			Namespace: test.tenantNamespace,
			UID:       types.UID(testutil.GetUniqueName("uid-")),
		},
		Spec: mantlev1.MantleRestoreSpec{
			Backup: test.backupName,
		},
	}

	It("should delete the PV", func() {
		var pv corev1.PersistentVolume

		err := test.reconciler.createRestoringPV(context.Background(), restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		// remove pv-protection finalizer from PV to allow deletion
		err = k8sClient.Get(context.Background(), client.ObjectKey{Name: test.reconciler.restoringPVName(restore)}, &pv)
		Expect(err).NotTo(HaveOccurred())
		pv.Finalizers = nil
		err = k8sClient.Update(context.Background(), &pv)
		Expect(err).NotTo(HaveOccurred())

		err = test.reconciler.deleteRestoringPV(context.Background(), restore)
		Expect(err).NotTo(HaveOccurred())

		err = k8sClient.Get(context.Background(), client.ObjectKey{Name: test.reconciler.restoringPVName(restore)}, &pv)
		Expect(err).To(HaveOccurred())
		Expect(errors.IsNotFound(err)).To(BeTrue())
	})

	It("should skip deleting the PV if it does not exist", func() {
		err := test.reconciler.deleteRestoringPV(context.Background(), restore)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should an error, if the PV having different RestoredBy annotation", func() {
		var pv corev1.PersistentVolume
		restoreDifferent := restore.DeepCopy()
		restoreDifferent.UID = types.UID(testutil.GetUniqueName("uid-"))

		err := test.reconciler.createRestoringPV(context.Background(), restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		// remove pv-protection finalizer from PV to allow deletion
		err = k8sClient.Get(context.Background(), client.ObjectKey{Name: test.reconciler.restoringPVName(restore)}, &pv)
		Expect(err).NotTo(HaveOccurred())
		pv.Finalizers = nil
		err = k8sClient.Update(context.Background(), &pv)
		Expect(err).NotTo(HaveOccurred())

		err = test.reconciler.deleteRestoringPV(context.Background(), restoreDifferent)
		Expect(err).To(HaveOccurred())

		// cleanup
		err = test.reconciler.deleteRestoringPV(context.Background(), restore)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should return an error, if the PV having finalizer", func() {
		err := test.reconciler.createRestoringPV(context.Background(), restore, test.backup)
		Expect(err).NotTo(HaveOccurred())

		err = test.reconciler.deleteRestoringPV(context.Background(), restore)
		Expect(err).To(HaveOccurred())
	})
}

// helper function to create a PV and PVC
func (test *mantleRestoreControllerUnitTest) createPVAndPVC(scName, pvName, pvcName, imageName string) (*corev1.PersistentVolume, *corev1.PersistentVolumeClaim) {
	accessModes := []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	size := resource.MustParse("1Gi")
	volumeMode := corev1.PersistentVolumeFilesystem

	pv := corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: accessModes,
			Capacity:    corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("5Gi")},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver: "restore.rbd.csi.ceph.com",
					VolumeAttributes: map[string]string{
						"clusterID":                        test.cephClusterID,
						"csi.storage.k8s.io/pv/name":       pvName,
						"csi.storage.k8s.io/pvc/name":      pvcName,
						"csi.storage.k8s.io/pvc/namespace": test.tenantNamespace,
						"imageFeatures":                    "layering",
						"imageFormat":                      "2",
						"imageName":                        imageName,
						"journalPool":                      test.poolName,
						"pool":                             test.poolName,
						"storage.kubernetes.io/csiProvisionerIdentity": "dummy",
					},
					VolumeHandle: "dummy",
				},
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              scName,
			VolumeMode:                    &volumeMode,
		},
	}
	err := k8sClient.Create(context.Background(), &pv)
	if err != nil {
		panic(err)
	}

	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: test.tenantNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      accessModes,
			StorageClassName: &scName,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: size,
				},
			},
			VolumeMode: &volumeMode,
			VolumeName: pvName,
		},
	}
	err = k8sClient.Create(context.Background(), &pvc)
	if err != nil {
		panic(err)
	}
	pvc.Status.Phase = corev1.ClaimBound
	err = k8sClient.Status().Update(context.Background(), &pvc)
	if err != nil {
		panic(err)
	}

	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: pvcName, Namespace: test.tenantNamespace}, &pvc)
	if err != nil {
		panic(err)
	}

	pv.Spec.ClaimRef = &corev1.ObjectReference{
		Name:      pvcName,
		Namespace: pvc.Namespace,
		UID:       pvc.UID,
	}
	err = k8sClient.Update(context.Background(), &pv)
	if err != nil {
		panic(err)
	}

	pv.Status.Phase = corev1.VolumeBound
	err = k8sClient.Status().Update(context.Background(), &pv)
	if err != nil {
		panic(err)
	}

	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: pvName}, &pv)
	if err != nil {
		panic(err)
	}

	return &pv, &pvc
}

// helper function to create a MantleBackup
func (test *mantleRestoreControllerUnitTest) createBackup(pvc *corev1.PersistentVolumeClaim, backupName string) *mantlev1.MantleBackup {
	backup := &mantlev1.MantleBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupName,
			Namespace: test.tenantNamespace,
		},
		Spec: mantlev1.MantleBackupSpec{
			PVC: pvc.Name,
		},
	}
	err := k8sClient.Create(context.Background(), backup)
	if err != nil {
		panic(err)
	}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: backupName, Namespace: pvc.Namespace}, backup)
	if err != nil {
		panic(err)
	}

	return backup
}
