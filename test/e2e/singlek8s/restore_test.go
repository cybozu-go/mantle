package singlek8s

import (
	"encoding/json"
	"fmt"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type restoreTest struct {
	poolName         string
	storageClassName string
	tenantNamespace  string

	pvcName            string
	mantleBackupName1  string
	mantleBackupName2  string
	mantleRestoreName1 string
	mantleRestoreName2 string
}

func restoreTestSuite() {
	test := &restoreTest{
		poolName:         util.GetUniqueName("pool-"),
		storageClassName: util.GetUniqueName("sc-"),
		tenantNamespace:  util.GetUniqueName("ns-"),

		pvcName:            util.GetUniqueName("pvc-"),
		mantleBackupName1:  util.GetUniqueName("backup-"),
		mantleBackupName2:  util.GetUniqueName("backup-"),
		mantleRestoreName1: util.GetUniqueName("restore-"),
		mantleRestoreName2: util.GetUniqueName("restore-"),
	}

	Describe("setup environment", test.setupEnv)
	Describe("test restore", test.testRestore)
	Describe("test cleanup", test.testCleanup)
	Describe("test restore with multiple backups", test.testRestoreWithMultipleBackups)
	Describe("test cloneImageFromBackup", test.testCloneImageFromBackup)
	Describe("test removeImage", test.testRemoveImage)
	Describe("tearDown environment", test.tearDownEnv)
}

func (test *restoreTest) setupEnv() {
	It("describing the test environment", func() {
		_, _ = fmt.Fprintf(GinkgoWriter, "%+v\n", *test)
	})

	It("creating common resources", func() {
		// namespace
		err := createNamespace(test.tenantNamespace)
		Expect(err).NotTo(HaveOccurred())

		// pool & storage class
		err = applyRBDPoolAndSCTemplate(cephCluster1Namespace, test.poolName, test.storageClassName)
		Expect(err).NotTo(HaveOccurred())
	})

	It("waiting for mantle-controller to get ready", func() {
		Eventually(func() error {
			return checkDeploymentReady(cephCluster1Namespace, "mantle-controller")
		}).Should(Succeed())
	})
}

func (test *restoreTest) tearDownEnv() {
	test.cleanup()

	It("delete namespace: "+test.tenantNamespace, func() {
		_, stderr, err := kubectl("delete", "namespace", test.tenantNamespace)
		Expect(err).NotTo(HaveOccurred(), string(stderr))
	})

	It("clean up the SCs and RBD pools", func() {
		_, stderr, err := kubectl("delete", "sc", test.storageClassName)
		Expect(err).NotTo(HaveOccurred(), string(stderr))

		err = removeAllRBDImageAndSnap(cephCluster1Namespace, test.poolName)
		Expect(err).NotTo(HaveOccurred())

		_, _, err = kubectl("delete", "-n", cephCluster1Namespace, "cephblockpool", test.poolName, "--wait=false")
		Expect(err).NotTo(HaveOccurred())
	})
}

func (test *restoreTest) testRestore() {
	testData1 := []byte("test data 1")
	testData2 := []byte("test data 2")

	It("should wait for the MantleBackup to be created", func() {
		test.cleanup()

		err := applyPVCTemplate(test.tenantNamespace, test.pvcName, test.storageClassName)
		Expect(err).NotTo(HaveOccurred())

		err = applyMantleRestoreTemplate(test.tenantNamespace, test.mantleRestoreName1, test.mantleBackupName1)
		Expect(err).NotTo(HaveOccurred())

		Consistently(func() bool {
			return isMantleRestoreReady(test.tenantNamespace, test.mantleRestoreName1)
		}, 10*time.Second).Should(BeFalse())

		By("creating the MantleBackup after the MantleRestore")
		err = applyMantleBackupTemplate(test.tenantNamespace, test.pvcName, test.mantleBackupName1)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() bool {
			return isMantleRestoreReady(test.tenantNamespace, test.mantleRestoreName1)
		}).Should(BeTrue())
	})

	It("should wait for the MantleBackup to be ReadyToUse", func() {
		test.cleanup()

		err := applyMantleBackupTemplate(test.tenantNamespace, test.pvcName, test.mantleBackupName1)
		Expect(err).NotTo(HaveOccurred())

		err = applyMantleRestoreTemplate(test.tenantNamespace, test.mantleRestoreName1, test.mantleBackupName1)
		Expect(err).NotTo(HaveOccurred())

		Consistently(func() bool {
			return isMantleRestoreReady(test.tenantNamespace, test.mantleRestoreName1)
		}, 10*time.Second).Should(BeFalse())

		By("creating the PVC after the MantleRestore")
		err = applyPVCTemplate(test.tenantNamespace, test.pvcName, test.storageClassName)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() bool {
			return isMantleRestoreReady(test.tenantNamespace, test.mantleRestoreName1)
		}).Should(BeTrue())
	})

	It("should reconcile the restore", func() {
		cloneImageName := fmt.Sprintf("mantle-%s-%s", test.tenantNamespace, test.mantleRestoreName1)
		test.cleanup()

		By("creating new PV/PVC with test data")
		err := applyPVCTemplate(test.tenantNamespace, test.pvcName, test.storageClassName)
		Expect(err).NotTo(HaveOccurred())
		err = writeTestData(test.tenantNamespace, test.pvcName, testData1)
		Expect(err).NotTo(HaveOccurred())

		err = applyMantleBackupTemplate(test.tenantNamespace, test.pvcName, test.mantleBackupName1)
		Expect(err).NotTo(HaveOccurred())
		err = applyMantleRestoreTemplate(test.tenantNamespace, test.mantleRestoreName1, test.mantleBackupName1)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() bool {
			return isMantleRestoreReady(test.tenantNamespace, test.mantleRestoreName1)
		}).Should(BeTrue())

		var restore mantlev1.MantleRestore
		bin, _, err := kubectl("get", "mantlerestore", "-n", test.tenantNamespace, test.mantleRestoreName1, "-o", "json")
		Expect(err).NotTo(HaveOccurred())
		err = json.Unmarshal(bin, &restore)
		Expect(err).NotTo(HaveOccurred())

		By("checking if the finalizer is added to the MantleRestore")
		Expect(controllerutil.ContainsFinalizer(&restore, controller.MantleRestoreFinalizerName)).To(BeTrue())

		By("checking the clone image")
		info, err := getRBDInfo(cephCluster1Namespace, test.poolName, cloneImageName)
		Expect(err).NotTo(HaveOccurred())
		pvName, err := getPVFromPVC(test.tenantNamespace, test.pvcName)
		Expect(err).NotTo(HaveOccurred())
		imageName, err := getImageNameFromPVName(pvName)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Parent.Image).To(Equal(imageName))
		Expect(info.Parent.Pool).To(Equal(test.poolName))
		Expect(info.Parent.Snapshot).To(Equal(test.mantleBackupName1))

		data, err := readTestData(test.tenantNamespace, test.mantleRestoreName1)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal(testData1))

		// silence lint & will be delete after implement multi-cluster test
		_, err = getRBDInfo(cephCluster2Namespace, test.poolName, cloneImageName)
		Expect(err).To(HaveOccurred())

		By("checking if the PV is created")
		_, _, err = kubectl("get", "pv", fmt.Sprintf("mr-%s-%s", test.tenantNamespace, test.mantleRestoreName1), "-o", "json")
		Expect(err).NotTo(HaveOccurred())

		By("checking if the PVC is created")
		_, _, err = kubectl("get", "pvc", "-n", test.tenantNamespace, test.pvcName, "-o", "json")
		Expect(err).NotTo(HaveOccurred())

		By("checking if the status is updated")
		Expect(restore.IsReady()).To(BeTrue())
	})

	It("should not update clone image if it is already created", func() {
		By("overwriting the PVC with new test data")
		err := writeTestData(test.tenantNamespace, test.pvcName, testData2)
		Expect(err).NotTo(HaveOccurred())

		Consistently(func() []byte {
			data, err := readTestData(test.tenantNamespace, test.mantleRestoreName1)
			Expect(err).NotTo(HaveOccurred())
			return data
		}, 30*time.Second).Should(Equal(testData1))
	})

	It("should have the correct images when create 2 MantleRestore", func() {
		err := applyMantleRestoreTemplate(test.tenantNamespace, test.mantleRestoreName2, test.mantleBackupName1)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() bool {
			return isMantleRestoreReady(test.tenantNamespace, test.mantleRestoreName2)
		}).Should(BeTrue())

		data, err := readTestData(test.tenantNamespace, test.mantleRestoreName2)
		Expect(err).NotTo(HaveOccurred())
		Expect(data).To(Equal(testData1))
	})
}

func (test *restoreTest) testCleanup() {
	It("should reconcile the restore", func() {
		test.cleanup()
		imageName := fmt.Sprintf("mantle-%s-%s", test.tenantNamespace, test.mantleRestoreName1)

		err := applyPVCTemplate(test.tenantNamespace, test.pvcName, test.storageClassName)
		Expect(err).NotTo(HaveOccurred())
		err = applyMantleBackupTemplate(test.tenantNamespace, test.pvcName, test.mantleBackupName1)
		Expect(err).NotTo(HaveOccurred())
		err = applyMantleRestoreTemplate(test.tenantNamespace, test.mantleRestoreName1, test.mantleBackupName1)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() bool {
			return isMantleRestoreReady(test.tenantNamespace, test.mantleRestoreName1)
		}).Should(BeTrue())

		By("checking resources are exist before delete")
		_, _, err = kubectl("get", "pv", fmt.Sprintf("mr-%s-%s", test.tenantNamespace, test.mantleRestoreName1))
		Expect(err).NotTo(HaveOccurred())
		_, _, err = kubectl("get", "pvc", "-n", test.tenantNamespace, test.mantleRestoreName1)
		Expect(err).NotTo(HaveOccurred())
		_, err = getRBDInfo(cephCluster1Namespace, test.poolName, imageName)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the MantleRestore")
		_, _, err = kubectl("delete", "mantlerestore", "-n", test.tenantNamespace, test.mantleRestoreName1)
		Expect(err).NotTo(HaveOccurred())

		By("checking if the MantleRestore is deleted")
		_, _, err = kubectl("get", "mantlerestore", "-n", test.tenantNamespace, test.mantleRestoreName1)
		Expect(err).To(HaveOccurred())

		Eventually(func(g Gomega) {
			By("checking if the PVC is deleted")
			_, _, err = kubectl("get", "pvc", "-n", test.tenantNamespace, test.mantleRestoreName1)
			g.Expect(err).To(HaveOccurred())

			By("checking if the PV is deleted")
			_, _, err = kubectl("get", "pv", fmt.Sprintf("mr-%s-%s", test.tenantNamespace, test.mantleRestoreName1))
			g.Expect(err).To(HaveOccurred())

			By("checking if the clone image is deleted")
			_, err = getRBDInfo(cephCluster1Namespace, test.poolName, imageName)
			g.Expect(err).To(HaveOccurred())
		}).Should(Succeed())
	})

	It("should NOT delete an RBD image while its corresponding PV and PVC are in use", func() {
		test.cleanup()
		imageName := fmt.Sprintf("mantle-%s-%s", test.tenantNamespace, test.mantleRestoreName1)
		podName := util.GetUniqueName("pod-")
		pvName := fmt.Sprintf("mr-%s-%s", test.tenantNamespace, test.mantleRestoreName1)

		err := applyPVCTemplate(test.tenantNamespace, test.pvcName, test.storageClassName)
		Expect(err).NotTo(HaveOccurred())
		err = applyMantleBackupTemplate(test.tenantNamespace, test.pvcName, test.mantleBackupName1)
		Expect(err).NotTo(HaveOccurred())
		err = applyMantleRestoreTemplate(test.tenantNamespace, test.mantleRestoreName1, test.mantleBackupName1)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() bool {
			return isMantleRestoreReady(test.tenantNamespace, test.mantleRestoreName1)
		}).Should(BeTrue())

		By("creating a pod with the restored PVC")
		err = applyPodMountVolumeTemplate(test.tenantNamespace, podName, test.mantleRestoreName1)
		Expect(err).NotTo(HaveOccurred())
		_, _, err = kubectl("wait", "--for=condition=Ready", "pod", podName, "-n", test.tenantNamespace, "--timeout=1m")
		Expect(err).NotTo(HaveOccurred())

		By("deleting the MantleRestore")
		_, _, err = kubectl("delete", "mantlerestore", "-n", test.tenantNamespace, test.mantleRestoreName1)
		Expect(err).NotTo(HaveOccurred())
		By("checking the MantleRestore is deleted")
		Eventually(func(g Gomega) {
			_, _, err = kubectl("get", "mantlerestore", "-n", test.tenantNamespace, test.mantleRestoreName1)
			g.Expect(err).To(HaveOccurred())
		}).Should(Succeed())

		By("checking the PV and RBD image exists while the PVC is in use")
		Consistently(func(g Gomega) error {
			_, _, err := kubectl("get", "pv", pvName)
			g.Expect(err).NotTo(HaveOccurred())

			_, err = getRBDInfo(cephCluster1Namespace, test.poolName, imageName)
			return err
		}, 30*time.Second).Should(BeNil())

		By("deleting the pod")
		_, _, err = kubectl("delete", "pod", "-n", test.tenantNamespace, podName)
		Expect(err).NotTo(HaveOccurred())

		By("checking the PV and RBD image is deleted")
		Eventually(func(g Gomega) {
			_, _, err := kubectl("get", "pv", pvName)
			g.Expect(err).To(HaveOccurred())

			_, err = getRBDInfo(cephCluster1Namespace, test.poolName, imageName)
			g.Expect(err).To(HaveOccurred())
		}).Should(Succeed())
	})
}

func (test *restoreTest) testRestoreWithMultipleBackups() {
	testData1 := []byte("test data 1")
	testData2 := []byte("test data 2")

	It("should have the correct images when create 2 MantleRestore", func() {
		test.cleanup()

		By("creating new PV/PVC with test data")
		err := applyPVCTemplate(test.tenantNamespace, test.pvcName, test.storageClassName)
		Expect(err).NotTo(HaveOccurred())
		err = writeTestData(test.tenantNamespace, test.pvcName, testData1)
		Expect(err).NotTo(HaveOccurred())

		By("creating the first MantleBackup")
		err = applyMantleBackupTemplate(test.tenantNamespace, test.pvcName, test.mantleBackupName1)
		Expect(err).NotTo(HaveOccurred())

		By("overwriting test data")
		err = writeTestData(test.tenantNamespace, test.pvcName, testData2)
		Expect(err).NotTo(HaveOccurred())

		By("creating the second MantleBackup")
		err = applyMantleBackupTemplate(test.tenantNamespace, test.pvcName, test.mantleBackupName2)
		Expect(err).NotTo(HaveOccurred())

		By("creating MantleRestores")
		err = applyMantleRestoreTemplate(test.tenantNamespace, test.mantleRestoreName1, test.mantleBackupName1)
		Expect(err).NotTo(HaveOccurred())
		err = applyMantleRestoreTemplate(test.tenantNamespace, test.mantleRestoreName2, test.mantleBackupName2)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() bool {
			return isMantleRestoreReady(test.tenantNamespace, test.mantleRestoreName1) &&
				isMantleRestoreReady(test.tenantNamespace, test.mantleRestoreName2)
		}).Should(BeTrue())

		By("checking the first restore")
		data1, err := readTestData(test.tenantNamespace, test.mantleRestoreName1)
		Expect(err).NotTo(HaveOccurred())
		Expect(data1).To(Equal(testData1))

		By("checking the second restore")
		data2, err := readTestData(test.tenantNamespace, test.mantleRestoreName2)
		Expect(err).NotTo(HaveOccurred())
		Expect(data2).To(Equal(testData2))
	})

	It("should delete the second restore, and the first restore PV/PVC and image should still exist", func() {
		imageName1 := fmt.Sprintf("mantle-%s-%s", test.tenantNamespace, test.mantleRestoreName1)
		imageName2 := fmt.Sprintf("mantle-%s-%s", test.tenantNamespace, test.mantleRestoreName2)

		By("deleting the second restore")
		_, _, err := kubectl("delete", "mantlerestore", "-n", test.tenantNamespace, test.mantleRestoreName2)
		Expect(err).NotTo(HaveOccurred())

		By("checking the second restore is deleted")
		Eventually(func(g Gomega) {
			_, err = getRBDInfo(cephCluster1Namespace, test.poolName, imageName2)
			g.Expect(err).To(HaveOccurred())
		}).Should(Succeed())

		By("checking the first restore PV/PVC and image should still exist")
		_, _, err = kubectl("get", "pv", fmt.Sprintf("mr-%s-%s", test.tenantNamespace, test.mantleRestoreName1))
		Expect(err).NotTo(HaveOccurred())
		_, _, err = kubectl("get", "pvc", "-n", test.tenantNamespace, test.mantleRestoreName1)
		Expect(err).NotTo(HaveOccurred())
		_, err = getRBDInfo(cephCluster1Namespace, test.poolName, imageName1)
		Expect(err).NotTo(HaveOccurred())
	})
}

func (test *restoreTest) testCloneImageFromBackup() {
	cloneImageName := fmt.Sprintf("mantle-%s-%s", test.tenantNamespace, test.mantleRestoreName1)
	reconciler := controller.NewMantleRestoreReconcilerE2E(cephCluster1Namespace, cephCluster1Namespace)
	backup := &mantlev1.MantleBackup{}
	restore := &mantlev1.MantleRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      test.mantleRestoreName1,
			Namespace: test.tenantNamespace,
		},
		Spec: mantlev1.MantleRestoreSpec{
			Backup: test.mantleBackupName1,
		},
		Status: mantlev1.MantleRestoreStatus{
			ClusterID: cephCluster1Namespace,
			Pool:      test.poolName,
		},
	}
	var info *rbdInfo

	It("should create a clone of the backup image", func(ctx SpecContext) {
		test.cleanup()

		By("creating a PVC and a MantleBackup")
		err := applyPVCTemplate(test.tenantNamespace, test.pvcName, test.storageClassName)
		Expect(err).NotTo(HaveOccurred())
		err = applyMantleBackupTemplate(test.tenantNamespace, test.pvcName, test.mantleBackupName1)
		Expect(err).NotTo(HaveOccurred())

		By("getting the backup manifest")
		Eventually(func() error {
			bin, _, err := kubectl("get", "mantlebackup", "-n", test.tenantNamespace, test.mantleBackupName1, "-o", "json")
			if err != nil {
				return err
			}

			err = json.Unmarshal(bin, &backup)
			if err != nil {
				return err
			}

			if !backup.IsReady() {
				return fmt.Errorf("backup is not ready")
			}
			return nil
		}).Should(Succeed())

		By("creating a clone image from the backup")
		err = reconciler.CloneImageFromBackup(ctx, restore, backup)
		Expect(err).NotTo(HaveOccurred())

		info, err = getRBDInfo(cephCluster1Namespace, test.poolName, cloneImageName)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should skip creating a clone if it already exists", func(ctx SpecContext) {
		err := reconciler.CloneImageFromBackup(ctx, restore, backup)
		Expect(err).NotTo(HaveOccurred())

		info2, err := getRBDInfo(cephCluster1Namespace, test.poolName, cloneImageName)
		Expect(err).NotTo(HaveOccurred())

		Expect(info2.CreateTimestamp).To(Equal(info.CreateTimestamp))
		Expect(info2.ModifyTimestamp).To(Equal(info.ModifyTimestamp))
	})

	It("should return an error, if the PV manifest is broken", func(ctx SpecContext) {
		b := backup.DeepCopy()
		b.Status.PVManifest = "broken"

		err := reconciler.CloneImageFromBackup(ctx, restore, b)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("failed to unmarshal PV manifest"))
	})

	It("should return an error, if imageName is not found", func(ctx SpecContext) {
		b := backup.DeepCopy()
		var pv corev1.PersistentVolume
		err := json.Unmarshal([]byte(backup.Status.PVManifest), &pv)
		Expect(err).NotTo(HaveOccurred())
		// delete the imageName field
		delete(pv.Spec.CSI.VolumeAttributes, "imageName")
		bin, err := json.Marshal(pv)
		Expect(err).NotTo(HaveOccurred())
		b.Status.PVManifest = string(bin)

		err = reconciler.CloneImageFromBackup(ctx, restore, b)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("imageName not found in PV manifest"))
	})

	It("should return an error, if the image already exists but is not a clone of the backup image",
		func(ctx SpecContext) {
			b := backup.DeepCopy()
			var pv corev1.PersistentVolume
			err := json.Unmarshal([]byte(backup.Status.PVManifest), &pv)
			Expect(err).NotTo(HaveOccurred())
			// override the imageName field
			pv.Spec.CSI.VolumeAttributes["imageName"] = "different-image-name"
			bin, err := json.Marshal(pv)
			Expect(err).NotTo(HaveOccurred())
			b.Status.PVManifest = string(bin)

			err = reconciler.CloneImageFromBackup(ctx, restore, b)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("image already exists but not a clone of the backup"))
		})
}

func (test *restoreTest) testRemoveImage() {
	cloneImageName := fmt.Sprintf("mantle-%s-%s", test.tenantNamespace, test.mantleRestoreName1)
	pvReconciler := controller.NewPersistentVolumeReconcilerE2E(cephCluster1Namespace)
	pv := &corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					VolumeHandle: cloneImageName,
					VolumeAttributes: map[string]string{
						"pool": test.poolName,
					},
				},
			},
		},
	}

	Describe("removeImage", func() {
		It("should remove the image", func(ctx SpecContext) {
			_, err := getRBDInfo(cephCluster1Namespace, test.poolName, cloneImageName)
			Expect(err).NotTo(HaveOccurred())

			err = pvReconciler.RemoveRBDImage(ctx, pv)
			Expect(err).NotTo(HaveOccurred())

			// should get an error since the image is removed
			_, err = getRBDInfo(cephCluster1Namespace, test.poolName, cloneImageName)
			Expect(err).To(HaveOccurred())
		})

		It("should skip removing the image if it does not exist", func(ctx SpecContext) {
			err := pvReconciler.RemoveRBDImage(ctx, pv)
			Expect(err).NotTo(HaveOccurred())
		})
	})
}

func (test *restoreTest) cleanup() {
	err := deleteNamespacedResource(test.tenantNamespace, "mantlerestore")
	Expect(err).NotTo(HaveOccurred())
	err = deleteNamespacedResource(test.tenantNamespace, "mantlebackup")
	Expect(err).NotTo(HaveOccurred())
	err = deleteNamespacedResource(test.tenantNamespace, "pvc")
	Expect(err).NotTo(HaveOccurred())
}
