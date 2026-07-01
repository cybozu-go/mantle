package singlek8s

import (
	"fmt"
	"strings"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

type multiRookCephTest struct {
	poolName          string // use the same pool name for both clusters
	storageClassName1 string
	storageClassName2 string
	tenantNamespace1  string
	tenantNamespace2  string

	pvcName           string // use the same PVC name for both tenants
	mantleBackupName  string // use the same MantleBackup name for both tenants
	mantleRestoreName string // use the same MantleRestore name for both tenants
}

func multiRookCephTestSuite() {
	test := &multiRookCephTest{
		poolName:          util.GetUniqueName("pool-"),
		storageClassName1: util.GetUniqueName("sc-"),
		storageClassName2: util.GetUniqueName("sc-"),
		tenantNamespace1:  util.GetUniqueName("ns-"),
		tenantNamespace2:  util.GetUniqueName("ns-"),

		pvcName:           util.GetUniqueName("pvc-"),
		mantleBackupName:  util.GetUniqueName("backup-"),
		mantleRestoreName: util.GetUniqueName("restore-"),
	}

	Describe("setup environment", test.setupEnv)
	Describe("test main", test.testMain)
	Describe("tearDown environment", test.teardownEnv)
}

func (test *multiRookCephTest) setupEnv() {
	It("setting up the test environment", func() {
		_, _ = fmt.Fprintf(GinkgoWriter, "%+v\n", *test)
	})

	It("creating common resources", func() {
		for _, ns := range []string{test.tenantNamespace1, test.tenantNamespace2} {
			err := createNamespace(ns)
			Expect(err).NotTo(HaveOccurred())
		}

		err := applyRBDPoolAndSCTemplate(cephCluster1Namespace, test.poolName, test.storageClassName1)
		Expect(err).NotTo(HaveOccurred())

		err = applyRBDPoolAndSCTemplate(cephCluster2Namespace, test.poolName, test.storageClassName2)
		Expect(err).NotTo(HaveOccurred())
	})
}

func (test *multiRookCephTest) teardownEnv() {
	for _, ns := range []string{test.tenantNamespace1, test.tenantNamespace2} {
		It("delete resources in the namespace: "+ns, func() {
			err := deleteNamespacedResource(ns, "mantlerestore")
			Expect(err).NotTo(HaveOccurred())
			err = deleteNamespacedResource(ns, "mantlebackup")
			Expect(err).NotTo(HaveOccurred())
			err = deleteNamespacedResource(ns, "pvc")
			Expect(err).NotTo(HaveOccurred())
		})

		It("delete the namespace: "+ns, func() {
			_, stderr, err := kubectl("delete", "namespace", ns)
			Expect(err).NotTo(HaveOccurred(), string(stderr))
		})
	}

	It("clean up the SCs and RBD pools", func() {
		for _, sc := range []string{test.storageClassName1, test.storageClassName2} {
			_, stderr, err := kubectl("delete", "sc", sc)
			Expect(err).NotTo(HaveOccurred(), string(stderr))
		}

		for _, ns := range []string{cephCluster1Namespace, cephCluster2Namespace} {
			err := removeAllRBDImageAndSnap(ns, test.poolName)
			Expect(err).NotTo(HaveOccurred())

			_, _, err = kubectl("delete", "-n", ns, "cephblockpool", test.poolName, "--wait=false")
			Expect(err).NotTo(HaveOccurred())
		}
	})
}

func (test *multiRookCephTest) testMain() {
	testData1 := []byte("this is a test data for the tenant namespace 1")
	testData2 := []byte("test data 2 should be in the tenant namespace 2")
	var srcImageName1, srcImageName2, srcImageNameDummy string
	var expectImageAndSnaps1, expectImageAndSnaps2 []string
	var dummySnapInfo, dummyCloneInfo *rbdInfo
	var backup1, backup2 *mantlev1.MantleBackup

	// --- Cluster 1 backup lifecycle ---

	It("create PVC and write data in tenant namespace 1", func() {
		err := applyPVCTemplate(test.tenantNamespace1, test.pvcName, test.storageClassName1)
		Expect(err).NotTo(HaveOccurred())
		err = writeTestData(test.tenantNamespace1, test.pvcName, testData1)
		Expect(err).NotTo(HaveOccurred())
	})

	It("create backup in tenant namespace 1 and wait for snapshot", func() {
		err := applyMantleBackupTemplate(test.tenantNamespace1, test.pvcName, test.mantleBackupName)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			ok, err := isMantleBackupSnapshotCaptured(test.tenantNamespace1, test.mantleBackupName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue())
		}).Should(Succeed())

		pvName, err := getPVFromPVC(test.tenantNamespace1, test.pvcName)
		Expect(err).NotTo(HaveOccurred())
		srcImageName1, err = getImageNameFromPVName(pvName)
		Expect(err).NotTo(HaveOccurred())
		expectImageAndSnaps1 = []string{
			srcImageName1,
			fmt.Sprintf("%s@%s", srcImageName1, test.mantleBackupName),
			fmt.Sprintf("mantle-%s-%s", test.tenantNamespace1, test.mantleRestoreName),
		}
	})

	It("check labels on MantleBackup after backup in tenant namespace 1", func() {
		pvc, err := getObject[corev1.PersistentVolumeClaim]("pvc", test.tenantNamespace1, test.pvcName)
		Expect(err).NotTo(HaveOccurred())

		mb, err := getMB(test.tenantNamespace1, test.mantleBackupName)
		Expect(err).NotTo(HaveOccurred())

		mantleLabels := make(map[string]string)
		for k, v := range mb.GetLabels() {
			if strings.HasPrefix(k, "mantle.cybozu.io/") {
				mantleLabels[k] = v
			}
		}
		Expect(mantleLabels).To(Equal(map[string]string{
			"mantle.cybozu.io/cluster-id":                  cephCluster1Namespace,
			"mantle.cybozu.io/local-backup-target-pvc-uid": string(pvc.GetUID()),
		}))
	})

	It("verify for tenant namespace 1 should run only in ceph cluster 1", func() {
		Eventually(func(g Gomega) {
			mb, err := getMB(test.tenantNamespace1, test.mantleBackupName)
			g.Expect(err).NotTo(HaveOccurred())
			// If the ceph cluster 2 controller mistakenly runs verify for this backup,
			// the verify job fails because the RBD snapshot does not exist in ceph cluster 2,
			// which causes IsVerifiedFalse to be set instead of IsVerifiedTrue.
			g.Expect(mb.IsVerifiedFalse()).To(BeFalse(),
				"verify failed: ceph cluster 2 controller may have run verify for ceph cluster 1's backup")
			g.Expect(mb.IsVerifiedTrue()).To(BeTrue())
			backup1 = mb
		}).Should(Succeed())

		jobName := controller.MakeVerifyJobName(backup1)
		pvcName := controller.MakeVerifyPVCName(backup1)

		// verify resources must not appear in the wrong ceph cluster namespace
		exists, err := checkJobExists(cephCluster2Namespace, jobName)
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(BeFalse(), "verify job must not run in ceph cluster 2")

		exists, err = checkPVCExists(cephCluster2Namespace, pvcName)
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(BeFalse(), "verify PVC must not be created in ceph cluster 2")
	})

	It("verify resources for tenant namespace 1 are cleaned up from ceph cluster 1", func() {
		jobName := controller.MakeVerifyJobName(backup1)
		pvcName := controller.MakeVerifyPVCName(backup1)

		Eventually(func(g Gomega) {
			exists, err := checkJobExists(cephCluster1Namespace, jobName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(exists).To(BeFalse(), "verify job should be cleaned up from ceph cluster 1")

			exists, err = checkPVCExists(cephCluster1Namespace, pvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(exists).To(BeFalse(), "verify PVC should be cleaned up from ceph cluster 1")
		}).Should(Succeed())
	})

	It("create restore in tenant namespace 1 and wait for ready", func() {
		err := applyMantleRestoreTemplate(test.tenantNamespace1, test.mantleRestoreName, test.mantleBackupName)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() bool {
			return isMantleRestoreReady(test.tenantNamespace1, test.mantleRestoreName)
		}).Should(BeTrue())
	})

	// --- Dummy data ---
	// Manually create RBD images/snaps/clones in ceph cluster 2 using the same names as
	// those created by ceph cluster 1's controller. This lets the subsequent RBD state
	// checks verify that ceph cluster 1's controller never touches ceph cluster 2's RBD
	// images. No corresponding dummy data is placed in ceph cluster 1 because the test
	// focuses on this one-directional isolation: cluster 1 must not interfere with cluster 2.

	It("create the dummy RBD image & snap in the ceph cluster 2 with the same name as ceph cluster 1", func() {
		By("get source image name from the PV/PVC")
		pvName, err := getPVFromPVC(test.tenantNamespace1, test.pvcName)
		Expect(err).NotTo(HaveOccurred())
		srcImageName, err := getImageNameFromPVName(pvName)
		Expect(err).NotTo(HaveOccurred())
		srcImageNameDummy = srcImageName

		By("create RBD image & snap in the ceph cluster 2")
		err = createRBDImage(cephCluster2Namespace, test.poolName, srcImageNameDummy, 100)
		Expect(err).NotTo(HaveOccurred())
		err = createRBDSnap(cephCluster2Namespace, test.poolName, srcImageNameDummy, test.mantleBackupName)
		Expect(err).NotTo(HaveOccurred())
		snapName := fmt.Sprintf("%s@%s", srcImageNameDummy, test.mantleBackupName)
		cloneName := fmt.Sprintf("mantle-%s-%s", test.tenantNamespace1, test.mantleRestoreName)
		err = createRBDCloneImage(cephCluster2Namespace, test.poolName, snapName, cloneName)
		Expect(err).NotTo(HaveOccurred())

		By("get the dummy infos")
		dummySnapInfo, err = getRBDInfo(cephCluster2Namespace, test.poolName, snapName)
		Expect(err).NotTo(HaveOccurred())
		dummyCloneInfo, err = getRBDInfo(cephCluster2Namespace, test.poolName, cloneName)
		Expect(err).NotTo(HaveOccurred())
	})

	It("check the rbd images in both clusters after cluster 1 backup", func() {
		imageAndSnaps1, err := getImageAndSnapNames(cephCluster1Namespace, test.poolName)
		Expect(err).NotTo(HaveOccurred())
		Expect(imageAndSnaps1).To(ConsistOf(expectImageAndSnaps1))

		imageAndSnaps2, err := getImageAndSnapNames(cephCluster2Namespace, test.poolName)
		Expect(err).NotTo(HaveOccurred())
		Expect(imageAndSnaps2).To(ConsistOf(expectImageAndSnaps1))
	})

	// --- Cluster 2 backup lifecycle ---

	It("create PVC and write data in tenant namespace 2", func() {
		err := applyPVCTemplate(test.tenantNamespace2, test.pvcName, test.storageClassName2)
		Expect(err).NotTo(HaveOccurred())
		err = writeTestData(test.tenantNamespace2, test.pvcName, testData2)
		Expect(err).NotTo(HaveOccurred())
	})

	It("create backup in tenant namespace 2 and wait for snapshot", func() {
		err := applyMantleBackupTemplate(test.tenantNamespace2, test.pvcName, test.mantleBackupName)
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			ok, err := isMantleBackupSnapshotCaptured(test.tenantNamespace2, test.mantleBackupName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ok).To(BeTrue())
		}).Should(Succeed())

		pvName, err := getPVFromPVC(test.tenantNamespace2, test.pvcName)
		Expect(err).NotTo(HaveOccurred())
		srcImageName2, err = getImageNameFromPVName(pvName)
		Expect(err).NotTo(HaveOccurred())
		expectImageAndSnaps2 = []string{
			srcImageName2,
			fmt.Sprintf("%s@%s", srcImageName2, test.mantleBackupName),
			fmt.Sprintf("mantle-%s-%s", test.tenantNamespace2, test.mantleRestoreName),
		}
	})

	It("check labels on MantleBackup after backup in tenant namespace 2", func() {
		pvc, err := getObject[corev1.PersistentVolumeClaim]("pvc", test.tenantNamespace2, test.pvcName)
		Expect(err).NotTo(HaveOccurred())

		mb, err := getMB(test.tenantNamespace2, test.mantleBackupName)
		Expect(err).NotTo(HaveOccurred())

		Expect(mb.GetLabels()).To(HaveKeyWithValue("mantle.cybozu.io/cluster-id", cephCluster2Namespace))
		Expect(mb.GetLabels()).To(HaveKeyWithValue("mantle.cybozu.io/local-backup-target-pvc-uid", string(pvc.GetUID())))
	})

	It("verify for tenant namespace 2 should run only in ceph cluster 2", func() {
		Eventually(func(g Gomega) {
			mb, err := getMB(test.tenantNamespace2, test.mantleBackupName)
			g.Expect(err).NotTo(HaveOccurred())
			// If the ceph cluster 1 controller mistakenly runs verify for this backup,
			// the verify job fails because the RBD snapshot does not exist in ceph cluster 1,
			// which causes IsVerifiedFalse to be set instead of IsVerifiedTrue.
			g.Expect(mb.IsVerifiedFalse()).To(BeFalse(),
				"verify failed: ceph cluster 1 controller may have run verify for ceph cluster 2's backup")
			g.Expect(mb.IsVerifiedTrue()).To(BeTrue())
			backup2 = mb
		}).Should(Succeed())

		jobName := controller.MakeVerifyJobName(backup2)
		pvcName := controller.MakeVerifyPVCName(backup2)

		// verify resources must not appear in the wrong ceph cluster namespace
		exists, err := checkJobExists(cephCluster1Namespace, jobName)
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(BeFalse(), "verify job must not run in ceph cluster 1")

		exists, err = checkPVCExists(cephCluster1Namespace, pvcName)
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(BeFalse(), "verify PVC must not be created in ceph cluster 1")
	})

	It("verify resources for tenant namespace 2 are cleaned up from ceph cluster 2", func() {
		jobName := controller.MakeVerifyJobName(backup2)
		pvcName := controller.MakeVerifyPVCName(backup2)

		Eventually(func(g Gomega) {
			exists, err := checkJobExists(cephCluster2Namespace, jobName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(exists).To(BeFalse(), "verify job should be cleaned up from ceph cluster 2")

			exists, err = checkPVCExists(cephCluster2Namespace, pvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(exists).To(BeFalse(), "verify PVC should be cleaned up from ceph cluster 2")
		}).Should(Succeed())
	})

	It("create restore in tenant namespace 2 and wait for ready", func() {
		err := applyMantleRestoreTemplate(test.tenantNamespace2, test.mantleRestoreName, test.mantleBackupName)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() bool {
			return isMantleRestoreReady(test.tenantNamespace2, test.mantleRestoreName)
		}).Should(BeTrue())
	})

	// --- Cross-cluster RBD state and data integrity checks ---

	It("check the rbd images in the both clusters", func() {
		imageAndSnaps1, err := getImageAndSnapNames(cephCluster1Namespace, test.poolName)
		Expect(err).NotTo(HaveOccurred())
		Expect(imageAndSnaps1).To(ConsistOf(expectImageAndSnaps1))
		backupData1, err := readTestData(test.tenantNamespace1, test.mantleRestoreName)
		Expect(err).NotTo(HaveOccurred())
		Expect(backupData1).To(Equal(testData1))

		imageAndSnaps2, err := getImageAndSnapNames(cephCluster2Namespace, test.poolName)
		Expect(err).NotTo(HaveOccurred())
		Expect(imageAndSnaps2).To(ConsistOf(append(expectImageAndSnaps1, expectImageAndSnaps2...)))
		backupData2, err := readTestData(test.tenantNamespace2, test.mantleRestoreName)
		Expect(err).NotTo(HaveOccurred())
		Expect(backupData2).To(Equal(testData2))
	})

	// --- Deletion isolation checks ---

	It("delete the resources in the tenant namespace 1", func() {
		test.deleteBackupRestore(test.tenantNamespace1)
	})

	It("check the rbd images in the both clusters", func() {
		Eventually(func(g Gomega) {
			imageAndSnaps1, err := getImageAndSnapNames(cephCluster1Namespace, test.poolName)
			g.Expect(err).NotTo(HaveOccurred())
			// after delete backup and restore, only the source image should be left
			g.Expect(imageAndSnaps1).To(ConsistOf(srcImageName1))

			imageAndSnaps2, err := getImageAndSnapNames(cephCluster2Namespace, test.poolName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(imageAndSnaps2).To(ConsistOf(append(expectImageAndSnaps1, expectImageAndSnaps2...)))
		}).Should(Succeed())
	})

	It("delete the resources in the tenant namespace 2", func() {
		test.deleteBackupRestore(test.tenantNamespace2)
	})

	It("check the rbd images in the both clusters", func() {
		Eventually(func(g Gomega) {
			imageAndSnaps1, err := getImageAndSnapNames(cephCluster1Namespace, test.poolName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(imageAndSnaps1).To(ConsistOf(srcImageName1))

			imageAndSnaps2, err := getImageAndSnapNames(cephCluster2Namespace, test.poolName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(imageAndSnaps2).To(ConsistOf(append(expectImageAndSnaps1, srcImageName2)))
		}).Should(Succeed())
	})

	It("check there is no change in the dummy rbd image", func() {
		snapName := fmt.Sprintf("%s@%s", srcImageNameDummy, test.mantleBackupName)
		cloneName := fmt.Sprintf("mantle-%s-%s", test.tenantNamespace1, test.mantleRestoreName)

		info, err := getRBDInfo(cephCluster2Namespace, test.poolName, snapName)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.CreateTimestamp).To(Equal(dummySnapInfo.CreateTimestamp))
		Expect(info.ModifyTimestamp).To(Equal(dummySnapInfo.ModifyTimestamp))

		info, err = getRBDInfo(cephCluster2Namespace, test.poolName, cloneName)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.CreateTimestamp).To(Equal(dummyCloneInfo.CreateTimestamp))
		Expect(info.ModifyTimestamp).To(Equal(dummyCloneInfo.ModifyTimestamp))
	})

	It("clean up the dummy data", func() {
		err := removeAllRBDImageAndSnap(cephCluster2Namespace, test.poolName)
		Expect(err).NotTo(HaveOccurred())
	})
}

func (test *multiRookCephTest) deleteBackupRestore(ns string) {
	_, stderr, err := kubectl("delete", "mantlerestore", "-n", ns, test.mantleRestoreName)
	Expect(err).NotTo(HaveOccurred(), string(stderr))
	_, stderr, err = kubectl("delete", "mantlebackup", "-n", ns, test.mantleBackupName)
	Expect(err).NotTo(HaveOccurred(), string(stderr))
}
