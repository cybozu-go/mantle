package singlek8s

import (
	"fmt"

	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
		fmt.Fprintf(GinkgoWriter, "%+v\n", *test)
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
		ns := ns
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

	It("create PVC, backup, and restore in the tenant namespace 1", func() {
		test.createPVCBackupRestoreWithData(test.tenantNamespace1, test.storageClassName1, testData1)

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

	It("check the rbd images in the both clusters", func() {
		imageAndSnaps1, err := getImageAndSnapNames(cephCluster1Namespace, test.poolName)
		Expect(err).NotTo(HaveOccurred())
		Expect(imageAndSnaps1).To(ConsistOf(expectImageAndSnaps1))

		imageAndSnaps2, err := getImageAndSnapNames(cephCluster2Namespace, test.poolName)
		Expect(err).NotTo(HaveOccurred())
		Expect(imageAndSnaps2).To(ConsistOf(expectImageAndSnaps1))
	})

	It("create PVC, backup, and restore in the tenant namespace 2", func() {
		test.createPVCBackupRestoreWithData(test.tenantNamespace2, test.storageClassName2, testData2)

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

	It("delete the resources in the tenant namespace 1", func() {
		test.deleteBackupRestore(test.tenantNamespace1)
	})

	It("check the rbd images in the both clusters", func() {
		imageAndSnaps1, err := getImageAndSnapNames(cephCluster1Namespace, test.poolName)
		Expect(err).NotTo(HaveOccurred())
		// after delete backup and restore, only the source image should be left
		Expect(imageAndSnaps1).To(ConsistOf(srcImageName1))

		imageAndSnaps2, err := getImageAndSnapNames(cephCluster2Namespace, test.poolName)
		Expect(err).NotTo(HaveOccurred())
		Expect(imageAndSnaps2).To(ConsistOf(append(expectImageAndSnaps1, expectImageAndSnaps2...)))
	})

	It("delete the resources in the tenant namespace 2", func() {
		test.deleteBackupRestore(test.tenantNamespace2)
	})

	It("check the rbd images in the both clusters", func() {
		imageAndSnaps1, err := getImageAndSnapNames(cephCluster1Namespace, test.poolName)
		Expect(err).NotTo(HaveOccurred())
		Expect(imageAndSnaps1).To(ConsistOf(srcImageName1))

		imageAndSnaps2, err := getImageAndSnapNames(cephCluster2Namespace, test.poolName)
		Expect(err).NotTo(HaveOccurred())
		Expect(imageAndSnaps2).To(ConsistOf(append(expectImageAndSnaps1, srcImageName2)))
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

func (test *multiRookCephTest) createPVCBackupRestoreWithData(ns, sc string, data []byte) {
	err := applyPVCTemplate(ns, test.pvcName, sc)
	Expect(err).NotTo(HaveOccurred())
	err = writeTestData(ns, test.pvcName, data)
	Expect(err).NotTo(HaveOccurred())

	err = applyMantleBackupTemplate(ns, test.pvcName, test.mantleBackupName)
	Expect(err).NotTo(HaveOccurred())

	err = applyMantleRestoreTemplate(ns, test.mantleRestoreName, test.mantleBackupName)
	Expect(err).NotTo(HaveOccurred())

	Eventually(func() bool {
		return isMantleRestoreReady(ns, test.mantleRestoreName)
	}).Should(BeTrue())
}

func (test *multiRookCephTest) deleteBackupRestore(ns string) {
	_, stderr, err := kubectl("delete", "mantlerestore", "-n", ns, test.mantleRestoreName)
	Expect(err).NotTo(HaveOccurred(), string(stderr))
	_, stderr, err = kubectl("delete", "mantlebackup", "-n", ns, test.mantleBackupName)
	Expect(err).NotTo(HaveOccurred(), string(stderr))
}
