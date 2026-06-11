package singlek8s

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/cybozu-go/mantle/internal/ceph"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type opsAfterPVCDeletionTest struct {
	poolName         string
	storageClassName string
	tenantNamespace  string

	pvcName    string
	backupName string
}

func opsAfterPVCDeletionTestSuite() {
	test := &opsAfterPVCDeletionTest{
		poolName:         util.GetUniqueName("pool-"),
		storageClassName: util.GetUniqueName("sc-"),
		tenantNamespace:  util.GetUniqueName("ns-"),

		pvcName:    util.GetUniqueName("pvc-"),
		backupName: util.GetUniqueName("backup-"),
	}

	Describe("setup environment", test.setupEnv)
	Describe("test delete after PVC deletion", test.testDeleteAfterPVCDeletion)
	Describe("teardown environment", test.teardownEnv)
}

func (test *opsAfterPVCDeletionTest) setupEnv() {
	It("setting up the test environment", func() {
		_, _ = fmt.Fprintf(GinkgoWriter, "%+v\n", *test)
	})

	It("creating common resources", func() {
		err := createNamespace(test.tenantNamespace)
		Expect(err).NotTo(HaveOccurred())

		err = applyRBDPoolAndSCTemplate(cephCluster1Namespace, test.poolName, test.storageClassName)
		Expect(err).NotTo(HaveOccurred())
	})
}

func (test *opsAfterPVCDeletionTest) teardownEnv() {
	It("cleanup remaining resources", func() {
		_ = deleteNamespacedResource(test.tenantNamespace, "mantlebackup")
		_ = deleteNamespacedResource(test.tenantNamespace, "pvc")
	})

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

func (test *opsAfterPVCDeletionTest) testDeleteAfterPVCDeletion() {
	It("should delete MantleBackup after the source PVC is deleted", func() {
		By("creating PVC")
		err := applyPVCTemplate(test.tenantNamespace, test.pvcName, test.storageClassName)
		Expect(err).NotTo(HaveOccurred())

		By("creating MantleBackup")
		err = applyMantleBackupTemplate(test.tenantNamespace, test.pvcName, test.backupName)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for MantleBackup to be SnapshotCaptured")
		Eventually(func() error {
			captured, err := isMantleBackupSnapshotCaptured(test.tenantNamespace, test.backupName)
			if err != nil {
				return err
			}
			if !captured {
				return errors.New("not yet captured")
			}

			return nil
		}).Should(Succeed())

		By("getting the RBD image name before deleting the PVC")
		pvName, err := getPVFromPVC(test.tenantNamespace, test.pvcName)
		Expect(err).NotTo(HaveOccurred())
		imageName, err := getImageNameFromPVName(pvName)
		Expect(err).NotTo(HaveOccurred())

		By("deleting the source PVC")
		_, _, err = kubectl("-n", test.tenantNamespace, "delete", "pvc", test.pvcName)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for the source PVC to be deleted")
		Eventually(func() error {
			stdout, stderr, err := kubectl("-n", test.tenantNamespace, "get", "pvc", test.pvcName)
			if err != nil {
				if strings.Contains(string(stderr), kubectlIsNotFoundMessage) {
					return nil
				}

				return fmt.Errorf("get pvc %s failed. stderr: %s, err: %w", test.pvcName, string(stderr), err)
			}

			return fmt.Errorf("PVC %s still exists. stdout: %s", test.pvcName, stdout)
		}).Should(Succeed())

		By("deleting MantleBackup")
		_, _, err = kubectl("-n", test.tenantNamespace, "delete", "mantlebackup", test.backupName)
		Expect(err).NotTo(HaveOccurred())

		By("waiting for MantleBackup to be deleted")
		Eventually(func() error {
			stdout, stderr, err := kubectl("-n", test.tenantNamespace, "get", "mantlebackup", test.backupName)
			if err != nil {
				if strings.Contains(string(stderr), kubectlIsNotFoundMessage) {
					return nil
				}

				return fmt.Errorf("get mantlebackup %s failed. stderr: %s, err: %w", test.backupName, string(stderr), err)
			}

			return fmt.Errorf("MantleBackup %s still exists. stdout: %s", test.backupName, stdout)
		}).Should(Succeed())

		By("verifying the RBD image is removed from both active images and trash")
		cephCmd := ceph.NewCephCmdWithTools(cephCluster1Namespace)
		Eventually(func(g Gomega) {
			images, err := cephCmd.RBDLs(test.poolName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(images).NotTo(ContainElement(imageName))

			trashItems, err := cephCmd.RBDTrashLs(test.poolName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(slices.ContainsFunc(trashItems, func(item *ceph.RBDTrashInfo) bool {
				return item.Name == imageName
			})).To(BeFalse())
		}).Should(Succeed())
	})
}
