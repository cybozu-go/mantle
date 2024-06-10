package e2e

import (
	"encoding/json"
	"fmt"
	"strings"

	backupv1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

const (
	kubectlIsNotFoundMessage = "Error from server (NotFound):"
)

type backupTest struct {
	poolName          string
	storageClassName1 string
	storageClassName2 string
	tenantNamespace1  string
	tenantNamespace2  string

	pvcName1          string
	pvcName2          string
	mantleBackupName1 string
	mantleBackupName2 string
	mantleBackupName3 string
}

func backupTestSuite() {
	test := &backupTest{
		poolName:          util.GetUniqueName("pool-"),
		storageClassName1: util.GetUniqueName("sc-"),
		storageClassName2: util.GetUniqueName("sc-"),
		tenantNamespace1:  util.GetUniqueName("ns-"),
		tenantNamespace2:  util.GetUniqueName("ns-"),

		pvcName1:          "rbd-pvc1",
		pvcName2:          "rbd-pvc2",
		mantleBackupName1: "mantlebackup-test1",
		mantleBackupName2: "mantlebackup-test2",
		mantleBackupName3: "mantlebackup-test3",
	}

	Describe("setup environment", test.setupEnv)
	Describe("test case 1", test.testCase1)
	Describe("teardown environment", test.teardownEnv)
}

func (test *backupTest) setupEnv() {
	It("creating common resources", func() {
		for _, ns := range []string{test.tenantNamespace1, test.tenantNamespace2} {
			err := createNamespace(ns)
			Expect(err).NotTo(HaveOccurred())
		}

		err := applyRBDPoolAndSCTemplate(cephCluster1Namespace, test.poolName, test.storageClassName1)
		Expect(err).NotTo(HaveOccurred())

		err = applyRBDPoolAndSCTemplate(cephCluster2Namespace, test.poolName, test.storageClassName2)
		Expect(err).NotTo(HaveOccurred())

		for _, name := range []string{test.pvcName1, test.pvcName2} {
			By(fmt.Sprintf("Creating PVC, PV and RBD image (%s)", name))
			err = applyPVCTemplate(test.tenantNamespace1, name, test.storageClassName1)
			Expect(err).NotTo(HaveOccurred())

			pvName, err := getPVFromPVC(test.tenantNamespace1, name)
			Expect(err).NotTo(HaveOccurred())

			imageName, err := getImageNameFromPVName(pvName)
			Expect(err).NotTo(HaveOccurred())

			// Create a new RBD image in cephNamespace2 with the same name as imageName.
			Eventually(func() error {
				return createRBDImage(cephCluster2Namespace, test.poolName, imageName, 100)
			}).Should(Succeed())
		}
	})

	It("waiting for mantle-controller to get ready", func() {
		Eventually(func() error {
			return checkDeploymentReady(cephCluster1Namespace, "mantle-controller")
		}).Should(Succeed())
	})
}

func (test *backupTest) teardownEnv() {
	It("deleting MantleBackups", func() {
		for _, mantleBackup := range []string{test.mantleBackupName1, test.mantleBackupName2, test.mantleBackupName3} {
			_, _, _ = kubectl("delete", "-n", test.tenantNamespace1, "mantlebackup", mantleBackup)
		}
	})

	It("deleting PVCs", func() {
		for _, pvc := range []string{test.pvcName1, test.pvcName2} {
			_, _, _ = kubectl("delete", "-n", test.tenantNamespace1, "pvc", pvc)
		}

		By("Deleting RBD images in " + cephCluster2Namespace)
		stdout, _, err := kubectl("exec", "-n", cephCluster2Namespace, "deploy/rook-ceph-tools", "--",
			"rbd", "ls", test.poolName, "--format=json")
		if err == nil {
			imageNames := []string{}
			if err := json.Unmarshal(stdout, &imageNames); err == nil {
				for _, imageName := range imageNames {
					_, _, _ = kubectl("exec", "-n", cephCluster2Namespace, "deploy/rook-ceph-tools", "--",
						"rbd", "rm", test.poolName+"/"+imageName)
				}
			}
		}
	})

	It("deleting common resources", func() {
		_, _, _ = kubectl("delete", "sc", test.storageClassName1, "--wait=false")
		_, _, _ = kubectl("delete", "sc", test.storageClassName2, "--wait=false")
		_, _, _ = kubectl("delete", "-n", cephCluster1Namespace, "cephblockpool", test.poolName, "--wait=false")
		_, _, _ = kubectl("delete", "-n", cephCluster2Namespace, "cephblockpool", test.poolName, "--wait=false")
	})
}

func (test *backupTest) testCase1() {
	var firstImageName string

	createMantleBackupAndGetImage := func(mantleBackupName string) string {
		By("Creating MantleBackup")
		err := applyMantleBackupTemplate(test.tenantNamespace1, test.pvcName1, mantleBackupName)
		Expect(err).NotTo(HaveOccurred())

		pvName, err := getPVFromPVC(test.tenantNamespace1, test.pvcName1)
		Expect(err).NotTo(HaveOccurred())

		By("Waiting for RBD snapshot to be created")
		imageName := ""
		Eventually(func() error {
			imageName, err = getImageNameFromPVName(pvName)
			if err != nil {
				return err
			}

			return checkSnapshotExist(cephCluster1Namespace, test.poolName, imageName, mantleBackupName)
		}).Should(Succeed())

		By("Checking that the mantle-controller deployed for a certain Rook/Ceph cluster (i.e., " +
			cephCluster1Namespace + ") doesn't create a snapshot for a MantleBackup for a different Rook/Ceph cluster (i.e., " +
			cephCluster2Namespace + ")")
		Consistently(func() error {
			err := checkSnapshotExist(cephCluster2Namespace, test.poolName, imageName, mantleBackupName)
			if err == nil {
				return fmt.Errorf("a wrong snapshot exists. snapshotName: %s", mantleBackupName)
			}
			return nil
		}).Should(Succeed())

		return imageName
	}

	It("should create MantleBackup resource", func() {
		firstImageName = createMantleBackupAndGetImage(test.mantleBackupName1)
	})

	It("should create multiple MantleBackup resources for the same PVC", func() {
		createMantleBackupAndGetImage(test.mantleBackupName2)
	})

	It("should create MantleBackups resources for different PVCs", func() {
		By("Creating a third MantleBackup for the other PVC")
		err := applyMantleBackupTemplate(test.tenantNamespace1, test.pvcName2, test.mantleBackupName3)
		Expect(err).NotTo(HaveOccurred())

		pvName, err := getPVFromPVC(test.tenantNamespace1, test.pvcName2)
		Expect(err).NotTo(HaveOccurred())

		By("Waiting for RBD snapshot to be created")
		Eventually(func() error {
			imageName, err := getImageNameFromPVName(pvName)
			if err != nil {
				return err
			}

			return checkSnapshotExist(cephCluster1Namespace, test.poolName, imageName, test.mantleBackupName3)
		}).Should(Succeed())
	})

	It("should not delete MantleBackup resource when delete backup target PVC", func() {
		By("Deleting backup target PVC")
		_, _, err := kubectl("-n", test.tenantNamespace1, "delete", "pvc", test.pvcName2)
		Expect(err).NotTo(HaveOccurred())

		By("Checking backup target PVC deletion")
		Eventually(func() error {
			stdout, stderr, err := kubectl("-n", test.tenantNamespace1, "get", "pvc", test.pvcName2)
			if err != nil {
				if strings.Contains(string(stderr), kubectlIsNotFoundMessage) {
					return nil
				}
				return fmt.Errorf("get pvc %s failed. stderr: %s, err: %w", test.pvcName2, string(stderr), err)
			}
			return fmt.Errorf("PVC %s still exists. stdout: %s", test.pvcName2, stdout)
		}).Should(Succeed())

		By("Checking that the status.conditions of the MantleBackup resource remain \"Bound\"")
		stdout, _, err := kubectl("-n", test.tenantNamespace1, "get", "mantlebackup", test.mantleBackupName3, "-o", "json")
		Expect(err).NotTo(HaveOccurred())
		var backup backupv1.MantleBackup
		err = yaml.Unmarshal(stdout, &backup)
		Expect(err).NotTo(HaveOccurred())
		Expect(meta.FindStatusCondition(backup.Status.Conditions, backupv1.BackupConditionReadyToUse).Status).
			To(Equal(metav1.ConditionTrue))
	})

	It("should delete MantleBackup resource", func() {
		By("Delete MantleBackup")
		_, _, err := kubectl("-n", test.tenantNamespace1, "delete", "mantlebackup", test.mantleBackupName1, "--wait=false")
		Expect(err).NotTo(HaveOccurred())

		By("Waiting for RBD snapshot to be deleted")
		Eventually(func() error {
			err := checkSnapshotExist(cephCluster1Namespace, test.poolName, firstImageName, test.mantleBackupName1)
			if err == nil {
				return fmt.Errorf("snapshot exists. snapshotName: %s", test.mantleBackupName1)
			}

			return nil
		}).Should(Succeed())

		By("Checking MantleBackup resource deletion")
		Eventually(func() error {
			stdout, stderr, err := kubectl("-n", test.tenantNamespace1, "get", "mantlebackup", test.mantleBackupName1)
			if err != nil {
				if strings.Contains(string(stderr), kubectlIsNotFoundMessage) {
					return nil
				}
				return fmt.Errorf("get mantlebackup %s failed. stderr: %s, err: %w", test.mantleBackupName1, string(stderr), err)
			}
			return fmt.Errorf("MantleBackup resource %s still exists. stdout: %s", test.mantleBackupName1, stdout)
		}).Should(Succeed())
	})

	It("should delete MantleBackup resource when backup target PVC is missing", func() {
		By("Deleting MantleBackup resource")
		_, _, err := kubectl("-n", test.tenantNamespace1, "delete", "mantlebackup", test.mantleBackupName3)
		Expect(err).NotTo(HaveOccurred())

		By("Checking MantleBackup resource deletion")
		Eventually(func() error {
			stdout, stderr, err := kubectl("-n", test.tenantNamespace1, "get", "mantlebackup", test.mantleBackupName3)
			if err != nil {
				if strings.Contains(string(stderr), kubectlIsNotFoundMessage) {
					return nil
				}
				return fmt.Errorf("get mantlebackup %s failed. stderr: %s, err: %w", test.mantleBackupName3, string(stderr), err)
			}
			return fmt.Errorf("MantleBackup resource %s still exists. stdout: %s", test.mantleBackupName3, stdout)
		}).Should(Succeed())
	})
}
