package e2e

import (
	"errors"
	"fmt"
	"strings"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	kubectlIsNotFoundMessage = "Error from server (NotFound):"
)

type backupTest struct {
	poolName         string
	storageClassName string
	tenantNamespace  string

	pvcName1               string
	pvcName2               string
	mantleBackupName1      string
	mantleBackupName2      string
	mantleBackupName3      string
	mantleBackupConfigName []string
}

func backupTestSuite() {
	test := &backupTest{
		poolName:         util.GetUniqueName("pool-"),
		storageClassName: util.GetUniqueName("sc-"),
		tenantNamespace:  util.GetUniqueName("ns-"),

		pvcName1:          "rbd-pvc1",
		pvcName2:          "rbd-pvc2",
		mantleBackupName1: "mantlebackup-test1",
		mantleBackupName2: "mantlebackup-test2",
		mantleBackupName3: "mantlebackup-test3",
		mantleBackupConfigName: []string{
			"mantlebackupconfig-test1",
			"mantlebackupconfig-test2",
			"mantlebackupconfig-test3",
			"mantlebackupconfig-test4",
			"mantlebackupconfig-test5",
		},
	}

	Describe("setup environment", test.setupEnv)
	Describe("test case 1", test.testCase1)
	Describe("test case 2", test.testCase2)
	Describe("teardown environment", test.teardownEnv)
}

func (test *backupTest) setupEnv() {
	It("setting up the test environment", func() {
		fmt.Fprintf(GinkgoWriter, "%+v\n", *test)
	})

	It("creating common resources", func() {
		err := createNamespace(test.tenantNamespace)
		Expect(err).NotTo(HaveOccurred())

		err = applyRBDPoolAndSCTemplate(cephCluster1Namespace, test.poolName, test.storageClassName)
		Expect(err).NotTo(HaveOccurred())

		for _, name := range []string{test.pvcName1, test.pvcName2} {
			By(fmt.Sprintf("Creating PVC, PV and RBD image (%s)", name))
			err = applyPVCTemplate(test.tenantNamespace, name, test.storageClassName)
			Expect(err).NotTo(HaveOccurred())
		}
	})
}

func (test *backupTest) teardownEnv() {
	It("delete resources in the namespace: "+test.tenantNamespace, func() {
		err := deleteNamespacedResource(test.tenantNamespace, "mantlebackupconfig")
		Expect(err).NotTo(HaveOccurred())
		err = deleteNamespacedResource(test.tenantNamespace, "mantlebackup")
		Expect(err).NotTo(HaveOccurred())
		err = deleteNamespacedResource(test.tenantNamespace, "pvc")
		Expect(err).NotTo(HaveOccurred())
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

func (test *backupTest) testCase1() {
	var firstImageName string

	createMantleBackupAndGetImage := func(mantleBackupName string) string {
		By("Creating MantleBackup")
		err := applyMantleBackupTemplate(test.tenantNamespace, test.pvcName1, mantleBackupName)
		Expect(err).NotTo(HaveOccurred())

		pvName, err := getPVFromPVC(test.tenantNamespace, test.pvcName1)
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
		err := applyMantleBackupTemplate(test.tenantNamespace, test.pvcName2, test.mantleBackupName3)
		Expect(err).NotTo(HaveOccurred())

		pvName, err := getPVFromPVC(test.tenantNamespace, test.pvcName2)
		Expect(err).NotTo(HaveOccurred())

		By("Waiting for RBD snapshot to be created")
		Eventually(func() error {
			imageName, err := getImageNameFromPVName(pvName)
			if err != nil {
				return err
			}

			return checkSnapshotExist(cephCluster1Namespace, test.poolName, imageName, test.mantleBackupName3)
		}).Should(Succeed())

		By("Checking that the status.conditions of the MantleBackup resource becomes \"ReadyToUse\"")
		Eventually(func() error {
			ready, err := isMantleBackupReady(test.tenantNamespace, test.mantleBackupName3)
			if err != nil {
				return err
			}
			if !ready {
				return fmt.Errorf("not ready")
			}
			return nil
		}).Should(Succeed())
	})

	It("should not delete MantleBackup resource when delete backup target PVC", func() {
		By("Deleting backup target PVC")
		_, _, err := kubectl("-n", test.tenantNamespace, "delete", "pvc", test.pvcName2)
		Expect(err).NotTo(HaveOccurred())

		By("Checking backup target PVC deletion")
		Eventually(func() error {
			stdout, stderr, err := kubectl("-n", test.tenantNamespace, "get", "pvc", test.pvcName2)
			if err != nil {
				if strings.Contains(string(stderr), kubectlIsNotFoundMessage) {
					return nil
				}
				return fmt.Errorf("get pvc %s failed. stderr: %s, err: %w", test.pvcName2, string(stderr), err)
			}
			return fmt.Errorf("PVC %s still exists. stdout: %s", test.pvcName2, stdout)
		}).Should(Succeed())

		By("Checking that the status.conditions of the MantleBackup resource remain \"ReadyToUse\"")
		ready, err := isMantleBackupReady(test.tenantNamespace, test.mantleBackupName3)
		Expect(err).NotTo(HaveOccurred())
		Expect(ready).To(Equal(true))
	})

	It("should delete MantleBackup resource", func() {
		By("Delete MantleBackup")
		_, _, err := kubectl("-n", test.tenantNamespace, "delete", "mantlebackup", test.mantleBackupName1, "--wait=false")
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
			stdout, stderr, err := kubectl("-n", test.tenantNamespace, "get", "mantlebackup", test.mantleBackupName1)
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
		_, _, err := kubectl("-n", test.tenantNamespace, "delete", "mantlebackup", test.mantleBackupName3)
		Expect(err).NotTo(HaveOccurred())

		By("Checking MantleBackup resource deletion")
		Eventually(func() error {
			stdout, stderr, err := kubectl("-n", test.tenantNamespace, "get", "mantlebackup", test.mantleBackupName3)
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

func (test *backupTest) testCase2() {
	It("should create and rotate MantleBackups from MantleBackupConfig", func() {
		By("Creating MantleBackupConfig")
		err := applyMantleBackupConfigTemplate(test.tenantNamespace, test.pvcName1, test.mantleBackupConfigName[2])
		Expect(err).NotTo(HaveOccurred())

		By("Waiting for a CronJob to be created")
		mbc, err := getMBC(test.tenantNamespace, test.mantleBackupConfigName[2])
		Expect(err).NotTo(HaveOccurred())
		cronJobName := "mbc-" + string(mbc.UID)
		Eventually(func() error {
			_, err := getCronJob(cephCluster1Namespace, cronJobName)
			return err
		}).Should(Succeed())

		// Because of the e2e's values-mantle.yaml, the CronJob's .spec.schedule
		// is overwritten with "* * * * *", so backup-and-rotate subcommand
		// should be triggered every minute. And every MantleBackup created at minute N will be deleted at minute (N+1),
		// because of `expire-offset` setting in the e2e's values-mantle.yaml.
		By("Waiting for a MantleBackup to be created")
		var mb mantlev1.MantleBackup
		Eventually(func() error {
			mbs, err := listMantleBackupsByMBCUID(test.tenantNamespace, string(mbc.UID))
			if err != nil {
				return err
			}
			if len(mbs) == 1 {
				mb = mbs[0]
				return nil
			}
			return errors.New("MantleBackup not found")
		}).Should(Succeed())

		By("Waiting for MantleBackups to be rotated")
		Eventually(func() error {
			mbs, err := listMantleBackupsByMBCUID(test.tenantNamespace, string(mbc.UID))
			if err != nil {
				return err
			}
			if len(mbs) == 1 && mbs[0].Name != mb.Name {
				return nil
			}
			return errors.New("MantleBackup not rotated")
		}).Should(Succeed())
	})

	It("should not delete the MantleBackups even when the MantleBackupConfig is deleted", func() {
		By("Creating MantleBackupConfig")
		err := applyMantleBackupConfigTemplate(test.tenantNamespace, test.pvcName1, test.mantleBackupConfigName[4])
		Expect(err).NotTo(HaveOccurred())
		mbc, err := getMBC(test.tenantNamespace, test.mantleBackupConfigName[4])
		Expect(err).NotTo(HaveOccurred())

		By("Waiting for a MantleBackup to be created")
		var mb mantlev1.MantleBackup
		Eventually(func() error {
			mbs, err := listMantleBackupsByMBCUID(test.tenantNamespace, string(mbc.UID))
			if err != nil {
				return err
			}
			if len(mbs) != 0 {
				mb = mbs[0]
				return nil
			}
			return errors.New("MantleBackup not found")
		}).Should(Succeed())

		By("Deleting the MantleBackupConfig")
		Eventually(func() error {
			_, _, err := kubectl("delete", "mantlebackupconfig", "-n", mbc.Namespace, mbc.Name)
			return err
		}).Should(Succeed())

		Consistently(func() error {
			_, err := getMB(mb.Namespace, mb.Name)
			return err
		}, "5s", "1s").Should(Succeed())
	})

	It("should not delete a MantleBackup if it has an annotation of retainIfExpired", func() {
		By("Creating MantleBackupConfig")
		err := applyMantleBackupConfigTemplate(test.tenantNamespace, test.pvcName1, test.mantleBackupConfigName[1])
		Expect(err).NotTo(HaveOccurred())

		By("Waiting for a CronJob to be created")
		mbc, err := getMBC(test.tenantNamespace, test.mantleBackupConfigName[1])
		Expect(err).NotTo(HaveOccurred())
		cronJobName := "mbc-" + string(mbc.UID)
		Eventually(func() error {
			_, err := getCronJob(cephCluster1Namespace, cronJobName)
			return err
		}).Should(Succeed())

		By("Attaching an annotation of retainIfExpired to a MantleBackup")
		var mb mantlev1.MantleBackup
		Eventually(func() error {
			mbs, err := listMantleBackupsByMBCUID(test.tenantNamespace, string(mbc.UID))
			if err != nil {
				return err
			}
			if len(mbs) == 0 {
				return errors.New("no MantleBackup available")
			}
			mb = mbs[0]

			_, _, err = kubectl("patch", "mantlebackup", "-n", mb.Namespace, mb.Name, "--type=json",
				"-p", "[{\"op\":\"add\",\"path\":\"/metadata/annotations\","+
					"\"value\":{\"mantle.cybozu.io/retainIfExpired\":\"true\"}}]")
			return err
		}, "120s", "1s").Should(Succeed())

		By("Checking mb is NOT deleted thanks to retainIfExpired annotation")
		Consistently(func() error {
			_, _, err := kubectl("get", "mantlebackup", "-n", mb.Namespace, mb.Name)
			return err
		}, "70s", "10s").Should(Succeed())

		By("Checking mbs not annotated with retainIfExpired are rotated")
		Eventually(func() error {
			mbs, err := listMantleBackupsByMBCUID(test.tenantNamespace, string(mbc.UID))
			if err != nil {
				return err
			}

			for _, mb1 := range mbs {
				value, ok := mb1.Annotations["mantle.cybozu.io/retainIfExpired"]
				if !ok || value != "true" {
					mb = mb1
					return nil
				}
			}
			return errors.New("no MantleBackup available")
		}).Should(Succeed())
		Eventually(func() error {
			_, stderr, err := kubectl("get", "mantlebackup", "-n", mb.Namespace, mb.Name)
			if err != nil {
				if strings.Contains(string(stderr), "NotFound") {
					return nil
				}
				return err
			}
			return errors.New("still exists")
		}).Should(Succeed())
	})

	It("should re-create a CronJob associated with a MantleBackup when it's deleted by someone", func() {
		By("Creating MantleBackupConfig")
		err := applyMantleBackupConfigTemplate(test.tenantNamespace, test.pvcName1, test.mantleBackupConfigName[3])
		Expect(err).NotTo(HaveOccurred())

		By("Waiting for a CronJob to be created")
		mbc, err := getMBC(test.tenantNamespace, test.mantleBackupConfigName[3])
		Expect(err).NotTo(HaveOccurred())
		cronJobName := "mbc-" + string(mbc.UID)
		Eventually(func() error {
			_, err := getCronJob(cephCluster1Namespace, cronJobName)
			return err
		}).Should(Succeed())

		By("Deleting the CronJob")
		_, _, err = kubectl("delete", "cronjob", "-n", cephCluster1Namespace, cronJobName)
		Expect(err).NotTo(HaveOccurred())

		By("Waiting for a CronJob to be created")
		Eventually(func() error {
			_, err := getCronJob(cephCluster1Namespace, cronJobName)
			return err
		}).Should(Succeed())
	})
}
