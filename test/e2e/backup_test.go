package e2e

import (
	"encoding/json"
	"fmt"
	"strings"

	backupv1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

const (
	pvcName           = "rbd-pvc"
	pvcName2          = "rbd-pvc-2"
	poolName          = "replicapool"
	mantleBackupName  = "mantlebackup-test"
	mantleBackupName2 = "mantlebackup-test-2"
	mantleBackupName3 = "mantlebackup-test-3"
	namespace         = "rook-ceph"
	namespace2        = "rook-ceph2"
	storageClassName  = "rook-ceph-block"
	storageClassName2 = "rook-ceph-block2"
)

func testBackup() {
	BeforeEach(func() {
		By("Creating common resources")
		Eventually(func() error {
			if err := applyRBDPoolAndSC(namespace, poolName, storageClassName); err != nil {
				return err
			}
			if err := applyRBDPoolAndSC(namespace2, poolName, storageClassName2); err != nil {
				return err
			}
			return nil
		}).Should(Succeed())

		for _, name := range []string{pvcName, pvcName2} {
			By(fmt.Sprintf("Creating PVC, PV and RBD image (%s)", name))
			Eventually(func() error {
				pvName, err := applyPVCAndGetPVName(namespace, name)
				if err != nil {
					return err
				}

				imageName, err := getImageNameFromPVName(pvName)
				if err != nil {
					return err
				}

				// Create a new RBD image in namespace2 with the same name as imageName.
				return createRBDImage(namespace2, poolName, imageName, 100)
			}).Should(Succeed())
		}

		By("Waiting for mantle-controller to get ready")
		Eventually(func() error {
			return checkDeploymentReady(namespace, "mantle-controller")
		}).Should(Succeed())
	})

	AfterEach(func() {
		By("Deleting MantleBackups")
		for _, mantleBackup := range []string{mantleBackupName, mantleBackupName2, mantleBackupName3} {
			_, _, _ = kubectl("delete", "-n", namespace, "mantlebackup", mantleBackup)
		}

		By("Deleting PVCs")
		for _, pvc := range []string{pvcName, pvcName2} {
			_, _, _ = kubectl("delete", "-n", namespace, "pvc", pvc)
		}

		By("Deleting RBD images in " + namespace2)
		stdout, _, err := kubectl("exec", "-n", namespace2, "deploy/rook-ceph-tools", "--",
			"rbd", "ls", poolName, "--format=json")
		if err == nil {
			imageNames := []string{}
			if err := json.Unmarshal(stdout, &imageNames); err == nil {
				for _, imageName := range imageNames {
					_, _, _ = kubectl("exec", "-n", namespace2, "deploy/rook-ceph-tools", "--",
						"rbd", "rm", poolName+"/"+imageName)
				}
			}
		}

		By("Deleting common resources")
		_, _, _ = kubectl("delete", "sc", storageClassName, "--wait=false")
		_, _, _ = kubectl("delete", "sc", storageClassName2, "--wait=false")
		_, _, _ = kubectl("delete", "-n", namespace, "cephblockpool", poolName, "--wait=false")
		_, _, _ = kubectl("delete", "-n", namespace2, "cephblockpool", poolName, "--wait=false")
	})

	Describe("rbd backup system", func() {
		var firstImageName string

		testMantleBackupResourceCreation := func(mantleBackupName string, saveImageName bool) {
			By("Creating MantleBackup")
			manifest := fmt.Sprintf(testMantleBackupTemplate, mantleBackupName, mantleBackupName, namespace, pvcName)
			_, _, err := kubectlWithInput([]byte(manifest), "apply", "-f", "-")
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for RBD snapshot to be created")
			imageName := ""
			Eventually(func() error {
				stdout, stderr, err := kubectl("-n", namespace, "get", "pvc", pvcName, "-o", "json")
				if err != nil {
					return fmt.Errorf("kubectl get pvc failed. stderr: %s, err: %w", string(stderr), err)
				}
				var pvc corev1.PersistentVolumeClaim
				err = yaml.Unmarshal(stdout, &pvc)
				if err != nil {
					return err
				}
				pvName := pvc.Spec.VolumeName

				imageName, err = getImageNameFromPVName(pvName)
				if err != nil {
					return err
				}
				if saveImageName {
					firstImageName = imageName
				}

				stdout, stderr, err = kubectl(
					"-n", namespace, "exec", "deploy/rook-ceph-tools", "--",
					"rbd", "snap", "ls", poolName+"/"+imageName, "--format=json")
				if err != nil {
					return fmt.Errorf("rbd snap ls failed. stderr: %s, err: %w", string(stderr), err)
				}
				var snapshots []controller.Snapshot
				err = yaml.Unmarshal(stdout, &snapshots)
				if err != nil {
					return err
				}
				existSnapshot := false
				for _, s := range snapshots {
					if s.Name == mantleBackupName {
						existSnapshot = true
						break
					}
				}
				if !existSnapshot {
					return fmt.Errorf("snapshot not exists. snapshotName: %s", mantleBackupName)
				}

				return nil
			}).Should(Succeed())

			By("Checking that the mantle-controller deployed for a certain Rook/Ceph cluster (i.e., " +
				namespace2 + ") doesn't create a snapshot for a MantleBackup for a different Rook/Ceph cluster (i.e., " +
				namespace + ")")
			Consistently(func() error {
				stdout, stderr, err := kubectl(
					"-n", namespace2, "exec", "deploy/rook-ceph-tools", "--",
					"rbd", "snap", "ls", poolName+"/"+imageName, "--format=json")
				if err != nil {
					return fmt.Errorf("rbd snap ls failed. stderr: %s, err: %w", string(stderr), err)
				}
				var snapshots []controller.Snapshot
				err = yaml.Unmarshal(stdout, &snapshots)
				if err != nil {
					return err
				}
				existSnapshot := false
				for _, s := range snapshots {
					if s.Name == mantleBackupName {
						existSnapshot = true
						break
					}
				}
				if existSnapshot {
					return fmt.Errorf("a wrong snapshot exists. snapshotName: %s", mantleBackupName)
				}
				return nil
			}).Should(Succeed())
		}

		It("should create MantleBackup resource", func() {
			testMantleBackupResourceCreation(mantleBackupName, true)
		})

		It("should create multiple MantleBackup resources for the same PVC", func() {
			testMantleBackupResourceCreation(mantleBackupName2, false)
		})

		It("should create MantleBackups resources for different PVCs", func() {
			By("Creating a third MantleBackup for the other PVC")
			manifest := fmt.Sprintf(testMantleBackupTemplate, mantleBackupName3, mantleBackupName3, namespace, pvcName2)
			_, _, err := kubectlWithInput([]byte(manifest), "apply", "-f", "-")
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for RBD snapshot to be created")
			Eventually(func() error {
				stdout, stderr, err := kubectl("-n", namespace, "get", "pvc", pvcName2, "-o", "json")
				if err != nil {
					return fmt.Errorf("kubectl get pvc failed. stderr: %s, err: %w", string(stderr), err)
				}
				var pvc corev1.PersistentVolumeClaim
				err = yaml.Unmarshal(stdout, &pvc)
				if err != nil {
					return err
				}
				pvName := pvc.Spec.VolumeName

				imageName, err := getImageNameFromPVName(pvName)
				if err != nil {
					return err
				}

				stdout, stderr, err = kubectl(
					"-n", namespace, "exec", "deploy/rook-ceph-tools", "--",
					"rbd", "snap", "ls", poolName+"/"+imageName, "--format=json")
				if err != nil {
					return fmt.Errorf("rbd snap ls failed. stderr: %s, err: %w", string(stderr), err)
				}
				var snapshots []controller.Snapshot
				err = yaml.Unmarshal(stdout, &snapshots)
				if err != nil {
					return err
				}
				existSnapshot := false
				for _, s := range snapshots {
					if s.Name == mantleBackupName3 {
						existSnapshot = true
						break
					}
				}
				if !existSnapshot {
					return fmt.Errorf("snapshot not exists. snapshotName: %s", mantleBackupName3)
				}

				return nil
			}).Should(Succeed())
		})

		It("should not delete MantleBackup resource when delete backup target PVC", func() {
			By("Deleting backup target PVC")
			_, _, err := kubectl("-n", namespace, "delete", "pvc", pvcName2)
			Expect(err).NotTo(HaveOccurred())

			By("Checking backup target PVC deletion")
			Eventually(func() error {
				stdout, stderr, err := kubectl("-n", namespace, "get", "pvc", pvcName2)
				if err != nil {
					if strings.Contains(string(stderr), kubectlIsNotFoundMessage) {
						return nil
					}
					return fmt.Errorf("get pvc %s failed. stderr: %s, err: %w", pvcName2, string(stderr), err)
				}
				return fmt.Errorf("PVC %s still exists. stdout: %s", pvcName2, stdout)
			}).Should(Succeed())

			By("Checking that the status.conditions of the MantleBackup resource remain \"Bound\"")
			stdout, _, err := kubectl("-n", namespace, "get", "mantlebackup", mantleBackupName3, "-o", "json")
			Expect(err).NotTo(HaveOccurred())
			var backup backupv1.MantleBackup
			err = yaml.Unmarshal(stdout, &backup)
			Expect(err).NotTo(HaveOccurred())
			Expect(meta.FindStatusCondition(backup.Status.Conditions, backupv1.BackupConditionReadyToUse).Status).
				To(Equal(metav1.ConditionTrue))
		})

		It("should delete MantleBackup resource", func() {
			By("Delete MantleBackup")
			_, _, err := kubectl("-n", namespace, "delete", "mantlebackup", mantleBackupName, "--wait=false")
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for RBD snapshot to be deleted")
			Eventually(func() error {
				stdout, stderr, err := kubectl(
					"-n", namespace, "exec", "deploy/rook-ceph-tools", "--",
					"rbd", "snap", "ls", poolName+"/"+firstImageName, "--format=json")
				if err != nil {
					return fmt.Errorf("rbd snap ls failed. stderr: %s, err: %w", string(stderr), err)
				}
				var snapshots []controller.Snapshot
				err = yaml.Unmarshal(stdout, &snapshots)
				if err != nil {
					return err
				}
				existSnapshot := false
				for _, s := range snapshots {
					if s.Name == mantleBackupName {
						existSnapshot = true
						break
					}
				}
				if existSnapshot {
					return fmt.Errorf("snapshot exists. snapshotName: %s", mantleBackupName)
				}

				return nil
			}).Should(Succeed())

			By("Checking MantleBackup resource deletion")
			Eventually(func() error {
				stdout, stderr, err := kubectl("-n", namespace, "get", "mantlebackup", mantleBackupName)
				if err != nil {
					if strings.Contains(string(stderr), kubectlIsNotFoundMessage) {
						return nil
					}
					return fmt.Errorf("get mantlebackup %s failed. stderr: %s, err: %w", mantleBackupName, string(stderr), err)
				}
				return fmt.Errorf("MantleBackup resource %s still exists. stdout: %s", mantleBackupName, stdout)
			}).Should(Succeed())
		})

		It("should delete MantleBackup resource when backup target PVC is missing", func() {
			By("Deleting MantleBackup resource")
			_, _, err := kubectl("-n", namespace, "delete", "mantlebackup", mantleBackupName3)
			Expect(err).NotTo(HaveOccurred())

			By("Checking MantleBackup resource deletion")
			Eventually(func() error {
				stdout, stderr, err := kubectl("-n", namespace, "get", "mantlebackup", mantleBackupName3)
				if err != nil {
					if strings.Contains(string(stderr), kubectlIsNotFoundMessage) {
						return nil
					}
					return fmt.Errorf("get mantlebackup %s failed. stderr: %s, err: %w", mantleBackupName3, string(stderr), err)
				}
				return fmt.Errorf("MantleBackup resource %s still exists. stdout: %s", mantleBackupName3, stdout)
			}).Should(Succeed())
		})
	})
}
