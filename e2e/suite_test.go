package e2e

import (
	"bytes"
	_ "embed"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	backupv1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/yaml"
)

var (
	//go:embed testdata/pvc-template.yaml
	testPVCTemplate string

	//go:embed testdata/rbd-pool-sc-template.yaml
	testRBDPoolSCTemplate string

	//go:embed testdata/mantlebackup-template.yaml
	testMantleBackupTemplate string

	kubectlIsNotFoundMessage = "Error from server (NotFound):"
)

const (
	pvcName           = "rbd-pvc"
	pvcName2          = "rbd-pvc-2"
	poolName          = "replicapool"
	mantleBackupName  = "mantlebackup-test"
	mantleBackupName2 = "mantlebackup-test-2"
	mantleBackupName3 = "mantlebackup-test-3"
	namespace         = "rook-ceph"
)

func execAtLocal(cmd string, input []byte, args ...string) ([]byte, []byte, error) {
	var stdout, stderr bytes.Buffer
	command := exec.Command(cmd, args...)
	command.Stdout = &stdout
	command.Stderr = &stderr

	if len(input) != 0 {
		command.Stdin = bytes.NewReader(input)
	}

	err := command.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

func kubectl(args ...string) ([]byte, []byte, error) {
	return execAtLocal("kubectl", nil, args...)
}

func kubectlWithInput(input []byte, args ...string) ([]byte, []byte, error) {
	return execAtLocal("kubectl", input, args...)
}

func TestMtest(t *testing.T) {
	if os.Getenv("E2ETEST") == "" {
		t.Skip("Run under e2e/")
	}

	RegisterFailHandler(Fail)

	SetDefaultEventuallyPollingInterval(time.Second)
	SetDefaultEventuallyTimeout(3 * time.Minute)

	RunSpecs(t, "rbd backup system test")
}

var _ = BeforeSuite(func() {
	By("[BeforeSuite] Creating common resources")
	Eventually(func() error {
		manifest := fmt.Sprintf(testRBDPoolSCTemplate, poolName, namespace, poolName, namespace, namespace, namespace)
		_, _, err := kubectlWithInput([]byte(manifest), "apply", "-n", namespace, "-f", "-")
		if err != nil {
			return err
		}

		return nil
	}).Should(Succeed())

	for _, name := range []string{pvcName, pvcName2} {
		By(fmt.Sprintf("[BeforeSuite] Creating PVC (%s)", name))
		Eventually(func() error {
			manifest := fmt.Sprintf(testPVCTemplate, name)
			_, _, err := kubectlWithInput([]byte(manifest), "apply", "-n", namespace, "-f", "-")
			if err != nil {
				return err
			}

			return nil
		}).Should(Succeed())

		By(fmt.Sprintf("[BeforeSuite] Waiting for PVC(%s) to get bound", name))
		Eventually(func() error {
			stdout, stderr, err := kubectl("-n", namespace, "get", "pvc", name, "-o", "json")
			if err != nil {
				return fmt.Errorf("kubectl get pvc failed. stderr: %s, err: %w", string(stderr), err)
			}

			var pvc corev1.PersistentVolumeClaim
			err = yaml.Unmarshal(stdout, &pvc)
			if err != nil {
				return err
			}

			if pvc.Status.Phase != "Bound" {
				return fmt.Errorf("PVC is not bound yet")
			}

			return nil
		}).Should(Succeed())
	}

	By("[BeforeSuite] Waiting for mantle-controller-manager to get ready")
	Eventually(func() error {
		stdout, stderr, err := kubectl("-n", namespace, "get", "deploy", "mantle-controller-manager", "-o", "json")
		if err != nil {
			return fmt.Errorf("kubectl get deploy failed. stderr: %s, err: %w", string(stderr), err)
		}

		var deploy appsv1.Deployment
		err = yaml.Unmarshal(stdout, &deploy)
		if err != nil {
			return err
		}

		if deploy.Status.AvailableReplicas != 1 {
			return fmt.Errorf("mantle-controller-manager is not available yet")
		}

		return nil
	}).Should(Succeed())
})

var _ = AfterSuite(func() {
	By("[AfterSuite] Deleting MantleBackups")
	for _, mantleBackup := range []string{mantleBackupName, mantleBackupName2, mantleBackupName3} {
		_, _, _ = kubectl("delete", "-n", namespace, "mantlebackup", mantleBackup)
	}

	By("[AfterSuite] Deleting PVCs")
	for _, pvc := range []string{pvcName, pvcName2} {
		_, _, _ = kubectl("delete", "-n", namespace, "pvc", pvc)
	}

	By("[AfterSuite] Deleting common resources")
	_, _, _ = kubectl("delete", "sc", "rook-ceph-block", "--wait=false")
	_, _, _ = kubectl("delete", "-n", namespace, "cephblockpool", "replicapool", "--wait=false")
})

var _ = Describe("rbd backup system", func() {
	var firstImageName string

	testMantleBackupResourceCreation := func(mantleBackupName string, saveImageName bool) {
		By("Creating MantleBackup")
		manifest := fmt.Sprintf(testMantleBackupTemplate, mantleBackupName, mantleBackupName, namespace, pvcName)
		_, _, err := kubectlWithInput([]byte(manifest), "apply", "-f", "-")
		Expect(err).NotTo(HaveOccurred())

		By("Waiting for RBD snapshot to be created")
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

			stdout, stderr, err = kubectl("get", "pv", pvName, "-o", "json")
			if err != nil {
				return fmt.Errorf("kubectl get pv failed. stderr: %s, err: %w", string(stderr), err)
			}
			var pv corev1.PersistentVolume
			err = yaml.Unmarshal(stdout, &pv)
			if err != nil {
				return err
			}
			if saveImageName {
				firstImageName = pv.Spec.CSI.VolumeAttributes["imageName"]
			}
			imageName := pv.Spec.CSI.VolumeAttributes["imageName"]

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

			stdout, stderr, err = kubectl("get", "pv", pvName, "-o", "json")
			if err != nil {
				return fmt.Errorf("kubectl get pv failed. stderr: %s, err: %w", string(stderr), err)
			}
			var pv corev1.PersistentVolume
			err = yaml.Unmarshal(stdout, &pv)
			if err != nil {
				return err
			}
			imageName := pv.Spec.CSI.VolumeAttributes["imageName"]

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
