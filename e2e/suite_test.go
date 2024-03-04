package e2e

import (
	"bytes"
	_ "embed"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"

	backupv1 "github.com/cybozu-go/rbd-backup-system/api/v1"
	"github.com/cybozu-go/rbd-backup-system/internal/controller"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

var (
	//go:embed testdata/pvc-template.yaml
	dummyPVCTemplate string

	//go:embed testdata/rook-pool-sc-template.yaml
	dummyRookPoolSCTemplate string

	//go:embed testdata/rbdpvcbackup-template.yaml
	dummyRBDPVCBackupTemplate string
)

const (
	pvcName          = "rbd-pvc"
	poolName         = "replicapool"
	rbdPVCBackupName = "rbdpvcbackup-test"
	namespace        = "rook-ceph"
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
	By("[BeforeSuite] Waiting for rook to get ready")
	Eventually(func() error {
		stdout, stderr, err := kubectl("-n", namespace, "get", "deploy", "rook-ceph-operator", "-o", "json")
		if err != nil {
			return fmt.Errorf("kubectl get deploy failed. stderr: %s, err: %w", string(stderr), err)
		}

		var deploy appsv1.Deployment
		err = yaml.Unmarshal(stdout, &deploy)
		if err != nil {
			return err
		}

		if deploy.Status.AvailableReplicas != 1 {
			return fmt.Errorf("rook operator is not available yet")
		}

		return nil
	}).Should(Succeed())

	By("[BeforeSuite] Waiting for ceph cluster to get ready")
	Eventually(func() error {
		stdout, stderr, err := kubectl("-n", namespace, "get", "deploy", "rook-ceph-osd-0", "-o", "json")
		if err != nil {
			return fmt.Errorf("kubectl get deploy failed. stderr: %s, err: %w", string(stderr), err)
		}

		var deploy appsv1.Deployment
		err = yaml.Unmarshal(stdout, &deploy)
		if err != nil {
			return err
		}

		if deploy.Status.AvailableReplicas != 1 {
			return fmt.Errorf("osd.0 is not available yet")
		}

		return nil
	}).Should(Succeed())

	By("[BeforeSuite] Creating Rook Pool and SC")
	Eventually(func() error {
		manifest := fmt.Sprintf(dummyRookPoolSCTemplate, poolName, namespace, poolName, namespace, namespace, namespace)
		_, _, err := kubectlWithInput([]byte(manifest), "apply", "-n", namespace, "-f", "-")
		if err != nil {
			return err
		}

		return nil
	}).Should(Succeed())

	By("[BeforeSuite] Creating PVC")
	Eventually(func() error {
		manifest := fmt.Sprintf(dummyPVCTemplate, pvcName)
		_, _, err := kubectlWithInput([]byte(manifest), "apply", "-n", namespace, "-f", "-")
		if err != nil {
			return err
		}

		return nil
	}).Should(Succeed())

	By("[BeforeSuite] Waiting for PVC to get bound")
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

		if pvc.Status.Phase != "Bound" {
			return fmt.Errorf("PVC is not bound yet")
		}

		return nil
	}).Should(Succeed())

	By("[BeforeSuite] Waiting for rbd-backup-system-controller-manager to get ready")
	Eventually(func() error {
		stdout, stderr, err := kubectl("-n", namespace, "get", "deploy", "rbd-backup-system-controller-manager", "-o", "json")
		if err != nil {
			return fmt.Errorf("kubectl get deploy failed. stderr: %s, err: %w", string(stderr), err)
		}

		var deploy appsv1.Deployment
		err = yaml.Unmarshal(stdout, &deploy)
		if err != nil {
			return err
		}

		if deploy.Status.AvailableReplicas != 1 {
			return fmt.Errorf("rbd-backup-system-controller-manager is not available yet")
		}

		return nil
	}).Should(Succeed())
})

var _ = Describe("rbd backup system", func() {
	var imageName string

	It("should create RBDPVCBackup resource", func() {
		By("Creating RBDPVCBackup")
		manifest := fmt.Sprintf(dummyRBDPVCBackupTemplate, rbdPVCBackupName, rbdPVCBackupName, namespace, pvcName)
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
			imageName = pv.Spec.CSI.VolumeAttributes["imageName"]

			stdout, stderr, err = kubectl("-n", namespace, "exec", "deploy/rook-ceph-tools", "--", "rbd", "snap", "ls", poolName+"/"+imageName, "--format=json")
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
				if s.Name == rbdPVCBackupName {
					existSnapshot = true
					break
				}
			}
			if !existSnapshot {
				return fmt.Errorf("snapshot not exists. snapshotName: %s", rbdPVCBackupName)
			}

			return nil
		}).Should(Succeed())
	})

	It("should not delete RBDPVCBackup resource when delete backup target PVC", func() {
		By("Deleting backup target PVC")
		_, _, err := kubectl("-n", namespace, "delete", "pvc", pvcName)
		Expect(err).NotTo(HaveOccurred())

		By("Checking that the status.conditions of the RBDPVCBackup resource remain \"Bound\"")
		stdout, _, err := kubectl("-n", namespace, "get", "rbdpvcbackup", rbdPVCBackupName, "-o", "json")
		Expect(err).NotTo(HaveOccurred())
		var backup backupv1.RBDPVCBackup
		err = yaml.Unmarshal(stdout, &backup)
		Expect(err).NotTo(HaveOccurred())
		Expect(backup.Status.Conditions).To(Equal(backupv1.RBDPVCBackupConditionsBound))
	})

	It("should delete RBDPVCBackup resource", func() {
		By("Delete RBDPVCBackup")
		_, _, err := kubectl("-n", namespace, "delete", "rbdpvcbackup", rbdPVCBackupName, "--wait=false")
		Expect(err).NotTo(HaveOccurred())

		By("Waiting for RBD snapshot to be deleted")
		Eventually(func() error {
			stdout, stderr, err := kubectl("-n", namespace, "exec", "deploy/rook-ceph-tools", "--", "rbd", "snap", "ls", poolName+"/"+imageName, "--format=json")
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
				if s.Name == rbdPVCBackupName {
					existSnapshot = true
					break
				}
			}
			if existSnapshot {
				return fmt.Errorf("snapshot exists. snapshotName: %s", rbdPVCBackupName)
			}

			return nil
		}).Should(Succeed())

		By("Confirming RBDPVCBackup resource deletion")
		Eventually(func() error {
			stdout, stderr, err := kubectl("-n", namespace, "get", "rbdpvcbackup", rbdPVCBackupName)
			if errors.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("get rbdpvcbackup %s failed. stderr: %s, err: %w", rbdPVCBackupName, string(stderr), err)
			}
			return fmt.Errorf("RBDPVCBackup resource %s still exists. stdout: %s", rbdPVCBackupName, stdout)
		}).Should(Succeed())
	})
})
