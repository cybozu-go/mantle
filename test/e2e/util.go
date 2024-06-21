package e2e

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	testutil "github.com/cybozu-go/mantle/test/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testDataFilename = "test-data.bin"
)

var (
	//go:embed testdata/pvc-template.yaml
	testPVCTemplate string
	//go:embed testdata/rbd-pool-sc-template.yaml
	testRBDPoolSCTemplate string
	//go:embed testdata/mantlebackup-template.yaml
	testMantleBackupTemplate string
	//go:embed testdata/mantlerestore-template.yaml
	testMantleRestoreTemplate string
	//go:embed testdata/pod-volume-mount-template.yaml
	testPodVolumeMountTemplate string

	kubectlPath = os.Getenv("KUBECTL")
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

func execAtPod(ns, pod, cmd string, input []byte, args ...string) ([]byte, []byte, error) {
	if len(kubectlPath) == 0 {
		panic("KUBECTL environment variable is not set")
	}
	return execAtLocal(kubectlPath, input, append([]string{"exec", "-n", ns, pod, "--", cmd}, args...)...)
}

func kubectl(args ...string) ([]byte, []byte, error) {
	if len(kubectlPath) == 0 {
		panic("KUBECTL environment variable is not set")
	}
	return execAtLocal(kubectlPath, nil, args...)
}

func kubectlWithInput(input []byte, args ...string) ([]byte, []byte, error) {
	if len(kubectlPath) == 0 {
		panic("KUBECTL environment variable is not set")
	}
	return execAtLocal(kubectlPath, input, args...)
}

func applyMantleBackupTemplate(namespace, pvcName, backupName string) error {
	manifest := fmt.Sprintf(testMantleBackupTemplate, backupName, backupName, namespace, pvcName)
	_, _, err := kubectlWithInput([]byte(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply mantlebackup failed. err: %w", err)
	}
	return nil
}

func applyMantleRestoreTemplate(namespace, restoreName, backupName string) error {
	manifest := fmt.Sprintf(testMantleRestoreTemplate, restoreName, namespace, backupName)
	_, _, err := kubectlWithInput([]byte(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply mantlerestore failed. err: %w", err)
	}
	return nil
}

func applyPodVolumeMountTemplate(namespace, podName, pvcName string) error {
	manifest := fmt.Sprintf(testPodVolumeMountTemplate, podName, namespace, pvcName)
	_, _, err := kubectlWithInput([]byte(manifest), "apply", "-n", namespace, "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply pod failed. err: %w", err)
	}
	return nil
}

func applyPVCTemplate(namespace, name, storageClassName string) error {
	manifest := fmt.Sprintf(testPVCTemplate, name, storageClassName)
	_, _, err := kubectlWithInput([]byte(manifest), "apply", "-n", namespace, "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply pvc failed. err: %w", err)
	}
	return nil
}

func applyRBDPoolAndSCTemplate(namespace, poolName, storageClassName string) error {
	manifest := fmt.Sprintf(testRBDPoolSCTemplate, poolName, namespace,
		storageClassName, poolName, namespace, namespace, namespace)
	_, _, err := kubectlWithInput([]byte(manifest), "apply", "-n", namespace, "-f", "-")
	if err != nil {
		return err
	}
	return nil
}

func checkDeploymentReady(namespace, name string) error {
	_, stderr, err := kubectl("-n", namespace, "wait", "--for=condition=Available", "deploy", name, "--timeout=1m")
	if err != nil {
		return fmt.Errorf("kubectl wait deploy failed. stderr: %s, err: %w", string(stderr), err)
	}
	return nil
}

func checkSnapshotExist(namespace, poolName, imageName, backupName string) error {
	stdout, stderr, err := kubectl(
		"-n", namespace, "exec", "deploy/rook-ceph-tools", "--",
		"rbd", "snap", "ls", poolName+"/"+imageName, "--format=json")
	if err != nil {
		return fmt.Errorf("rbd snap ls failed. stderr: %s, err: %w", string(stderr), err)
	}
	var snapshots []controller.Snapshot
	err = json.Unmarshal(stdout, &snapshots)
	if err != nil {
		return err
	}

	for _, s := range snapshots {
		if s.Name == backupName {
			return nil
		}
	}

	return fmt.Errorf("snapshot not exists. snapshotName: %s", backupName)
}

func createNamespace(name string) error {
	_, _, err := kubectl("create", "ns", name)
	if err != nil {
		return fmt.Errorf("kubectl create ns failed. err: %w", err)
	}
	return nil
}

func createRBDImage(namespace, poolName, imageName string, siz uint) error {
	_, stderr, err := kubectl(
		"-n", namespace, "exec", "deploy/rook-ceph-tools", "--",
		"rbd", "create", "--size", fmt.Sprintf("%d", siz), poolName+"/"+imageName)
	if err != nil {
		return fmt.Errorf("rbd create failed. stderr: %s, err: %w", string(stderr), err)
	}
	return nil
}

func getImageNameFromPVName(pvName string) (string, error) {
	stdout, stderr, err := kubectl("get", "pv", pvName, "-o", "json")
	if err != nil {
		return "", fmt.Errorf("kubectl get pv failed. stderr: %s, err: %w", string(stderr), err)
	}
	var pv corev1.PersistentVolume
	err = json.Unmarshal(stdout, &pv)
	if err != nil {
		return "", err
	}
	imageName := pv.Spec.CSI.VolumeAttributes["imageName"]
	return imageName, nil
}

func getPVFromPVC(namespace, pvcName string) (string, error) {
	for i := 0; i < 60; i++ {
		stdout, stderr, err := kubectl("-n", namespace, "get", "pvc", pvcName, "-o", "json")
		if err != nil {
			return "", fmt.Errorf("kubectl get pvc failed. stderr: %s, err: %w", string(stderr), err)
		}

		var pvc corev1.PersistentVolumeClaim
		err = json.Unmarshal(stdout, &pvc)
		if err != nil {
			return "", fmt.Errorf("yaml unmarshal failed. err: %w", err)
		}

		if pvc.Status.Phase == "Bound" {
			return pvc.Spec.VolumeName, nil
		}

		time.Sleep(5 * time.Second)
	}

	return "", fmt.Errorf("timeout to get pv from pvc. namespace: %s, pvcName: %s", namespace, pvcName)
}

type rbdInfoParent struct {
	Pool     string `json:"pool"`
	Image    string `json:"image"`
	Snapshot string `json:"snapshot"`
}

type rbdInfo struct {
	CreateTimestamp string         `json:"create_timestamp"`
	ModifyTimestamp string         `json:"modify_timestamp"`
	Parent          *rbdInfoParent `json:"parent"`
}

func getRBDInfo(clusterNS, pool, image string) (*rbdInfo, error) {
	stdout, stderr, err := kubectl(
		"-n", clusterNS, "exec", "deploy/rook-ceph-tools", "--",
		"rbd", "info", "--format", "json", fmt.Sprintf("%s/%s", pool, image))
	if err != nil {
		return nil, fmt.Errorf("rbd info failed. stderr: %s, err: %w", string(stderr), err)
	}

	var info rbdInfo
	err = json.Unmarshal(stdout, &info)
	if err != nil {
		return nil, fmt.Errorf("yaml unmarshal failed. err: %w", err)
	}

	return &info, nil
}

func isMantleRestoreReady(namespace, name string) bool {
	stdout, stderr, err := kubectl("get", "mantlerestore", "-n", namespace, name, "-o", "json")
	if err != nil {
		fmt.Println(string(stderr))
		panic(err)
	}

	var restore mantlev1.MantleRestore
	if err := json.Unmarshal(stdout, &restore); err != nil {
		panic(err)
	}

	if restore.Status.Conditions == nil {
		return false
	}
	readyToUseCondition := meta.FindStatusCondition(restore.Status.Conditions, mantlev1.RestoreConditionReadyToUse)
	return readyToUseCondition.Status == metav1.ConditionTrue
}

func writeTestData(namespace, pvc string, data []byte) error {
	podName := testutil.GetUniqueName("pod-")

	if err := applyPodVolumeMountTemplate(namespace, podName, pvc); err != nil {
		return err
	}

	_, _, err := kubectl("wait", "--for=condition=Ready", "pod", podName, "-n", namespace, "--timeout=1m")
	if err != nil {
		return fmt.Errorf("kubectl wait pod failed. err: %w", err)
	}

	_, _, err = execAtPod(namespace, podName, "sh", nil, "-c", fmt.Sprintf("echo -n %q > /mnt/%s", data, testDataFilename))
	if err != nil {
		return fmt.Errorf("write file failed. err: %w", err)
	}

	_, _, err = kubectl("delete", "pod", podName, "-n", namespace)
	if err != nil {
		return fmt.Errorf("kubectl delete pod failed. err: %w", err)
	}

	return nil
}

func readTestData(namespace, pvc string) ([]byte, error) {
	podName := testutil.GetUniqueName("pod-")

	if err := applyPodVolumeMountTemplate(namespace, podName, pvc); err != nil {
		return nil, err
	}

	_, _, err := kubectl("wait", "--for=condition=Ready", "pod", podName, "-n", namespace, "--timeout=1m")
	if err != nil {
		return nil, fmt.Errorf("kubectl wait pod failed. err: %w", err)
	}

	stdout, stderr, err := execAtPod(namespace, podName, "sh", nil, "-c", fmt.Sprintf("cat /mnt/%s", testDataFilename))
	if err != nil {
		return nil, fmt.Errorf("read file failed. stderr: %s, err: %w", string(stderr), err)
	}

	_, _, err = kubectl("delete", "pod", podName, "-n", namespace)
	if err != nil {
		return nil, fmt.Errorf("kubectl delete pod failed. err: %w", err)
	}

	return stdout, nil
}
