package e2e

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os/exec"

	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
)

var (
	//go:embed testdata/pvc-template.yaml
	testPVCTemplate string
	//go:embed testdata/rbd-pool-sc-template.yaml
	testRBDPoolSCTemplate string
	//go:embed testdata/mantlebackup-template.yaml
	testMantleBackupTemplate string
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

func applyPVCAndGetPVName(namespace, name string) (string, error) {
	manifest := fmt.Sprintf(testPVCTemplate, name)
	_, _, err := kubectlWithInput([]byte(manifest), "apply", "-n", namespace, "-f", "-")
	if err != nil {
		return "", fmt.Errorf("kubectl apply pvc failed. err: %w", err)
	}

	_, stderr, err := kubectl("-n", namespace, "wait", "--for=jsonpath='{.status.phase}'=Bound", "pvc", name, "--timeout=1m")
	if err != nil {
		return "", fmt.Errorf("kubectl wait pvc failed. stderr: %s, err: %w", string(stderr), err)
	}

	stdout, stderr, err := kubectl("-n", namespace, "get", "pvc", name, "-o", "json")
	if err != nil {
		return "", fmt.Errorf("kubectl get pvc failed. stderr: %s, err: %w", string(stderr), err)
	}

	var pvc corev1.PersistentVolumeClaim
	err = yaml.Unmarshal(stdout, &pvc)
	if err != nil {
		return "", fmt.Errorf("yaml unmarshal failed. err: %w", err)
	}

	return pvc.Spec.VolumeName, nil
}

func applyRBDPoolAndSC(namespace, poolName, storageClassName string) error {
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
