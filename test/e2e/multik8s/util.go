package multik8s

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	cephClusterNamespace = "rook-ceph"
	primaryK8sCluster    = 1
	secondaryK8sCluster  = 2
)

var (
	//go:embed testdata/pvc-template.yaml
	testPVCTemplate string
	//go:embed testdata/rbd-pool-sc-template.yaml
	testRBDPoolSCTemplate string
	//go:embed testdata/mantlebackup-template.yaml
	testMantleBackupTemplate string

	kubectlPrefixPrimary   = os.Getenv("KUBECTL_PRIMARY")
	kubectlPrefixSecondary = os.Getenv("KUBECTL_SECONDARY")
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

// input can be nil
func kubectl(clusterNo int, input []byte, args ...string) ([]byte, []byte, error) {
	kubectlPrefix := ""
	switch clusterNo {
	case primaryK8sCluster:
		kubectlPrefix = kubectlPrefixPrimary
	case secondaryK8sCluster:
		kubectlPrefix = kubectlPrefixSecondary
	default:
		panic(fmt.Sprintf("invalid clusterNo: %d", clusterNo))
	}
	if len(kubectlPrefix) == 0 {
		panic("Either KUBECTL_PRIMARY or KUBECTL_SECONDARY environment variable is not set")
	}
	fields := strings.Fields(kubectlPrefix)
	fields = append(fields, args...)
	return execAtLocal(fields[0], input, fields[1:]...)
}

func checkDeploymentReady(clusterNo int, namespace, name string) error {
	_, stderr, err := kubectl(
		clusterNo, nil,
		"-n", namespace, "wait", "--for=condition=Available", "deploy", name, "--timeout=1m",
	)
	if err != nil {
		return fmt.Errorf("kubectl wait deploy failed. stderr: %s, err: %w", string(stderr), err)
	}
	return nil
}

func applyMantleBackupTemplate(clusterNo int, namespace, pvcName, backupName string) error {
	manifest := fmt.Sprintf(testMantleBackupTemplate, backupName, backupName, namespace, pvcName)
	_, _, err := kubectl(clusterNo, []byte(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply mantlebackup failed. err: %w", err)
	}
	return nil
}

func applyPVCTemplate(clusterNo int, namespace, name, storageClassName string) error {
	manifest := fmt.Sprintf(testPVCTemplate, name, storageClassName)
	_, _, err := kubectl(clusterNo, []byte(manifest), "apply", "-n", namespace, "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply pvc failed. err: %w", err)
	}
	return nil
}

func createNamespace(clusterNo int, name string) error {
	_, _, err := kubectl(clusterNo, nil, "create", "ns", name)
	if err != nil {
		return fmt.Errorf("kubectl create ns failed. err: %w", err)
	}
	return nil
}

func applyRBDPoolAndSCTemplate(clusterNo int, namespace, poolName, storageClassName string) error {
	manifest := fmt.Sprintf(
		testRBDPoolSCTemplate, poolName, namespace,
		storageClassName, namespace, poolName, namespace, namespace, namespace)
	_, _, err := kubectl(clusterNo, []byte(manifest), "apply", "-n", namespace, "-f", "-")
	if err != nil {
		return err
	}
	return nil
}

func getObject[T any](clusterNo int, kind, namespace, name string) (*T, error) {
	stdout, _, err := kubectl(clusterNo, nil, "get", kind, "-n", namespace, name, "-o", "json")
	if err != nil {
		return nil, err
	}

	var obj T
	if err := json.Unmarshal(stdout, &obj); err != nil {
		return nil, err
	}

	return &obj, nil
}

func getMB(clusterNo int, namespace, name string) (*mantlev1.MantleBackup, error) {
	return getObject[mantlev1.MantleBackup](clusterNo, "mantlebackup", namespace, name)
}

func getPVC(clusterNo int, namespace, name string) (*corev1.PersistentVolumeClaim, error) {
	return getObject[corev1.PersistentVolumeClaim](clusterNo, "pvc", namespace, name)
}
