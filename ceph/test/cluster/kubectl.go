package cluster

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"

	storagev1 "k8s.io/api/storage/v1"
)

const (
	ROOK_NAMESPACE  = "rook-ceph"
	RBD_PROVISIONER = "rook-ceph.rbd.csi.ceph.com"
)

var (
	//go:embed template/pod.yaml
	podTemplate string
	//go:embed template/pool.yaml
	poolTemplate string
	//go:embed template/pvc.yaml
	pvcTemplate string
	//go:embed template/sc.yaml
	scTemplate string
)

var kubectlCmd = os.Getenv("KUBECTL")

func Kubectl(args ...string) ([]byte, error) {
	if len(kubectlCmd) == 0 {
		return nil, fmt.Errorf("KUBECTL environment variable should be set")
	}

	var stdout bytes.Buffer
	command := exec.Command(kubectlCmd, args...)
	command.Stdout = &stdout
	command.Stderr = os.Stderr

	err := command.Run()
	return stdout.Bytes(), err
}

func KubectlWithInput(stdin []byte, args ...string) ([]byte, error) {
	if len(kubectlCmd) == 0 {
		return nil, fmt.Errorf("KUBECTL environment variable should be set")
	}

	var stdout bytes.Buffer
	command := exec.Command(kubectlCmd, args...)
	command.Stdin = bytes.NewReader(stdin)
	command.Stdout = &stdout
	command.Stderr = os.Stderr

	err := command.Run()
	return stdout.Bytes(), err
}

func GetObjectList[T any](kind, namespace string) (*T, error) {
	var stdout []byte
	var err error
	if namespace == "" {
		stdout, err = Kubectl("get", kind, "-o", "json")
	} else {
		stdout, err = Kubectl("get", kind, "-n", namespace, "-o", "json")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get %s list: %w", kind, err)
	}

	var objList T
	if err := json.Unmarshal(stdout, &objList); err != nil {
		return nil, fmt.Errorf("failed to unmarshal %s list: %w", kind, err)
	}

	return &objList, nil
}

func DeleteObjects(kind, namespace string) error {
	_, err := Kubectl("delete", kind, "-n", namespace)
	if err != nil {
		return fmt.Errorf("failed to delete %s in %s: %w", kind, namespace, err)
	}
	return nil
}

func CreateNamespace(namespace string) error {
	_, err := Kubectl("create", "namespace", namespace)
	if err != nil {
		return fmt.Errorf("failed to create namespace: %w", err)
	}
	return nil
}

func CreatePod(namespace, podName, pvcName string) error {
	manifest := fmt.Sprintf(podTemplate, namespace, podName, pvcName)
	_, err := KubectlWithInput([]byte(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("failed to create pod: %w", err)
	}
	return nil
}

func CreatePool(poolName string) error {
	manifest := fmt.Sprintf(poolTemplate, poolName)
	_, err := KubectlWithInput([]byte(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("failed to create pool: %w", err)
	}
	return nil
}

func CreatePVC(namespace, pvcName, scName, size string) error {
	manifest := fmt.Sprintf(pvcTemplate, namespace, pvcName, size, scName)
	_, err := KubectlWithInput([]byte(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("failed to create PVC: %w", err)
	}
	return nil
}

func CreateSC(scName, poolName string) error {
	manifest := fmt.Sprintf(scTemplate, scName, poolName)
	_, err := KubectlWithInput([]byte(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("failed to create SC: %w", err)
	}
	return nil
}

func CleanupNamespace(namespace string) error {
	err := DeleteObjects("pod", namespace)
	if err != nil {
		return err
	}

	err = DeleteObjects("pvc", namespace)
	if err != nil {
		return err
	}

	_, err = Kubectl("delete", "namespace", namespace)
	if err != nil {
		return fmt.Errorf("failed to delete namespace: %w", err)
	}
	return nil
}

func CleanupGlobal() error {
	SCs, err := GetObjectList[storagev1.StorageClassList]("sc", "")
	if err != nil {
		return err
	}
	for _, sc := range SCs.Items {
		if sc.Provisioner == RBD_PROVISIONER {
			_, err = Kubectl("delete", "sc", sc.Name)
			if err != nil {
				return fmt.Errorf("failed to delete SC: %w", err)
			}
		}
	}

	err = DeleteObjects("cephblockpool", ROOK_NAMESPACE)
	if err != nil {
		return err
	}

	return nil
}

func GetImageNameByPVC(namespace, pvcName string) (string, error) {
	var volumeName string
	for len(volumeName) == 0 {
		stdout, err := Kubectl("get", "-n", namespace, "pvc", pvcName, "-o", "jsonpath={.spec.volumeName}")
		if err != nil {
			return "", fmt.Errorf("failed to get volume name by PVC: %w", err)
		}
		volumeName = string(stdout)
	}

	stdout, err := Kubectl("get", "pv", volumeName, "-o", "jsonpath={.spec.csi.volumeAttributes.imageName}")
	if err != nil {
		return "", fmt.Errorf("failed to get image name by volume: %w", err)
	}

	return string(stdout), nil
}
