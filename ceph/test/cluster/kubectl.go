package cluster

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"

	storagev1 "k8s.io/api/storage/v1"
)

const (
	ROOK_NAMESPACE  = "rook-ceph"
	RBD_PROVISIONER = "rook-ceph.rbd.csi.ceph.com"
)

var (
	//go:embed template/deployment.yaml
	deploymentTemplate string
	//go:embed template/pool.yaml
	poolTemplate string
	//go:embed template/pvc.yaml
	pvcTemplate string
	//go:embed template/sc.yaml
	scTemplate string
)

var kubectlCmd = os.Getenv("KUBECTL")
var cephMatcher = regexp.MustCompile(` (ceph|rbd) `)

func Kubectl(args ...string) ([]byte, error) {
	if len(kubectlCmd) == 0 {
		return nil, fmt.Errorf("KUBECTL environment variable should be set")
	}

	icon := "⚓"
	if cephMatcher.MatchString(strings.Join(args, " ")) {
		icon = "🐙"
	}
	log.Printf("%s kubectl %s", icon, strings.Join(args, " "))
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

	icon := "⚓"
	if cephMatcher.MatchString(strings.Join(args, " ")) {
		icon = "🐙"
	}
	log.Printf("%s kubectl %s", icon, strings.Join(args, " "))
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

func DeleteAllObjects(kind, namespace string) error {
	_, err := Kubectl("delete", kind, "-n", namespace, "--all", "--wait=false")
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

func CreateDeployment(namespace, deployName, pvcName string) error {
	manifest := fmt.Sprintf(deploymentTemplate, namespace, deployName, deployName, deployName, pvcName)
	_, err := KubectlWithInput([]byte(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("failed to create deployment: %w", err)
	}
	_, err = Kubectl("wait", "--for=condition=available", "-n", namespace, "deploy/"+deployName, "--timeout=60s")
	if err != nil {
		return fmt.Errorf("failed to wait for deployment: %w", err)
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
	err := DeleteAllObjects("pod", namespace)
	if err != nil {
		return err
	}

	err = DeleteAllObjects("pvc", namespace)
	if err != nil {
		return err
	}

	_, err = Kubectl("delete", "namespace", namespace, "--wait=false")
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

	err = DeleteAllObjects("cephblockpool", ROOK_NAMESPACE)
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

func ScaleDeployment(namespace, deployName string, replicas int) error {
	_, err := Kubectl("scale", "deploy", "-n", namespace, deployName, fmt.Sprintf("--replicas=%d", replicas))
	if err != nil {
		return fmt.Errorf("failed to scale deployment: %w", err)
	}
	if replicas == 0 {
		_, err = Kubectl("wait", "--for=delete", "pod", "-n", namespace, "--selector=app="+deployName)
		if err != nil {
			return fmt.Errorf("failed to wait for pod deletion: %w", err)
		}
	} else {
		_, err = Kubectl("wait", "--for=condition=available", "deploy", "-n", namespace, deployName)
		if err != nil {
			return fmt.Errorf("failed to wait for deployment available: %w", err)
		}
	}
	return nil
}

// RunWithStopPod runs the function with stopping the pod.
// If deployName is empty, it just runs the function.
func RunWithStopPod(namespace, deployName string, f func() error) error {
	if len(deployName) == 0 {
		return f()
	}

	if err := ScaleDeployment(namespace, deployName, 0); err != nil {
		return err
	}
	if err := f(); err != nil {
		return err
	}
	return ScaleDeployment(namespace, deployName, 1)
}
