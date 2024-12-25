package multik8s

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
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
	//go:embed testdata/mantlerestore-template.yaml
	testMantleRestoreTemplate string
	//go:embed testdata/mount-deploy-template.yaml
	mountDeployTemplate string
	//go:embed testdata/write-job-template.yaml
	writeJobTemplate string

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

func applyMantleRestoreTemplate(clusterNo int, namespace, restoreName, backupName string) error {
	manifest := fmt.Sprintf(testMantleRestoreTemplate, restoreName, restoreName, namespace, backupName)
	_, _, err := kubectl(clusterNo, []byte(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply mantlerestore failed. err: %w", err)
	}
	return nil
}

func applyPVCTemplate(clusterNo int, namespace, name string) error {
	manifest := fmt.Sprintf(testPVCTemplate, name)
	_, _, err := kubectl(clusterNo, []byte(manifest), "apply", "-n", namespace, "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply pvc failed. err: %w", err)
	}
	return nil
}

func applyMountDeployTemplate(clusterNo int, namespace, name, pvcName string) error {
	manifest := fmt.Sprintf(mountDeployTemplate, name, namespace, name, name, pvcName)
	_, _, err := kubectl(clusterNo, []byte(manifest), "apply", "-n", namespace, "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply mount deploy failed. err: %w", err)
	}
	return nil
}

func applyWriteJobTemplate(clusterNo int, namespace, name, pvcName string) error {
	manifest := fmt.Sprintf(writeJobTemplate, name, namespace, pvcName)
	_, _, err := kubectl(clusterNo, []byte(manifest), "apply", "-n", namespace, "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply write job failed. err: %w", err)
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

func applyRBDPoolAndSCTemplate(clusterNo int, namespace string) error { //nolint:unparam
	manifest := fmt.Sprintf(
		testRBDPoolSCTemplate, namespace,
		namespace, namespace, namespace, namespace)
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

func getMR(clusterNo int, namespace, name string) (*mantlev1.MantleRestore, error) {
	return getObject[mantlev1.MantleRestore](clusterNo, "mantlerestore", namespace, name)
}

func getDeploy(clusterNo int, namespace, name string) (*appsv1.Deployment, error) {
	return getObject[appsv1.Deployment](clusterNo, "deploy", namespace, name)
}

func getJob(clusterNo int, namespace, name string) (*batchv1.Job, error) {
	return getObject[batchv1.Job](clusterNo, "job", namespace, name)
}

func changeClusterRole(clusterNo int, newRole string) error {
	deployName := "mantle-controller"
	deploy, err := getDeploy(clusterNo, cephClusterNamespace, deployName)
	if err != nil {
		return fmt.Errorf("failed to get mantle-controller deploy: %w", err)
	}

	roleIndex := slices.IndexFunc(
		deploy.Spec.Template.Spec.Containers[0].Args,
		func(arg string) bool { return strings.HasPrefix(arg, "--role=") },
	)
	if roleIndex == -1 {
		return errors.New("failed to find --role= argument")
	}

	_, _, err = kubectl(
		clusterNo, nil, "patch", "deploy", "-n", cephClusterNamespace, deployName, "--type=json",
		fmt.Sprintf(
			`-p=[{"op": "replace", "path": "/spec/template/spec/containers/0/args/%d", "value":"--role=%s"}]`,
			roleIndex,
			newRole,
		),
	)
	if err != nil {
		return fmt.Errorf("failed to patch mantle-controller deploy: %w", err)
	}

	// Wait for the new controller to start
	numRetries := 10
	for i := 0; i < numRetries; i++ {
		stdout, _, err := kubectl(clusterNo, nil, "get", "pod", "-n", cephClusterNamespace, "-o", "json")
		if err != nil {
			return fmt.Errorf("failed to get pod: %w", err)
		}
		var pods corev1.PodList
		err = json.Unmarshal(stdout, &pods)
		if err != nil {
			return fmt.Errorf("failed to unmarshal pod list: %w", err)
		}
		ready := true
		for _, pod := range pods.Items {
			if strings.HasPrefix(pod.GetName(), deployName) {
				for _, container := range pod.Spec.Containers {
					if !slices.Contains(container.Args, fmt.Sprintf("--role=%s", newRole)) {
						ready = false
					}
				}
			}
		}
		if ready {
			break
		}
		time.Sleep(10 * time.Second)
	}

	return nil
}

// IsJobConditionTrue returns true when the conditionType is present and set to
// `metav1.ConditionTrue`.  Otherwise, it returns false.  Note that we can't use
// meta.IsStatusConditionTrue because it doesn't accept []JobCondition.
func IsJobConditionTrue(conditions []batchv1.JobCondition, conditionType batchv1.JobConditionType) bool {
	for _, cond := range conditions {
		if cond.Type == conditionType && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
