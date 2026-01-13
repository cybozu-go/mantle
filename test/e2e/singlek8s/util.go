package singlek8s

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/ceph"
	testutil "github.com/cybozu-go/mantle/test/util"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
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
	//go:embed testdata/pod-mount-volume-template.yaml
	testPodMountVolumeTemplate string
	//go:embed testdata/mantlebackupconfig-template.yaml
	testMantleBackupConfigTemplate string

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

func listMantleBackupsByMBCUID(namespace, uid string) ([]mantlev1.MantleBackup, error) {
	stdout, _, err := kubectl("get", "mantlebackup", "-n", namespace, "-l", "mantle.cybozu.io/mbc-uid="+uid, "-o", "json")
	if err != nil {
		return nil, err
	}
	var mbs mantlev1.MantleBackupList
	if err := json.Unmarshal(stdout, &mbs); err != nil {
		return nil, err
	}

	return mbs.Items, nil
}

func getObject[T any](kind, namespace, name string) (*T, error) {
	stdout, _, err := kubectl("get", kind, "-n", namespace, name, "-o", "json")
	if err != nil {
		return nil, err
	}

	var obj T
	if err := json.Unmarshal(stdout, &obj); err != nil {
		return nil, err
	}

	return &obj, nil
}

func getCronJob(namespace, name string) (*batchv1.CronJob, error) {
	return getObject[batchv1.CronJob]("cronjob", namespace, name)
}

func getMB(namespace, name string) (*mantlev1.MantleBackup, error) {
	return getObject[mantlev1.MantleBackup]("mantlebackup", namespace, name)
}

func getMBC(namespace, name string) (*mantlev1.MantleBackupConfig, error) {
	return getObject[mantlev1.MantleBackupConfig]("mantlebackupconfig", namespace, name)
}

func applyMantleBackupConfigTemplate(namespace, pvcName, mbcName string) error {
	manifest := fmt.Sprintf(testMantleBackupConfigTemplate, mbcName, namespace, pvcName)
	_, _, err := kubectlWithInput([]byte(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply mantlebackupconfig failed. err: %w", err)
	}

	return nil
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

func applyPodMountVolumeTemplate(namespace, podName, pvcName string) error {
	manifest := fmt.Sprintf(testPodMountVolumeTemplate, podName, namespace, pvcName)
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
	manifest := fmt.Sprintf(
		testRBDPoolSCTemplate, poolName, namespace,
		storageClassName, namespace, poolName, namespace, namespace, namespace)
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
	cephCmd := ceph.NewCephCmdWithTools(namespace)
	_, err := ceph.FindRBDSnapshot(cephCmd, poolName, imageName, backupName)

	return err
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
		"rbd", "create", "--size", strconv.FormatUint(uint64(siz), 10), poolName+"/"+imageName)
	if err != nil {
		return fmt.Errorf("rbd create failed. stderr: %s, err: %w", string(stderr), err)
	}

	return nil
}

func createRBDCloneImage(namespace, poolName, snapName, cloneName string) error {
	_, stderr, err := kubectl(
		"-n", namespace, "exec", "deploy/rook-ceph-tools", "--",
		"rbd", "clone",
		"--rbd-default-clone-format", "2",
		"--image-feature", "layering,deep-flatten",
		poolName+"/"+snapName, poolName+"/"+cloneName)
	if err != nil {
		return fmt.Errorf("rbd clone failed. stderr: %s, err: %w", string(stderr), err)
	}

	return nil
}

func removeRBDImage(namespace, poolName, imageName string) error {
	_, stderr, err := kubectl(
		"-n", namespace, "exec", "deploy/rook-ceph-tools", "--",
		"rbd", "rm", poolName+"/"+imageName)
	if err != nil {
		return fmt.Errorf("rbd rm failed. stderr: %s, err: %w", string(stderr), err)
	}

	return nil
}

func createRBDSnap(namespace, poolName, imageName, snapName string) error {
	_, stderr, err := kubectl(
		"-n", namespace, "exec", "deploy/rook-ceph-tools", "--",
		"rbd", "snap", "create", poolName+"/"+imageName+"@"+snapName)
	if err != nil {
		return fmt.Errorf("rbd snap create failed. stderr: %s, err: %w", string(stderr), err)
	}

	return nil
}

func removeRBDSnap(namespace, poolName, imageName, snapName string) error {
	_, stderr, err := kubectl(
		"-n", namespace, "exec", "deploy/rook-ceph-tools", "--",
		"rbd", "snap", "rm", poolName+"/"+imageName+"@"+snapName)
	if err != nil {
		return fmt.Errorf("rbd snap rm failed. stderr: %s, err: %w", string(stderr), err)
	}

	return nil
}

func removeAllRBDImageAndSnap(namespace, pool string) error {
	targets, err := getImageAndSnapNames(namespace, pool)
	if err != nil {
		return err
	}

	// remove RBD clone images of targets, RBD snapshots of targets, and target images in order
	clones := []string{}
	snaps := []string{}
	images := []string{}
	for _, target := range targets {
		if strings.Contains(target, "@") {
			snaps = append(snaps, target)
		} else {
			info, err := getRBDInfo(namespace, pool, target)
			if err != nil {
				return err
			}
			if info.Parent != nil {
				clones = append(clones, target)
			} else {
				images = append(images, target)
			}
		}
	}

	// remove RBD clones
	for _, clone := range clones {
		err = removeRBDImage(namespace, pool, clone)
		if err != nil {
			return err
		}
	}

	// remove RBD snapshots
	for _, snap := range snaps {
		imageAndSnap := strings.Split(snap, "@")
		err = removeRBDSnap(namespace, pool, imageAndSnap[0], imageAndSnap[1])
		if err != nil {
			return err
		}
	}

	// remove RBD images
	for _, image := range images {
		err = removeRBDImage(namespace, pool, image)
		if err != nil {
			return err
		}
	}

	return nil
}

func deleteNamespacedResource(namespace, kind string) error {
	stdout, stderr, err := kubectl("get", kind, "-n", namespace, "-o", "jsonpath={.items[*].metadata.name}")
	if err != nil {
		return fmt.Errorf("kubectl get failed. stderr: %s, err: %w", string(stderr), err)
	}

	names := strings.SplitSeq(string(stdout), " ")
	for name := range names {
		if name == "" {
			continue
		}
		_, stderr, err = kubectl("delete", kind, "-n", namespace, "--ignore-not-found=true", name)
		if err != nil {
			return fmt.Errorf("kubectl delete failed. ns: %s, kind: %s, name: %s, stderr: %s, err: %w",
				namespace, kind, name, string(stderr), err)
		}
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
	for range 60 {
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

func isMantleBackupSnapshotCaptured(namespace, name string) (bool, error) {
	stdout, _, err := kubectl("-n", namespace, "get", "mantlebackup", name, "-o", "json")
	if err != nil {
		return false, err
	}
	var backup mantlev1.MantleBackup
	err = yaml.Unmarshal(stdout, &backup)
	if err != nil {
		return false, err
	}

	return backup.IsSnapshotCaptured(), nil
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

	return restore.IsReady()
}

func writeTestData(namespace, pvc string, data []byte) error {
	podName := testutil.GetUniqueName("pod-")

	if err := applyPodMountVolumeTemplate(namespace, podName, pvc); err != nil {
		return err
	}

	_, _, err := kubectl("wait", "--for=condition=Ready", "pod", podName, "-n", namespace, "--timeout=5m")
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

	if err := applyPodMountVolumeTemplate(namespace, podName, pvc); err != nil {
		return nil, err
	}

	_, _, err := kubectl("wait", "--for=condition=Ready", "pod", podName, "-n", namespace, "--timeout=1m")
	if err != nil {
		return nil, fmt.Errorf("kubectl wait pod failed. err: %w", err)
	}

	stdout, stderr, err := execAtPod(namespace, podName, "sh", nil, "-c", "cat /mnt/"+testDataFilename)
	if err != nil {
		return nil, fmt.Errorf("read file failed. stderr: %s, err: %w", string(stderr), err)
	}

	_, _, err = kubectl("delete", "pod", podName, "-n", namespace)
	if err != nil {
		return nil, fmt.Errorf("kubectl delete pod failed. err: %w", err)
	}

	return stdout, nil
}

func getImageAndSnapNames(namespace, pool string) ([]string, error) {
	stdout, stderr, err := kubectl(
		"-n", namespace, "exec", "deploy/rook-ceph-tools", "--",
		"rbd", "ls", "-p", pool, "--format", "json")
	if err != nil {
		return nil, fmt.Errorf("rbd ls failed. stderr: %s, err: %w", string(stderr), err)
	}

	var images []string
	err = json.Unmarshal(stdout, &images)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal output of rbd ls: %w", err)
	}

	cephCmd := ceph.NewCephCmdWithTools(namespace)

	var snaps []string
	for _, image := range images {
		snapshots, err := cephCmd.RBDSnapLs(pool, image)
		if err != nil {
			return nil, fmt.Errorf("rbd snap ls failed. err: %w", err)
		}

		for _, s := range snapshots {
			snaps = append(snaps, fmt.Sprintf("%s@%s", image, s.Name))
		}
	}

	return append(images, snaps...), nil
}

func checkJobExists(namespace, jobName string) (bool, error) {
	stdout, _, err := kubectl("get", "job", "-n", namespace, "-o", "json")
	if err != nil {
		return false, err
	}
	jobList := batchv1.JobList{}
	err = json.Unmarshal(stdout, &jobList)
	if err != nil {
		return false, err
	}
	found := slices.ContainsFunc(jobList.Items, func(j batchv1.Job) bool {
		return j.Name == jobName
	})

	return found, nil
}
