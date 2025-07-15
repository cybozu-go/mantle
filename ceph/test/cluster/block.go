package cluster

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/cybozu-go/mantle/test/util"
)

func ZeroOutBlock(namespace, deployName string) error {
	_, err := Kubectl("exec", "-n", namespace, "deploy/"+deployName, "--",
		"blkdiscard", "-z", "/dev/rbd-device")
	if err != nil {
		return fmt.Errorf("failed to zero out volume: %w", err)
	}
	return nil
}

func WriteRandomBlock(namespace, deployName string, offset, size uint64) error {
	_, err := Kubectl("exec", "-n", namespace, "deploy/"+deployName, "--",
		"dd", "if=/dev/urandom", "of=/dev/rbd-device", "bs=1K",
		fmt.Sprintf("seek=%d", offset/1024), fmt.Sprintf("count=%d", size/1024), "oflag=direct,dsync")
	if err != nil {
		return fmt.Errorf("failed to write random block: %w", err)
	}
	return nil
}

func GetBlockAsFile(namespace, deployName, filename string) error {
	const workFilename = "/tmp/work.bin"

	_, err := Kubectl("exec", "-n", namespace, "deploy/"+deployName, "--",
		"dd", "if=/dev/rbd-device", "of="+workFilename, "bs=1K")
	if err != nil {
		return fmt.Errorf("failed to get block as file: %w", err)
	}

	podName, err := GetPodNameByDeploy(namespace, deployName)
	if err != nil {
		return err
	}
	_, err = Kubectl("cp", namespace+"/"+podName+":"+workFilename, path.Join(workDir, filename))
	if err != nil {
		return fmt.Errorf("failed to copy file to pod: %w", err)
	}

	return RemoveFileByPod(namespace, deployName, workFilename)
}

func CompareBlockWithFile(namespace, deployName, filename string) error {
	workFilename := util.GetUniqueName("compare-file-")
	defer func() {
		_ = os.Remove(path.Join(workDir, workFilename))
	}()

	if err := GetBlockAsFile(namespace, deployName, workFilename); err != nil {
		return err
	}

	args := []string{path.Join(workDir, filename), path.Join(workDir, workFilename)}
	log.Printf("📂 cmp %s", strings.Join(args, " "))
	_, err := exec.Command("cmp", args...).CombinedOutput()
	if err != nil {
		showMD5Sum(path.Join(workDir, filename))
		showMD5Sum(path.Join(workDir, workFilename))
		return fmt.Errorf("the devices having differences: %w", err)
	}
	return nil
}
