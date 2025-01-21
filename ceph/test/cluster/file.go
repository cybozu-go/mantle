package cluster

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/cybozu-go/mantle/test/util"
)

var workDir string

func MakeRandomFile(filename string, size int) error {
	args := []string{"if=/dev/urandom", "of=" + path.Join(workDir, filename), "bs=1K", "count=" + strconv.Itoa(size/1024)}
	log.Printf("📂 dd %s", strings.Join(args, " "))
	command := exec.Command("dd", args...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	defer showMD5Sum(path.Join(workDir, filename))
	return command.Run()
}

func getPodNameByDeploy(namespace, deployName string) (string, error) {
	stdout, err := Kubectl("get", "pod", "-n", namespace, "-l", "app="+deployName, "-o", "jsonpath={.items[0].metadata.name}")
	if err != nil {
		return "", err
	}
	return string(stdout), nil
}

func PushFileToPod(filename, namespace, deployName, dst string) error {
	podName, err := getPodNameByDeploy(namespace, deployName)
	if err != nil {
		return err
	}
	_, err = Kubectl("cp", path.Join(workDir, filename), namespace+"/"+podName+":"+dst)
	if err != nil {
		return fmt.Errorf("failed to copy file to pod: %w", err)
	}

	_, err = Kubectl("exec", "-n", namespace, podName, "--", "sync")
	return err
}

func RemoveFileByPod(namespace, deployName, target string) error {
	podName, err := getPodNameByDeploy(namespace, deployName)
	if err != nil {
		return err
	}
	_, err = Kubectl("exec", "-n", namespace, podName, "--", "rm", "-f", target)
	return err
}

func CompareFilesInPod(filename, namespace, deployName, target string) error {
	workFilename := util.GetUniqueName("compare-file-")
	defer func() {
		_ = os.Remove(path.Join(workDir, workFilename))
	}()

	podName, err := getPodNameByDeploy(namespace, deployName)
	if err != nil {
		return err
	}
	_, err = Kubectl("cp", namespace+"/"+podName+":"+target, path.Join(workDir, workFilename))
	if err != nil {
		return err
	}

	args := []string{path.Join(workDir, filename), path.Join(workDir, workFilename)}
	log.Printf("📂 diff %s", strings.Join(args, " "))
	_, err = exec.Command("diff", args...).CombinedOutput()
	if err != nil {
		showMD5Sum(path.Join(workDir, filename))
		showMD5Sum(path.Join(workDir, workFilename))
		return fmt.Errorf("the file would have differences: %w", err)
	}
	return nil
}

func RemoveWorkDir() {
	if err := os.RemoveAll(workDir); err != nil {
		log.Fatalf("failed to remove workDir: %v", err)
	}
}

func showMD5Sum(filename string) {
	args := []string{filename}
	log.Printf("📂 md5sum %s", strings.Join(args, " "))
	command := exec.Command("md5sum", args...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	err := command.Run()
	if err != nil {
		log.Fatalf("failed to run md5sum: %v", err)
	}
}

func init() {
	dir, err := os.MkdirTemp("", "test-mantle-ceph-")
	if err != nil {
		log.Fatalf("failed to create workDir: %v", err)
	}
	workDir = dir
}
