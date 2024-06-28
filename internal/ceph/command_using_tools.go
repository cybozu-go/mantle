package ceph

import (
	"bytes"
	"os"
	"os/exec"
)

var (
	kubectlPath = os.Getenv("KUBECTL")
)

type commandToolsImpl struct {
	namespace string
}

func newCommandTools(namespace string) command {
	return &commandToolsImpl{
		namespace: namespace,
	}
}

func (c *commandToolsImpl) execute(cephCommand ...string) ([]byte, error) {
	if len(kubectlPath) == 0 {
		panic("KUBECTL environment variable is not set")
	}

	var stdout bytes.Buffer
	arg := []string{"exec", "-n", c.namespace, "deploy/rook-ceph-tools", "--"}
	arg = append(arg, cephCommand...)
	command := exec.Command(kubectlPath, arg...)
	command.Stdout = &stdout

	err := command.Run()
	return stdout.Bytes(), err
}
