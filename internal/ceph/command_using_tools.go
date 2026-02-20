package ceph

import (
	"bytes"
	"os"
	"os/exec"
)

var (
	envKubectlPath = os.Getenv("KUBECTL")
)

type commandToolsImpl struct {
	kubectl   []string
	namespace string
}

func newCommandTools(kubectl []string, namespace string) command {
	return &commandToolsImpl{
		kubectl:   kubectl,
		namespace: namespace,
	}
}

func (c *commandToolsImpl) execute(cephCommand ...string) ([]byte, []byte, error) {
	var stdout, stderr bytes.Buffer
	arg := make([]string, 0, len(c.kubectl)+5+len(cephCommand))
	arg = append(arg, c.kubectl...)
	arg = append(arg, "exec", "-n", c.namespace, "deploy/rook-ceph-tools", "--")
	arg = append(arg, cephCommand...)
	command := exec.Command(arg[0], arg[1:]...)
	command.Stdout = &stdout
	command.Stderr = &stderr

	err := command.Run()

	return stdout.Bytes(), stderr.Bytes(), err
}
