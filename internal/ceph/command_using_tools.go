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

func (c *commandToolsImpl) execute(cephCommand ...string) ([]byte, error) {
	var stdout bytes.Buffer
	arg := []string{}
	arg = append(arg, c.kubectl...)
	arg = append(arg, "exec", "-n", c.namespace, "deploy/rook-ceph-tools", "--")
	arg = append(arg, cephCommand...)
	command := exec.Command(arg[0], arg[1:]...)
	command.Stdout = &stdout

	err := command.Run()
	return stdout.Bytes(), err
}
