package ceph

import (
	"bytes"
	"os/exec"
)

type command interface {
	execute(command ...string) ([]byte, []byte, error)
}

type commandImpl struct {
}

func newCommand() command {
	return &commandImpl{}
}

func (c *commandImpl) execute(command ...string) ([]byte, []byte, error) {
	cmd := exec.Command(command[0], command[1:]...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}
