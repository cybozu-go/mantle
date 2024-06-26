package ceph

import (
	"bytes"
	"os/exec"
)

type command interface {
	execute(command ...string) ([]byte, error)
}

type commandImpl struct {
}

func newCommand() command {
	return &commandImpl{}
}

func (c *commandImpl) execute(command ...string) ([]byte, error) {
	cmd := exec.Command(command[0], command[1:]...)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	err := cmd.Run()
	return stdout.Bytes(), err
}
