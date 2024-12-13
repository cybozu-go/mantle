package ceph

import (
	"strings"
	"time"
)

type RBDImageInfo struct {
	ID     string              `json:"id"`
	Parent *RBDImageInfoParent `json:"parent,omitempty"`
}

type RBDImageInfoParent struct {
	Pool     string `json:"pool"`
	Image    string `json:"image"`
	Snapshot string `json:"snapshot"`
}

type RBDTimeStamp struct {
	time.Time
}

func NewRBDTimeStamp(t time.Time) RBDTimeStamp {
	return RBDTimeStamp{t}
}

func (t *RBDTimeStamp) UnmarshalJSON(data []byte) error {
	var err error
	t.Time, err = time.Parse("Mon Jan  2 15:04:05 2006", strings.Trim(string(data), `"`))
	return err
}

type RBDSnapshot struct {
	Id        int          `json:"id,omitempty"`
	Name      string       `json:"name,omitempty"`
	Size      int          `json:"size,omitempty"`
	Protected bool         `json:"protected,string,omitempty"`
	Timestamp RBDTimeStamp `json:"timestamp,omitempty"`
}

type CephCmd interface {
	RBDClone(pool, srcImage, srcSnap, dstImage, features string) error
	RBDInfo(pool, image string) (*RBDImageInfo, error)
	RBDLs(pool string) ([]string, error)
	RBDRm(pool, image string) error
	RBDTrashMv(pool, image string) error
	CephRBDTaskAddTrashRemove(pool, image string) error
	RBDSnapCreate(pool, image, snap string) error
	RBDSnapLs(pool, image string) ([]RBDSnapshot, error)
	RBDSnapRm(pool, image, snap string) error
}

type cephCmdImpl struct {
	command command
}

func NewCephCmd() CephCmd {
	return &cephCmdImpl{
		command: newCommand(),
	}
}

func NewCephCmdWithTools(namespace string) CephCmd {
	return &cephCmdImpl{
		command: newCommandTools(namespace),
	}
}
