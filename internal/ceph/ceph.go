package ceph

type RBDInfo struct {
	ParentPool  string
	ParentImage string
	ParentSnap  string
}

type CephCmd interface {
	RBDClone(pool, srcImage, srcSnap, dstImage, features string) error
	RBDInfo(pool, image string) (*RBDInfo, error)
	RBDLs(pool string) ([]string, error)
	RBDRm(pool, image string) error
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
