package cluster

import "fmt"

func Rbd(args ...string) ([]byte, error) {
	return Kubectl(append([]string{"exec", "-n", ROOK_NAMESPACE, "deploy/rook-ceph-tools", "--", "rbd"}, args...)...)
}

func ExportDiff(args ...string) error {
	stdout, err := Rbd(append([]string{"export-diff"}, args...)...)
	if err != nil {
		return fmt.Errorf("failed to run rbd export-diff command: %w, %s", err, string(stdout))
	}
	return nil
}

func SnapCreate(pool, image, snap string) error {
	stdout, err := Rbd("snap", "create", pool+"/"+image+"@"+snap)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w, %s", err, string(stdout))
	}
	return nil
}

func SnapRemove(pool, image, snap string) error {
	stdout, err := Rbd("snap", "rm", pool+"/"+image+"@"+snap)
	if err != nil {
		return fmt.Errorf("failed to remove snapshot: %w, %s", err, string(stdout))
	}
	return nil
}
