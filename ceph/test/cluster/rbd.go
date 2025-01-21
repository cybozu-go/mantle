package cluster

import "fmt"

func Rbd(args ...string) ([]byte, error) {
	return Kubectl(append([]string{"exec", "-n", ROOK_NAMESPACE, "deploy/rook-ceph-tools", "--", "rbd"}, args...)...)
}

func ExportDiff(filename string, args ...string) error {
	_, err := Kubectl("exec", "-n", ROOK_NAMESPACE, "deploy/rook-ceph-tools", "--", "rm", "-f", filename)
	if err != nil {
		return err
	}
	args = append([]string{"export-diff"}, args...)
	args = append(args, filename)
	stdout, err := Rbd(args...)
	if err != nil {
		return fmt.Errorf("failed to run rbd export-diff command: %w, %s", err, string(stdout))
	}
	return nil
}

func ImportDiff(filename, pool, image string) error {
	stdout, err := Kubectl("exec", "-n", ROOK_NAMESPACE, "deploy/rook-ceph-tools", "--",
		"sh", "-c", fmt.Sprintf("cat %s | rbd import-diff -p %s - %s", filename, pool, image))
	if err != nil {
		return fmt.Errorf("failed to import diff: %w, %s", err, string(stdout))
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

func SnapRemove(pool, image string, snaps []string) error {
	for _, snap := range snaps {
		stdout, err := Rbd("snap", "rm", pool+"/"+image+"@"+snap)
		if err != nil {
			return fmt.Errorf("failed to remove snapshot: %w, %s", err, string(stdout))
		}
	}
	return nil
}

func SnapRollback(namespace, deployName, pool, image, snap string) error {
	if len(deployName) != 0 {
		_, err := Kubectl("scale", "deploy", "-n", namespace, deployName, "--replicas=0")
		if err != nil {
			return fmt.Errorf("failed to scale down deployment: %w", err)
		}
		_, err = Kubectl("wait", "--for=delete", "pod", "-n", namespace, "--selector=app="+deployName)
		if err != nil {
			return fmt.Errorf("failed to wait for pod deletion: %w", err)
		}
	}
	stdout, err := Rbd("snap", "rollback", pool+"/"+image+"@"+snap)
	if err != nil {
		return fmt.Errorf("failed to rollback snapshot: %w, %s", err, string(stdout))
	}
	if len(deployName) != 0 {
		_, err := Kubectl("scale", "deploy", "-n", namespace, deployName, "--replicas=1")
		if err != nil {
			return fmt.Errorf("failed to scale up deployment: %w", err)
		}
		_, err = Kubectl("wait", "--for=condition=available", "deploy", "-n", namespace, deployName)
		if err != nil {
			return fmt.Errorf("failed to wait for deployment: %w", err)
		}
	}
	return nil
}
