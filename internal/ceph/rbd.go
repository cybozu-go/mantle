package ceph

import (
	"encoding/json"
	"fmt"
)

// RBDClone clones an RBD image from a snapshot with specified features.
func (c *cephCmdImpl) RBDClone(pool, srcImage, srcSnap, dstImage, features string) error {
	src := fmt.Sprintf("%s/%s@%s", pool, srcImage, srcSnap)
	dst := fmt.Sprintf("%s/%s", pool, dstImage)
	_, stderr, err := c.command.execute("rbd", "clone",
		"--rbd-default-clone-format", "2",
		"--image-feature", features,
		src, dst,
	)
	if err != nil {
		return fmt.Errorf("failed to clone RBD image: %w, stderr: %s", err, string(stderr))
	}

	return nil
}

// RBDInfo gets information about an RBD image.
func (c *cephCmdImpl) RBDInfo(pool, image string) (*RBDImageInfo, error) {
	stdout, stderr, err := c.command.execute("rbd", "info", "--format", "json", fmt.Sprintf("%s/%s", pool, image))
	if err != nil {
		return nil, fmt.Errorf("failed to get RBD info: %w, stderr: %s", err, string(stderr))
	}

	imageInfo := &RBDImageInfo{}
	err = json.Unmarshal(stdout, imageInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal RBD info: %w, stdout: %s", err, stdout)
	}

	return imageInfo, nil
}

// RBDLs lists RBD images in a pool.
func (c *cephCmdImpl) RBDLs(pool string) ([]string, error) {
	stdout, stderr, err := c.command.execute("rbd", "ls", "-p", pool, "--format", "json")
	if err != nil {
		return nil, fmt.Errorf("failed to list RBD images: %w, stderr: %s", err, string(stderr))
	}

	var images []string
	err = json.Unmarshal(stdout, &images)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal RBD images: %w, stdout: %s", err, stdout)
	}

	return images, nil
}

// RBDLockAdd adds a lock to an RBD image.
func (c *cephCmdImpl) RBDLockAdd(pool, image, lockID string) error {
	_, stderr, err := c.command.execute("rbd", "-p", pool, "lock", "add", image, lockID)
	if err != nil {
		return fmt.Errorf("failed to add lock to RBD image: %w, stderr: %s", err, string(stderr))
	}

	return nil
}

// RBDLockLs lists locks on an RBD image.
func (c *cephCmdImpl) RBDLockLs(pool, image string) ([]*RBDLock, error) {
	stdout, stderr, err := c.command.execute("rbd", "-p", pool, "--format", "json", "lock", "ls", image)
	if err != nil {
		return nil, fmt.Errorf("failed to list locks on RBD image: %w, stderr: %s", err, string(stderr))
	}

	var locks []*RBDLock
	err = json.Unmarshal(stdout, &locks)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal RBD locks: %w, stdout: %s", err, stdout)
	}

	return locks, nil
}

// RBDLockRm releases a lock from an RBD image.
func (c *cephCmdImpl) RBDLockRm(pool, image string, lock *RBDLock) error {
	_, stderr, err := c.command.execute("rbd", "-p", pool, "lock", "rm", image, lock.LockID, lock.Locker)
	if err != nil {
		return fmt.Errorf("failed to remove lock from RBD image: %w, stderr: %s", err, string(stderr))
	}

	return nil
}

// RBDRm removes an RBD image.
func (c *cephCmdImpl) RBDRm(pool, image string) error {
	_, stderr, err := c.command.execute("rbd", "rm", fmt.Sprintf("%s/%s", pool, image))
	if err != nil {
		return fmt.Errorf("failed to remove RBD image: %w, stderr: %s", err, string(stderr))
	}

	return nil
}

// RBDTrashMv removes an RBD image asynchronously.
func (c *cephCmdImpl) RBDTrashMv(pool, image string) error {
	_, stderr, err := c.command.execute("rbd", "trash", "mv", fmt.Sprintf("%s/%s", pool, image))
	if err != nil {
		return fmt.Errorf("failed to move RBD image to trash: %w, stderr: %s", err, string(stderr))
	}

	return nil
}

// CephRBDTaskTrashRemove adds a task to remove the image from trash.
func (c *cephCmdImpl) CephRBDTaskAddTrashRemove(pool, imageID string) error {
	_, stderr, err := c.command.execute("ceph", "rbd", "task", "add", "trash", "remove", fmt.Sprintf("%s/%s", pool, imageID))
	if err != nil {
		return fmt.Errorf("failed to add task to remove the image from trash: %w, stderr: %s", err, string(stderr))
	}

	return nil
}

// RBDSnapCreate creates an RBD snapshot.
func (c *cephCmdImpl) RBDSnapCreate(pool, image, snap string) error {
	_, stderr, err := c.command.execute("rbd", "snap", "create", fmt.Sprintf("%s/%s@%s", pool, image, snap))
	if err != nil {
		return fmt.Errorf("failed to create RBD snapshot: %w, stderr: %s", err, string(stderr))
	}

	return nil
}

// RBDSnapLs lists RBD snapshots of an image.
func (c *cephCmdImpl) RBDSnapLs(pool, image string) ([]RBDSnapshot, error) {
	stdout, stderr, err := c.command.execute("rbd", "snap", "ls", "--format", "json", fmt.Sprintf("%s/%s", pool, image))
	if err != nil {
		return nil, fmt.Errorf("failed to list RBD snapshots: %w, stderr: %s", err, string(stderr))
	}

	var snapshots []RBDSnapshot
	err = json.Unmarshal(stdout, &snapshots)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal RBD snapshots: %w, stdout: %s", err, stdout)
	}

	return snapshots, nil
}

// RBDSnapRm removes an RBD snapshot.
func (c *cephCmdImpl) RBDSnapRm(pool, image, snap string) error {
	_, stderr, err := c.command.execute("rbd", "snap", "rm", fmt.Sprintf("%s/%s@%s", pool, image, snap))
	if err != nil {
		return fmt.Errorf("failed to remove RBD snapshot: %w, stderr: %s", err, string(stderr))
	}

	return nil
}
