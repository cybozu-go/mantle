package ceph

import (
	"encoding/json"
	"fmt"
)

// RBDClone clones an RBD image from a snapshot with specified features.
func (c *cephCmdImpl) RBDClone(pool, srcImage, srcSnap, dstImage, features string) error {
	src := fmt.Sprintf("%s/%s@%s", pool, srcImage, srcSnap)
	dst := fmt.Sprintf("%s/%s", pool, dstImage)
	_, err := c.command.execute("rbd", "clone",
		"--rbd-default-clone-format", "2",
		"--image-feature", features,
		src, dst,
	)
	if err != nil {
		return fmt.Errorf("failed to clone RBD image: %v", err)
	}

	return nil
}

// RBDInfo gets information about an RBD image.
func (c *cephCmdImpl) RBDInfo(pool, image string) (*RBDImageInfo, error) {
	out, err := c.command.execute("rbd", "info", "--format", "json", fmt.Sprintf("%s/%s", pool, image))
	if err != nil {
		return nil, fmt.Errorf("failed to get RBD info: %v", err)
	}

	imageInfo := &RBDImageInfo{}
	err = json.Unmarshal(out, imageInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal RBD info: %v", err)
	}
	return imageInfo, nil
}

// RBDLs lists RBD images in a pool.
func (c *cephCmdImpl) RBDLs(pool string) ([]string, error) {
	out, err := c.command.execute("rbd", "ls", "-p", pool, "--format", "json")
	if err != nil {
		return nil, fmt.Errorf("failed to list RBD images: %v", err)
	}

	var images []string
	err = json.Unmarshal(out, &images)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal RBD images: %v", err)
	}

	return images, nil
}

// RBDRm removes an RBD image.
func (c *cephCmdImpl) RBDRm(pool, image string) error {
	_, err := c.command.execute("rbd", "rm", fmt.Sprintf("%s/%s", pool, image))
	if err != nil {
		return fmt.Errorf("failed to remove RBD image: %v", err)
	}

	return nil
}

// RBDTrashMv removes an RBD image asynchronously.
func (c *cephCmdImpl) RBDTrashMv(pool, image string) error {
	_, err := c.command.execute("rbd", "trash", "mv", fmt.Sprintf("%s/%s", pool, image))
	if err != nil {
		return fmt.Errorf("failed to move RBD image to trash: %w", err)
	}
	return nil
}

// CephRBDTaskTrashRemove adds a task to remove the image from trash.
func (c *cephCmdImpl) CephRBDTaskAddTrashRemove(pool, imageID string) error {
	_, err := c.command.execute("ceph", "rbd", "task", "add", "trash", "remove", fmt.Sprintf("%s/%s", pool, imageID))
	if err != nil {
		return fmt.Errorf("failed to add task to remove the image from trash: %w", err)
	}
	return nil
}

// RBDSnapCreate creates an RBD snapshot.
func (c *cephCmdImpl) RBDSnapCreate(pool, image, snap string) error {
	_, err := c.command.execute("rbd", "snap", "create", fmt.Sprintf("%s/%s@%s", pool, image, snap))
	if err != nil {
		return fmt.Errorf("failed to create RBD snapshot: %v", err)
	}

	return nil
}

// RBDSnapLs lists RBD snapshots of an image.
func (c *cephCmdImpl) RBDSnapLs(pool, image string) ([]RBDSnapshot, error) {
	out, err := c.command.execute("rbd", "snap", "ls", "--format", "json", fmt.Sprintf("%s/%s", pool, image))
	if err != nil {
		return nil, fmt.Errorf("failed to list RBD snapshots: %v", err)
	}

	var snapshots []RBDSnapshot
	err = json.Unmarshal(out, &snapshots)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal RBD snapshots: %v", err)
	}

	return snapshots, nil
}

// RBDSnapRm removes an RBD snapshot.
func (c *cephCmdImpl) RBDSnapRm(pool, image, snap string) error {
	_, err := c.command.execute("rbd", "snap", "rm", fmt.Sprintf("%s/%s@%s", pool, image, snap))
	if err != nil {
		return fmt.Errorf("failed to remove RBD snapshot: %v", err)
	}

	return nil
}
