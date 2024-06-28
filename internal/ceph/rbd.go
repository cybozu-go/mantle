package ceph

import (
	"encoding/json"
	"fmt"
)

type rbdInfoParentJS struct {
	Pool     string `json:"pool"`
	Image    string `json:"image"`
	Snapshot string `json:"snapshot"`
}

type rbdInfoJS struct {
	Parent *rbdInfoParentJS `json:"parent,omitempty"`
}

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
func (c *cephCmdImpl) RBDInfo(pool, image string) (*RBDInfo, error) {
	out, err := c.command.execute("rbd", "info", "--format", "json", fmt.Sprintf("%s/%s", pool, image))
	if err != nil {
		return nil, fmt.Errorf("failed to get RBD info: %v", err)
	}

	infoJS := &rbdInfoJS{}
	err = json.Unmarshal(out, infoJS)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal RBD info: %v", err)
	}

	if infoJS.Parent == nil {
		return nil, fmt.Errorf("RBD info parent field is empty")
	}
	info := &RBDInfo{
		ParentPool:  infoJS.Parent.Pool,
		ParentImage: infoJS.Parent.Image,
		ParentSnap:  infoJS.Parent.Snapshot,
	}

	return info, nil
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
