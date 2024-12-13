package testutil

import (
	"fmt"
	"slices"
	"time"

	"github.com/cybozu-go/mantle/internal/ceph"
)

type fakeRBD struct {
	nextSnapId int
	snapshots  map[string][]ceph.RBDSnapshot
}

var _ ceph.CephCmd = &fakeRBD{}

func NewFakeRBD() ceph.CephCmd {
	return &fakeRBD{
		nextSnapId: 0,
		snapshots:  make(map[string][]ceph.RBDSnapshot),
	}
}

func (f *fakeRBD) RBDClone(pool, srcImage, srcSnap, dstImage, features string) error {
	return nil
}

func (f *fakeRBD) RBDInfo(pool, image string) (*ceph.RBDImageInfo, error) {
	return nil, nil
}

func (f *fakeRBD) RBDLs(pool string) ([]string, error) {
	return nil, nil
}

func (f *fakeRBD) RBDRm(pool, image string) error {
	return nil
}

func (f *fakeRBD) RBDTrashMv(pool, image string) error {
	return nil
}

func (f *fakeRBD) CephRBDTaskAddTrashRemove(pool, image string) error {
	return nil
}

func (f *fakeRBD) RBDSnapCreate(pool, image, snap string) error {
	key := pool + "/" + image

	snaps := f.snapshots[key]

	if slices.ContainsFunc(snaps, ceph.IsRBDSnapshotNamed(snap)) {
		return fmt.Errorf("snap already exists: %s@%s", key, snap)
	}

	f.snapshots[key] = append(snaps, ceph.RBDSnapshot{
		Id:        f.nextSnapId,
		Name:      snap,
		Size:      10,
		Protected: false,
		Timestamp: ceph.NewRBDTimeStamp(time.Now().UTC()),
	})

	f.nextSnapId++

	return nil
}

func (f *fakeRBD) RBDSnapLs(pool, image string) ([]ceph.RBDSnapshot, error) {
	return f.snapshots[pool+"/"+image], nil
}

func (f *fakeRBD) RBDSnapRm(pool, image, snap string) error {
	key := pool + "/" + image

	snaps := f.snapshots[key]

	if !slices.ContainsFunc(snaps, ceph.IsRBDSnapshotNamed(snap)) {
		return fmt.Errorf("snap not found: %s@%s", key, snap)
	}

	f.snapshots[key] = slices.DeleteFunc(snaps, ceph.IsRBDSnapshotNamed(snap))

	return nil
}
