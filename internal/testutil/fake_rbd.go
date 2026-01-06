package testutil

import (
	"fmt"
	"math/rand"
	"slices"
	"time"

	"github.com/cybozu-go/mantle/internal/ceph"
)

const FakeRBDSnapshotSize = 5368709120 // 5Gi

type FakeRBD struct {
	// if this is set, all methods return this error
	err error
	// key: pool/image
	locks      map[string][]*ceph.RBDLock
	nextSnapId int
	snapshots  map[string][]ceph.RBDSnapshot
}

var _ ceph.CephCmd = &FakeRBD{}

func NewFakeRBD() *FakeRBD {
	return &FakeRBD{
		locks:      make(map[string][]*ceph.RBDLock),
		nextSnapId: 0,
		snapshots:  make(map[string][]ceph.RBDSnapshot),
	}
}

func (f *FakeRBD) SetError(err error) {
	f.err = err
}

func (f *FakeRBD) RBDClone(pool, srcImage, srcSnap, dstImage, features string) error {
	if f.err != nil {
		return f.err
	}

	return nil
}

func (f *FakeRBD) RBDInfo(pool, image string) (*ceph.RBDImageInfo, error) {
	if f.err != nil {
		return nil, f.err
	}

	return nil, nil
}

func (f *FakeRBD) RBDLs(pool string) ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}

	return nil, nil
}

func (f *FakeRBD) RBDLockAdd(pool, image, lockID string) error {
	if f.err != nil {
		return f.err
	}

	key := pool + "/" + image
	if f.locks[key] == nil {
		f.locks[key] = []*ceph.RBDLock{}
	}

	if len(f.locks[key]) > 0 {
		return fmt.Errorf("lock already exists: %s", lockID)
	}

	f.locks[key] = append(f.locks[key], &ceph.RBDLock{
		LockID: lockID,
		Locker: fmt.Sprintf("client:%d", rand.Int63()), // ignore collision for test
		Address: fmt.Sprintf("%d,%d,%d,%d:%d/%d",
			rand.Int31n(256), rand.Int31n(256), rand.Int31n(256), rand.Int31n(256),
			rand.Int31n(65536), rand.Int63()),
	})

	return nil
}

func (f *FakeRBD) RBDLockLs(pool, image string) ([]*ceph.RBDLock, error) {
	if f.err != nil {
		return nil, f.err
	}

	key := pool + "/" + image

	return f.locks[key], nil
}

func (f *FakeRBD) RBDLockRm(pool, image string, lock *ceph.RBDLock) error {
	if f.err != nil {
		return f.err
	}

	key := pool + "/" + image
	for _, l := range f.locks[key] {
		if l.LockID == lock.LockID && l.Locker == lock.Locker {
			f.locks[key] = slices.DeleteFunc(f.locks[key], func(r *ceph.RBDLock) bool {
				return r.LockID == lock.LockID && r.Locker == lock.Locker
			})

			return nil
		}
	}

	return fmt.Errorf("lock not found: %s", lock.LockID)
}

func (f *FakeRBD) RBDRm(pool, image string) error {
	if f.err != nil {
		return f.err
	}

	return nil
}

func (f *FakeRBD) RBDTrashMv(pool, image string) error {
	if f.err != nil {
		return f.err
	}

	return nil
}

func (f *FakeRBD) CephRBDTaskAddTrashRemove(pool, image string) error {
	if f.err != nil {
		return f.err
	}

	return nil
}

func (f *FakeRBD) RBDSnapCreate(pool, image, snap string) error {
	if f.err != nil {
		return f.err
	}

	key := pool + "/" + image
	snaps := f.snapshots[key]

	if slices.ContainsFunc(snaps, ceph.IsRBDSnapshotNamed(snap)) {
		return fmt.Errorf("snap already exists: %s@%s", key, snap)
	}

	f.snapshots[key] = append(snaps, ceph.RBDSnapshot{
		Id:        f.nextSnapId,
		Name:      snap,
		Size:      FakeRBDSnapshotSize,
		Protected: false,
		Timestamp: ceph.NewRBDTimeStamp(time.Now().UTC()),
	})

	f.nextSnapId++

	return nil
}

func (f *FakeRBD) RBDSnapLs(pool, image string) ([]ceph.RBDSnapshot, error) {
	if f.err != nil {
		return nil, f.err
	}

	return f.snapshots[pool+"/"+image], nil
}

func (f *FakeRBD) RBDSnapRm(pool, image, snap string) error {
	if f.err != nil {
		return f.err
	}

	key := pool + "/" + image
	snaps := f.snapshots[key]

	if !slices.ContainsFunc(snaps, ceph.IsRBDSnapshotNamed(snap)) {
		return fmt.Errorf("snap not found: %s@%s", key, snap)
	}

	f.snapshots[key] = slices.DeleteFunc(snaps, ceph.IsRBDSnapshotNamed(snap))

	return nil
}
