package testutil

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"time"

	"github.com/cybozu-go/mantle/internal/ceph"
)

const FakeRBDSnapshotSize = 5368709120 // 5Gi

// fakeImage holds all state for a single RBD image in FakeRBD.
type fakeImage struct {
	name    string
	id      string
	trashed bool
	snaps   []ceph.RBDSnapshot
}

type FakeRBD struct {
	// if this is set, all methods return this error
	err error
	// key: pool/image
	locks      map[string][]*ceph.RBDLock
	nextSnapId int
	// key: pool/imageName → *fakeImage
	images map[string]*fakeImage
}

var _ ceph.CephCmd = &FakeRBD{}

func NewFakeRBD() *FakeRBD {
	return &FakeRBD{
		locks:  make(map[string][]*ceph.RBDLock),
		images: make(map[string]*fakeImage),
	}
}

func (f *FakeRBD) SetError(err error) {
	f.err = err
}

// ensureImage returns the fakeImage for pool/name, creating it if not yet registered.
func (f *FakeRBD) ensureImage(pool, name string) *fakeImage {
	key := pool + "/" + name
	if img, ok := f.images[key]; ok {
		return img
	}
	img := &fakeImage{
		name: name,
		id:   "id-" + name,
	}
	f.images[key] = img

	return img
}

// findImageByID returns the fakeImage whose ID matches imageID in the given pool, or nil.
func (f *FakeRBD) findImageByID(pool, imageID string) *fakeImage {
	prefix := pool + "/"
	for key, img := range f.images {
		if strings.HasPrefix(key, prefix) && img.id == imageID {
			return img
		}
	}

	return nil
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

	img, ok := f.images[pool+"/"+image]
	if !ok || img.trashed {
		return nil, fmt.Errorf("image not found: %s/%s", pool, image)
	}

	return &ceph.RBDImageInfo{ID: img.id}, nil
}

func (f *FakeRBD) RBDLs(pool string) ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}

	prefix := pool + "/"
	var names []string
	for key, img := range f.images {
		if strings.HasPrefix(key, prefix) && !img.trashed {
			names = append(names, img.name)
		}
	}

	return names, nil
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

	return f.locks[pool+"/"+image], nil
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

	img, ok := f.images[pool+"/"+image]
	if !ok {
		// Image not tracked (e.g., created via RBDClone which is a no-op); nothing to move.
		return nil
	}

	img.trashed = true

	return nil
}

func (f *FakeRBD) RBDTrashLs(pool string) ([]*ceph.RBDTrashInfo, error) {
	if f.err != nil {
		return nil, f.err
	}

	prefix := pool + "/"
	var items []*ceph.RBDTrashInfo
	for key, img := range f.images {
		if strings.HasPrefix(key, prefix) && img.trashed {
			items = append(items, &ceph.RBDTrashInfo{
				ID:   img.id,
				Name: img.name,
			})
		}
	}

	return items, nil
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

	img := f.ensureImage(pool, image)

	if slices.ContainsFunc(img.snaps, ceph.IsRBDSnapshotNamed(snap)) {
		return fmt.Errorf("snap already exists: %s/%s@%s", pool, image, snap)
	}

	img.snaps = append(img.snaps, ceph.RBDSnapshot{
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

	img, ok := f.images[pool+"/"+image]
	if !ok {
		return nil, nil
	}

	return img.snaps, nil
}

func (f *FakeRBD) RBDSnapLsByID(pool, imageID string) ([]ceph.RBDSnapshot, error) {
	if f.err != nil {
		return nil, f.err
	}

	img := f.findImageByID(pool, imageID)
	if img == nil {
		return nil, nil
	}

	return img.snaps, nil
}

func (f *FakeRBD) RBDSnapRm(pool, imageID, snap string) error {
	if f.err != nil {
		return f.err
	}

	img := f.findImageByID(pool, imageID)
	if img == nil {
		return fmt.Errorf("image not found by ID: %s/%s", pool, imageID)
	}

	if !slices.ContainsFunc(img.snaps, ceph.IsRBDSnapshotNamed(snap)) {
		return fmt.Errorf("snap not found: %s/%s@%s", pool, img.name, snap)
	}

	img.snaps = slices.DeleteFunc(img.snaps, ceph.IsRBDSnapshotNamed(snap))

	return nil
}
