package testutil

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("FakeRBD", func() {
	It("should create/list/rm a snapshot", func() {
		f := NewFakeRBD()

		By("checking creating a snapshot succeeds")
		err := f.RBDSnapCreate("pool", "image", "snap1")
		Expect(err).ToNot(HaveOccurred())

		By("checking creating the same snapshot fails")
		err = f.RBDSnapCreate("pool", "image", "snap1")
		Expect(err).To(HaveOccurred())

		By("checking creating another snapshot succeeds")
		err = f.RBDSnapCreate("pool", "image", "snap2")
		Expect(err).ToNot(HaveOccurred())

		By("checking listing snapshots succeeds")
		snaps, err := f.RBDSnapLs("pool", "image")
		Expect(err).ToNot(HaveOccurred())
		Expect(snaps).To(ConsistOf(
			HaveField("Name", "snap1"),
			HaveField("Name", "snap2"),
		))

		By("checking getting the image ID succeeds")
		info, err := f.RBDInfo("pool", "image")
		Expect(err).ToNot(HaveOccurred())
		imageID := info.ID

		By("checking removing a snapshot succeeds")
		err = f.RBDSnapRm("pool", imageID, "snap1")
		Expect(err).ToNot(HaveOccurred())
		snaps, err = f.RBDSnapLs("pool", "image")
		Expect(err).ToNot(HaveOccurred())
		Expect(snaps).To(ConsistOf(
			HaveField("Name", "snap2"),
		))

		By("checking removing the same snapshot fails")
		err = f.RBDSnapRm("pool", imageID, "snap1")
		Expect(err).To(HaveOccurred())

		By("checking removing another snapshot succeeds")
		err = f.RBDSnapRm("pool", imageID, "snap2")
		Expect(err).ToNot(HaveOccurred())
	})

	It("should lock/unlock a volume", func() {
		f := NewFakeRBD()

		By("checking adding a lock succeeds")
		err := f.RBDLockAdd("pool", "image1", "lock1")
		Expect(err).ToNot(HaveOccurred())

		By("checking adding the same lock fails")
		err = f.RBDLockAdd("pool", "image1", "lock1")
		Expect(err).To(HaveOccurred())

		By("checking adding a different lock fails")
		err = f.RBDLockAdd("pool", "image1", "lock2")
		Expect(err).To(HaveOccurred())

		By("checking adding another lock succeeds")
		err = f.RBDLockAdd("pool", "image2", "lock1")
		Expect(err).ToNot(HaveOccurred())

		By("checking listing locks succeeds")
		locks1, err := f.RBDLockLs("pool", "image1")
		Expect(err).ToNot(HaveOccurred())
		Expect(locks1).To(HaveLen(1))
		Expect(locks1).To(ConsistOf(
			HaveField("LockID", "lock1"),
		))
		locks2, err := f.RBDLockLs("pool", "image2")
		Expect(err).ToNot(HaveOccurred())
		Expect(locks2).To(HaveLen(1))
		Expect(locks2).To(ConsistOf(
			HaveField("LockID", "lock1"),
		))

		By("checking removing a lock succeeds")
		err = f.RBDLockRm("pool", "image1", locks1[0])
		Expect(err).ToNot(HaveOccurred())
		locks1, err = f.RBDLockLs("pool", "image1")
		Expect(err).ToNot(HaveOccurred())
		Expect(locks1).To(BeEmpty())
	})

	It("should list images that have snapshots", func() {
		f := NewFakeRBD()

		By("checking RBDLs returns empty before any snapshot is created")
		images, err := f.RBDLs("pool")
		Expect(err).ToNot(HaveOccurred())
		Expect(images).To(BeEmpty())

		By("checking RBDLs returns the image after a snapshot is created")
		err = f.RBDSnapCreate("pool", "image1", "snap1")
		Expect(err).ToNot(HaveOccurred())
		images, err = f.RBDLs("pool")
		Expect(err).ToNot(HaveOccurred())
		Expect(images).To(ConsistOf("image1"))

		By("checking RBDLs returns multiple images")
		err = f.RBDSnapCreate("pool", "image2", "snap1")
		Expect(err).ToNot(HaveOccurred())
		images, err = f.RBDLs("pool")
		Expect(err).ToNot(HaveOccurred())
		Expect(images).To(ConsistOf("image1", "image2"))

		By("checking RBDLs does not return images from another pool")
		images, err = f.RBDLs("other-pool")
		Expect(err).ToNot(HaveOccurred())
		Expect(images).To(BeEmpty())
	})

	It("should list snapshots by image ID", func() {
		f := NewFakeRBD()

		By("checking RBDSnapLsByID returns empty for unknown imageID")
		snaps, err := f.RBDSnapLsByID("pool", "unknown-id")
		Expect(err).ToNot(HaveOccurred())
		Expect(snaps).To(BeEmpty())

		By("checking RBDSnapLsByID returns snapshots for a known imageID")
		err = f.RBDSnapCreate("pool", "image", "snap1")
		Expect(err).ToNot(HaveOccurred())
		err = f.RBDSnapCreate("pool", "image", "snap2")
		Expect(err).ToNot(HaveOccurred())
		info, err := f.RBDInfo("pool", "image")
		Expect(err).ToNot(HaveOccurred())
		snaps, err = f.RBDSnapLsByID("pool", info.ID)
		Expect(err).ToNot(HaveOccurred())
		Expect(snaps).To(ConsistOf(
			HaveField("Name", "snap1"),
			HaveField("Name", "snap2"),
		))
	})

	It("should move an image to trash and reflect the change in listings", func() {
		f := NewFakeRBD()

		By("creating an image with a snapshot")
		err := f.RBDSnapCreate("pool", "image", "snap1")
		Expect(err).ToNot(HaveOccurred())
		info, err := f.RBDInfo("pool", "image")
		Expect(err).ToNot(HaveOccurred())
		imageID := info.ID

		By("checking the image appears in RBDLs and not in RBDTrashLs before trash")
		images, err := f.RBDLs("pool")
		Expect(err).ToNot(HaveOccurred())
		Expect(images).To(ConsistOf("image"))
		trashItems, err := f.RBDTrashLs("pool")
		Expect(err).ToNot(HaveOccurred())
		Expect(trashItems).To(BeEmpty())

		By("moving the image to trash")
		err = f.RBDTrashMv("pool", "image")
		Expect(err).ToNot(HaveOccurred())

		By("checking the image disappears from RBDLs after trash")
		images, err = f.RBDLs("pool")
		Expect(err).ToNot(HaveOccurred())
		Expect(images).To(BeEmpty())

		By("checking the image appears in RBDTrashLs with correct ID and name")
		trashItems, err = f.RBDTrashLs("pool")
		Expect(err).ToNot(HaveOccurred())
		Expect(trashItems).To(ConsistOf(
			And(HaveField("ID", imageID), HaveField("Name", "image")),
		))

		By("checking RBDInfo fails for the trashed image")
		_, err = f.RBDInfo("pool", "image")
		Expect(err).To(HaveOccurred())

		By("checking RBDTrashLs does not return images from another pool")
		trashItems, err = f.RBDTrashLs("other-pool")
		Expect(err).ToNot(HaveOccurred())
		Expect(trashItems).To(BeEmpty())
	})

	It("should allow snapshot operations on a trashed image via imageID", func() {
		f := NewFakeRBD()

		By("creating an image with a snapshot and moving it to trash")
		err := f.RBDSnapCreate("pool", "image", "snap1")
		Expect(err).ToNot(HaveOccurred())
		info, err := f.RBDInfo("pool", "image")
		Expect(err).ToNot(HaveOccurred())
		imageID := info.ID
		err = f.RBDTrashMv("pool", "image")
		Expect(err).ToNot(HaveOccurred())

		By("checking RBDSnapLsByID still returns the snapshot after trash")
		snaps, err := f.RBDSnapLsByID("pool", imageID)
		Expect(err).ToNot(HaveOccurred())
		Expect(snaps).To(ConsistOf(HaveField("Name", "snap1")))

		By("checking RBDSnapRm removes the snapshot on the trashed image via imageID")
		err = f.RBDSnapRm("pool", imageID, "snap1")
		Expect(err).ToNot(HaveOccurred())

		By("checking the snapshot is gone after removal")
		snaps, err = f.RBDSnapLsByID("pool", imageID)
		Expect(err).ToNot(HaveOccurred())
		Expect(snaps).To(BeEmpty())
	})
})
