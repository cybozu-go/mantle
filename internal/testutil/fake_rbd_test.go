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

		By("checking removing a snapshot succeeds")
		err = f.RBDSnapRm("pool", "image", "snap1")
		Expect(err).ToNot(HaveOccurred())
		snaps, err = f.RBDSnapLs("pool", "image")
		Expect(err).ToNot(HaveOccurred())
		Expect(snaps).To(ConsistOf(
			HaveField("Name", "snap2"),
		))

		By("checking removing the same snapshot fails")
		err = f.RBDSnapRm("pool", "image", "snap1")
		Expect(err).To(HaveOccurred())

		By("checking removing another snapshot succeeds")
		err = f.RBDSnapRm("pool", "image", "snap2")
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
})
