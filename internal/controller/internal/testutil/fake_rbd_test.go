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
})
