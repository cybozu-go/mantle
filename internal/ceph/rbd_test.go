package ceph

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	gomock "go.uber.org/mock/gomock"
)

type reporter struct{}

func (g reporter) Errorf(format string, args ...any) {
	Fail(fmt.Sprintf(format, args...))
}

func (g reporter) Fatalf(format string, args ...any) {
	Fail(fmt.Sprintf(format, args...))
}

func mockedCephCmd(m *Mockcommand) CephCmd {
	return &cephCmdImpl{
		command: m,
	}
}

var _ = Describe("CephCmd.RBDClone", func() {
	var t reporter

	It("should run without error", func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := NewMockcommand(ctrl)
		m.EXPECT().execute("rbd", "clone",
			"--rbd-default-clone-format", "2",
			"--image-feature", "feature",
			"pool/srcImage@srcSnap",
			"pool/dstImage").Return([]byte{}, nil)

		cmd := mockedCephCmd(m)
		err := cmd.RBDClone("pool", "srcImage", "srcSnap", "dstImage", "feature")
		Expect(err).ToNot(HaveOccurred())
	})

	It("should return an error, if the command failed", func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := NewMockcommand(ctrl)
		m.EXPECT().execute(gomock.Any()).Return([]byte{}, fmt.Errorf("error"))

		cmd := mockedCephCmd(m)
		err := cmd.RBDClone("pool", "srcImage", "srcSnap", "dstImage", "feature")
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("CephCmd.RBDInfo", func() {
	var t reporter

	It("should return the correct RBDInfo", func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := NewMockcommand(ctrl)
		m.EXPECT().execute("rbd", "info", "--format", "json", "pool/image").Return([]byte(`{
			"name": "csi-vol-32492735-a048-43f1-a576-28e807ec0a30",
			"id": "60dd8f9270ba",
			"size": 5368709120,
			"objects": 1280,
			"order": 22,
			"object_size": 4194304,
			"snapshot_count": 0,
			"block_name_prefix": "rbd_data.60dd8f9270ba",
			"format": 2,
			"features": [
				"layering"
			],
			"op_features": [],
			"flags": [],
			"create_timestamp": "Tue May 28 06:54:51 2024",
			"access_timestamp": "Tue May 28 06:54:51 2024",
			"modify_timestamp": "Tue May 28 06:54:51 2024",
			"parent": {
				"pool": "pool",
				"pool_namespace": "",
				"image": "csi-vol-39ca122a-88e1-44b6-aa2b-cae64fb383db",
				"id": "60dd8f9270bc",
				"snapshot": "test-snap",
				"trash": false,
				"overlap": 5368709120
			}
		}`), nil)

		cmd := mockedCephCmd(m)
		info, err := cmd.RBDInfo("pool", "image")
		Expect(err).ToNot(HaveOccurred())
		Expect(info.Parent).NotTo(BeNil())
		Expect(info.Parent.Pool).To(Equal("pool"))
		Expect(info.Parent.Image).To(Equal("csi-vol-39ca122a-88e1-44b6-aa2b-cae64fb383db"))
		Expect(info.Parent.Snapshot).To(Equal("test-snap"))
	})

	It("should return an error, if the command failed", func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := NewMockcommand(ctrl)
		m.EXPECT().execute(gomock.Any()).Return([]byte{}, fmt.Errorf("error"))

		cmd := mockedCephCmd(m)
		_, err := cmd.RBDInfo("pool", "image")
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("CephCmd.RBDLs", func() {
	var t reporter

	It("should return the correct list of images", func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := NewMockcommand(ctrl)
		m.EXPECT().execute("rbd", "ls", "-p", "pool", "--format", "json").Return([]byte(`[
			"csi-vol-d2556cc0-5ba6-4e70-b966-56522225bdc5",
			"csi-vol-f2a83492-3cf2-48e7-8500-804754c42ce6"
		]`), nil)

		cmd := mockedCephCmd(m)
		images, err := cmd.RBDLs("pool")
		Expect(err).ToNot(HaveOccurred())
		Expect(images).To(HaveLen(2))
		Expect(images[0]).To(Equal("csi-vol-d2556cc0-5ba6-4e70-b966-56522225bdc5"))
		Expect(images[1]).To(Equal("csi-vol-f2a83492-3cf2-48e7-8500-804754c42ce6"))
	})

	It("should return empty list, if there are no images", func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := NewMockcommand(ctrl)
		m.EXPECT().execute("rbd", "ls", "-p", "pool", "--format", "json").Return([]byte(`[]`), nil)

		cmd := mockedCephCmd(m)
		images, err := cmd.RBDLs("pool")
		Expect(err).ToNot(HaveOccurred())
		Expect(images).To(HaveLen(0))
	})

	It("should return an error, if the command failed", func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := NewMockcommand(ctrl)
		m.EXPECT().execute(gomock.Any()).Return([]byte{}, fmt.Errorf("error"))

		cmd := mockedCephCmd(m)
		_, err := cmd.RBDLs("pool")
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("CephCmd.RBDRm", func() {
	var t reporter

	It("should run without error", func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := NewMockcommand(ctrl)
		m.EXPECT().execute("rbd", "rm", "pool/image").Return([]byte{}, nil)

		cmd := mockedCephCmd(m)
		err := cmd.RBDRm("pool", "image")
		Expect(err).ToNot(HaveOccurred())
	})

	It("should return an error, if the command failed", func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := NewMockcommand(ctrl)
		m.EXPECT().execute(gomock.Any()).Return([]byte{}, fmt.Errorf("error"))

		cmd := mockedCephCmd(m)
		err := cmd.RBDRm("pool", "image")
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("CephCmd.RBDSnapCreate", func() {
	var t reporter

	It("should run without error", func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := NewMockcommand(ctrl)
		m.EXPECT().
			execute("rbd", "snap", "create", "pool/image@snap").
			Return([]byte{}, nil)

		cmd := mockedCephCmd(m)
		err := cmd.RBDSnapCreate("pool", "image", "snap")
		Expect(err).ToNot(HaveOccurred())
	})

	It("should return an error, if the command failed", func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := NewMockcommand(ctrl)
		m.EXPECT().execute(gomock.Any()).Return([]byte{}, fmt.Errorf("error"))

		cmd := mockedCephCmd(m)
		err := cmd.RBDSnapCreate("pool", "image", "snap")
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("CephCmd.RBDSnapLs", func() {
	var t reporter

	It("should return the correct RBDSnapshot", func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := NewMockcommand(ctrl)
		m.EXPECT().
			execute("rbd", "snap", "ls", "--format", "json", "pool/image").
			Return([]byte(`
		[{"id":4,"name":"test","size":10737418240,"protected":"false","timestamp":"Tue Oct  1 10:11:31 2024"}]
		`), nil)

		cmd := mockedCephCmd(m)
		snaps, err := cmd.RBDSnapLs("pool", "image")
		Expect(err).ToNot(HaveOccurred())
		Expect(snaps).To(HaveLen(1))
		snap := snaps[0]
		Expect(snap.Id).To(Equal(int(4)))
		Expect(snap.Name).To(Equal("test"))
		Expect(snap.Size).To(Equal(int64(10737418240)))
		Expect(snap.Protected).To(Equal(false))
		Expect(snap.Timestamp).To(Equal(NewRBDTimeStamp(time.Date(2024, 10, 1, 10, 11, 31, 0, time.UTC))))
	})

	It("should return an error, if the command failed", func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := NewMockcommand(ctrl)
		m.EXPECT().execute(gomock.Any()).Return([]byte{}, fmt.Errorf("error"))

		cmd := mockedCephCmd(m)
		_, err := cmd.RBDSnapLs("pool", "image")
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("CephCmd.RBDSnapRm", func() {
	var t reporter

	It("should run without error", func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := NewMockcommand(ctrl)
		m.EXPECT().
			execute("rbd", "snap", "rm", "pool/image@snap").
			Return([]byte{}, nil)

		cmd := mockedCephCmd(m)
		err := cmd.RBDSnapRm("pool", "image", "snap")
		Expect(err).ToNot(HaveOccurred())
	})

	It("should return an error, if the command failed", func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		m := NewMockcommand(ctrl)
		m.EXPECT().execute(gomock.Any()).Return([]byte{}, fmt.Errorf("error"))

		cmd := mockedCephCmd(m)
		err := cmd.RBDSnapRm("pool", "image", "snap")
		Expect(err).To(HaveOccurred())
	})
})
