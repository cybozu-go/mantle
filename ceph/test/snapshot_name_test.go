package test

import (
	"fmt"

	"github.com/cybozu-go/mantle/ceph/test/cluster"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type snapshotNameTest struct {
	namespace string
	poolName  string
	scName    string

	srcPVCName   string
	srcImageName string // will be set in setupEnv after creating PVC

	dstPVCName   string
	dstImageName string // will be set in setupEnv after creating PVC

	snapshots []string
}

func testSnapshotName() {
	test := &snapshotNameTest{
		namespace: util.GetUniqueName("ns-"),
		poolName:  util.GetUniqueName("pool-"),
		scName:    util.GetUniqueName("sc-"),

		srcPVCName: util.GetUniqueName("src-pvc-"),
		dstPVCName: util.GetUniqueName("dst-pvc-"),

		snapshots: []string{
			util.GetUniqueName("snap-"),
			util.GetUniqueName("snap-"),
		},
	}

	Describe("setup environment", test.setupEnv)
	Describe("test main 1", test.test1)
	Describe("test main 2", test.test2)
	Describe("teardown environment", test.teardownEnv)
}

func (t *snapshotNameTest) setupEnv() {
	It("create resources", func() {
		err := cluster.CreatePool(t.poolName)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateSC(t.scName, t.poolName)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.CreateNamespace(t.namespace)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.CreatePVC(t.namespace, t.srcPVCName, t.scName, "10Mi", cluster.VolumeModeFilesystem)
		Expect(err).NotTo(HaveOccurred())
		imageName, err := cluster.GetImageNameByPVC(t.namespace, t.srcPVCName)
		Expect(err).NotTo(HaveOccurred())
		t.srcImageName = imageName

		err = cluster.CreatePVC(t.namespace, t.dstPVCName, t.scName, "10Mi", cluster.VolumeModeFilesystem)
		Expect(err).NotTo(HaveOccurred())
		imageName, err = cluster.GetImageNameByPVC(t.namespace, t.dstPVCName)
		Expect(err).NotTo(HaveOccurred())
		t.dstImageName = imageName

		// creating snapshots
		for i := range 2 {
			err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[i])
			Expect(err).NotTo(HaveOccurred())
		}
	})
}

func (t *snapshotNameTest) teardownEnv() {
	It("delete resources", func() {
		err := cluster.SnapRemoveAll(t.poolName, t.srcImageName)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.SnapRemoveAll(t.poolName, t.dstImageName)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CleanupNamespace(t.namespace)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CleanupGlobal()
		Expect(err).NotTo(HaveOccurred())
	})
}

func (t *snapshotNameTest) test1() {
	It("checks the snapshot names contained in the exported data", func() {
		err := cluster.ExportDiff("/tmp/snapshot0-offset-1Ki.bin",
			"--read-offset", "0",
			"--read-length", Quantity2Str("1Ki"),
			"--mid-snap-prefix", "snapshot0",
			fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot0.bin",
			fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot1-offset-1Ki.bin",
			"--read-offset", "0",
			"--read-length", Quantity2Str("1Ki"),
			"--mid-snap-prefix", "snapshot1",
			"--from-snap", t.snapshots[0],
			fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
		)
		Expect(err).NotTo(HaveOccurred())

		rollbackMap := map[string]string{
			"/tmp/snapshot0-offset-1Ki.bin": "",
			"/tmp/snapshot0.bin":            "",
			"/tmp/snapshot1-offset-1Ki.bin": t.snapshots[0],
		}

		tests := []struct {
			description          string
			expectedSnapshotName string
			importsBefore        []string
			exportArgs           []string
			rollbackTo           string
		}{
			{
				description:          "(190) not specify offset or length, without from-snap",
				expectedSnapshotName: t.snapshots[0],
				exportArgs: []string{
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
			},
			{
				description:          "(191) both offset and length are specified as 0, without from-snap",
				expectedSnapshotName: t.snapshots[0],
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", "0",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
			},
			{
				description:          "(192) offset == 0, offset + length < rbd volume size, without from-snap",
				expectedSnapshotName: "mid-snap-offset-1024",
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", Quantity2Str("1Ki"),
					"--mid-snap-prefix", "mid-snap",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
			},
			{
				description:          "(193) offset == 0, offset + length == rbd volume size, without from-snap",
				expectedSnapshotName: t.snapshots[0],
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", Quantity2Str("10Mi"),
					"--mid-snap-prefix", "mid-snap",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
			},
			{
				description:          "(194) offset == 0, offset + length > rbd volume size, without from-snap",
				expectedSnapshotName: t.snapshots[0],
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", Quantity2Str("11Mi"),
					"--mid-snap-prefix", "mid-snap",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
			},
			{
				description:          "(195) offset > 0, length is not specified, without from-snap",
				expectedSnapshotName: t.snapshots[0],
				importsBefore:        []string{"/tmp/snapshot0-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", "0",
					"--mid-snap-prefix", "snapshot0",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
				rollbackTo: "snapshot0-offset-1024",
			},
			{
				description:          "(198) offset > 0, offset + length < rbd volume size, without from-snap",
				expectedSnapshotName: "snapshot0-offset-2048",
				importsBefore:        []string{"/tmp/snapshot0-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", Quantity2Str("1Ki"),
					"--mid-snap-prefix", "snapshot0",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
				rollbackTo: "snapshot0-offset-1024",
			},
			{
				description:          "(199) offset > 0, offset + length == rbd volume size, without from-snap",
				expectedSnapshotName: t.snapshots[0],
				importsBefore:        []string{"/tmp/snapshot0-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", fmt.Sprintf("%d", Quantity2Int("10Mi")-1024),
					"--mid-snap-prefix", "snapshot0",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
				rollbackTo: "snapshot0-offset-1024",
			},
			{
				description:          "(200) offset > 0, offset + length > rbd volume size, without from-snap",
				expectedSnapshotName: t.snapshots[0],
				importsBefore:        []string{"/tmp/snapshot0-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", Quantity2Str("11Mi"),
					"--mid-snap-prefix", "snapshot0",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
				rollbackTo: "snapshot0-offset-1024",
			},
			{
				description:          "(203) not specify offset or length, having from-snap",
				expectedSnapshotName: t.snapshots[1],
				importsBefore:        []string{"/tmp/snapshot0.bin"},
				exportArgs: []string{
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: t.snapshots[0],
			},
			{
				description:          "(204) both offset and length are specified as 0, having from-snap",
				expectedSnapshotName: t.snapshots[1],
				importsBefore:        []string{"/tmp/snapshot0.bin"},
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", "0",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: t.snapshots[0],
			},
			{
				description:          "(205) offset == 0, offset + length < rbd volume size, having from-snap",
				expectedSnapshotName: "snapshot1-offset-1024",
				importsBefore:        []string{"/tmp/snapshot0.bin"},
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", Quantity2Str("1Ki"),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: t.snapshots[0],
			},
			{
				description:          "(206) offset == 0, offset + length == rbd volume size, having from-snap",
				expectedSnapshotName: t.snapshots[1],
				importsBefore:        []string{"/tmp/snapshot0.bin"},
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", Quantity2Str("10Mi"),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: t.snapshots[0],
			},
			{
				description:          "(207) offset == 0, offset + length > rbd volume size, having from-snap",
				expectedSnapshotName: t.snapshots[1],
				importsBefore:        []string{"/tmp/snapshot0.bin"},
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", Quantity2Str("11Mi"),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: t.snapshots[0],
			},
			{
				description:          "(208) offset > 0, length is not specified, having from-snap",
				expectedSnapshotName: t.snapshots[1],
				importsBefore:        []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", "0",
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: "snapshot1-offset-1024",
			},
			{
				description:          "(209) offset > 0, offset + length < rbd volume size, having from-snap",
				expectedSnapshotName: "snapshot1-offset-2048",
				importsBefore:        []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", Quantity2Str("1Ki"),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: "snapshot1-offset-1024",
			},
			{
				description:          "(210) offset > 0, offset + length == rbd volume size, having from-snap",
				expectedSnapshotName: t.snapshots[1],
				importsBefore:        []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", fmt.Sprintf("%d", Quantity2Int("10Mi")-1024),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: "snapshot1-offset-1024",
			},
			{
				description:          "(211) offset > 0, offset + length > rbd volume size, having from-snap",
				expectedSnapshotName: t.snapshots[1],
				importsBefore:        []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", Quantity2Str("11Mi"),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: "snapshot1-offset-1024",
			},
		}

		for _, tt := range tests {
			By(tt.description)

			// export from source snapshot to file
			err := cluster.ExportDiff("/tmp/exported.bin", tt.exportArgs...)
			Expect(err).NotTo(HaveOccurred())

			if len(tt.importsBefore) != 0 {
				// try import and should fail
				err = cluster.ImportDiff("/tmp/exported.bin", t.poolName, t.dstImageName,
					"", t.namespace, "", t.dstPVCName)
				Expect(err).To(HaveOccurred())

				for _, file := range tt.importsBefore {
					err = cluster.ImportDiff(file, t.poolName, t.dstImageName,
						rollbackMap[file], t.namespace, "", t.dstPVCName)
					Expect(err).NotTo(HaveOccurred())
				}
			}

			// check snapshot before import
			exists, err := cluster.SnapExists(t.poolName, t.dstImageName, tt.expectedSnapshotName)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeFalse())

			// import to destination image
			err = cluster.ImportDiff("/tmp/exported.bin", t.poolName, t.dstImageName,
				tt.rollbackTo, t.namespace, "", t.dstPVCName)
			Expect(err).NotTo(HaveOccurred())

			// check snapshot after import
			exists, err = cluster.SnapExists(t.poolName, t.dstImageName, tt.expectedSnapshotName)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())

			// cleanup
			err = cluster.SnapRemoveAll(t.poolName, t.dstImageName)
			Expect(err).NotTo(HaveOccurred())
		}
	})
}

func (t *snapshotNameTest) test2() {
	It("checks error if offset is greater than rbd volume size", func() {
		tests := []struct {
			description string
			exportArgs  []string
		}{
			{
				description: "(196) offset == rbd volume size, unset length",
				exportArgs: []string{
					"--read-offset", Quantity2Str("10Mi"),
					"--read-length", "0",
					"--mid-snap-prefix", "mid-snap",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
			},
			{
				description: "(197) offset > rbd volume size, unset length",
				exportArgs: []string{
					"--read-offset", Quantity2Str("11Mi"),
					"--read-length", "0",
					"--mid-snap-prefix", "mid-snap",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
			},
			{
				description: "(201) offset == rbd volume size, set length",
				exportArgs: []string{
					"--read-offset", Quantity2Str("10Mi"),
					"--read-length", Quantity2Str("1Mi"),
					"--mid-snap-prefix", "mid-snap",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
			},
			{
				description: "(202) offset > rbd volume size, set length",
				exportArgs: []string{
					"--read-offset", Quantity2Str("11Mi"),
					"--read-length", Quantity2Str("1Mi"),
					"--mid-snap-prefix", "mid-snap",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
			},
		}

		for _, tt := range tests {
			By(tt.description)
			err := cluster.ExportDiff("/tmp/exported.bin", tt.exportArgs...)
			Expect(err).To(HaveOccurred())
		}
	})
}
