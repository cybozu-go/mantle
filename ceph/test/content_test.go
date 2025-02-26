package test

import (
	"fmt"

	"github.com/cybozu-go/mantle/ceph/test/cluster"
	"github.com/cybozu-go/mantle/test/util"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type contentTest struct {
	namespace string
	poolName  string
	scName    string

	srcDeployName string
	srcPVCName    string
	srcImageName  string // will be set in setupEnv after creating PVC

	dstDeployName string
	dstPVCName    string
	dstImageName  string // will be set in setupEnv after creating PVC

	snapshots []string
}

func testContent() {
	test := &contentTest{
		namespace: util.GetUniqueName("ns-"),
		poolName:  util.GetUniqueName("pool-"),
		scName:    util.GetUniqueName("sc-"),

		srcDeployName: util.GetUniqueName("pod-"),
		srcPVCName:    util.GetUniqueName("src-pvc-"),
		dstDeployName: util.GetUniqueName("pod-"),
		dstPVCName:    util.GetUniqueName("dst-pvc-"),

		snapshots: []string{
			util.GetUniqueName("snap-"),
			util.GetUniqueName("snap-"),
			util.GetUniqueName("snap-"),
			util.GetUniqueName("snap-"),
			util.GetUniqueName("snap-"),
		},
	}

	Describe("setup environment", test.setupEnv)
	Describe("create snapshot", test.createSnapshot)
	Describe("test main", test.test)
	Describe("teardown environment", test.teardownEnv)
}

//nolint:dupl
func (t *contentTest) setupEnv() {
	It("create resources", func() {
		err := cluster.CreatePool(t.poolName)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateSC(t.scName, t.poolName)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.CreateNamespace(t.namespace)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.CreatePVC(t.namespace, t.srcPVCName, t.scName, "3Mi", cluster.VolumeModeBlock)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateDeployment(t.namespace, t.srcDeployName, t.srcPVCName, cluster.VolumeModeBlock)
		Expect(err).NotTo(HaveOccurred())
		imageName, err := cluster.GetImageNameByPVC(t.namespace, t.srcPVCName)
		Expect(err).NotTo(HaveOccurred())
		t.srcImageName = imageName

		err = cluster.CreatePVC(t.namespace, t.dstPVCName, t.scName, "3Mi", cluster.VolumeModeBlock)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateDeployment(t.namespace, t.dstDeployName, t.dstPVCName, cluster.VolumeModeBlock)
		Expect(err).NotTo(HaveOccurred())
		imageName, err = cluster.GetImageNameByPVC(t.namespace, t.dstPVCName)
		Expect(err).NotTo(HaveOccurred())
		t.dstImageName = imageName
	})
}

func (t *contentTest) createSnapshot() {
	It("create snapshot", func() {
		// snapshot0 has 0.5MiB data
		err := cluster.WriteRandomBlock(t.namespace, t.srcDeployName, 0, Quantity2Int("512Ki"))
		Expect(err).NotTo(HaveOccurred())
		err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[0])
		Expect(err).NotTo(HaveOccurred())
		err = cluster.GetBlockAsFile(t.namespace, t.srcDeployName, t.snapshots[0])
		Expect(err).NotTo(HaveOccurred())

		// snapshot1 has the same data as snapshot0
		err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[1])
		Expect(err).NotTo(HaveOccurred())

		// snapshot2 has additional 1.0MiB data
		err = cluster.WriteRandomBlock(t.namespace, t.srcDeployName, Quantity2Int("512Ki"), Quantity2Int("1Mi"))
		Expect(err).NotTo(HaveOccurred())
		err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[2])
		Expect(err).NotTo(HaveOccurred())
		err = cluster.GetBlockAsFile(t.namespace, t.srcDeployName, t.snapshots[2])
		Expect(err).NotTo(HaveOccurred())

		// snapshot3 has different data from snapshot2
		err = cluster.WriteRandomBlock(t.namespace, t.srcDeployName, 0, Quantity2Int("512Ki")+Quantity2Int("1Mi"))
		Expect(err).NotTo(HaveOccurred())
		err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[3])
		Expect(err).NotTo(HaveOccurred())
		err = cluster.GetBlockAsFile(t.namespace, t.srcDeployName, t.snapshots[3])
		Expect(err).NotTo(HaveOccurred())

		// snapshot4 has discard volume
		err = cluster.DiscardBlock(t.namespace, t.srcDeployName)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[4])
		Expect(err).NotTo(HaveOccurred())
		err = cluster.GetBlockAsFile(t.namespace, t.srcDeployName, t.snapshots[4])
		Expect(err).NotTo(HaveOccurred())
	})
}

func (t *contentTest) teardownEnv() {
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

func (t *contentTest) test() {
	It("checks that the data exported with the offset and length options is correct, "+
		" by import, rollback, and comparing it with the original data.", func() {
		err := cluster.ExportDiff("/tmp/snapshot0-offset-1Ki.bin",
			"--read-offset", "0",
			"--read-length", Quantity2Str("1Ki"),
			"--mid-snap-prefix", "snapshot0",
			fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot0-offset-1Mi.bin",
			"--read-offset", "0",
			"--read-length", Quantity2Str("1Mi"),
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

		err = cluster.ExportDiff("/tmp/snapshot1-offset-1Mi.bin",
			"--read-offset", "0",
			"--read-length", Quantity2Str("1Mi"),
			"--mid-snap-prefix", "snapshot1",
			"--from-snap", t.snapshots[0],
			fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot1.bin",
			fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot2-offset-1Mi.bin",
			"--read-offset", "0",
			"--read-length", Quantity2Str("1Mi"),
			"--mid-snap-prefix", "snapshot2",
			"--from-snap", t.snapshots[1],
			fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[2]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot2.bin",
			fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[2]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot3-offset-1Ki.bin",
			"--read-offset", "0",
			"--read-length", Quantity2Str("1Ki"),
			"--mid-snap-prefix", "snapshot3",
			"--from-snap", t.snapshots[2],
			fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[3]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot3.bin",
			fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[3]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot4-offset-1Mi.bin",
			"--read-offset", "0",
			"--read-length", Quantity2Str("1Mi"),
			"--mid-snap-prefix", "snapshot4",
			"--from-snap", t.snapshots[3],
			fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[4]),
		)
		Expect(err).NotTo(HaveOccurred())

		rollbackMap := map[string]string{
			"/tmp/snapshot0-offset-1Ki.bin": "",
			"/tmp/snapshot0-offset-1Mi.bin": "",
			"/tmp/snapshot0.bin":            "",
			"/tmp/snapshot1-offset-1Ki.bin": t.snapshots[0],
			"/tmp/snapshot1-offset-1Mi.bin": t.snapshots[0],
			"/tmp/snapshot1.bin":            "",
			"/tmp/snapshot2-offset-1Mi.bin": t.snapshots[1],
			"/tmp/snapshot2.bin":            "",
			"/tmp/snapshot3-offset-1Ki.bin": t.snapshots[2],
			"/tmp/snapshot3.bin":            "",
			"/tmp/snapshot4-offset-1Mi.bin": t.snapshots[3],
		}

		tests := []struct {
			description      string
			expectedDataName string
			importsBefore    []string
			exportArgs       []string
			rollbackTo       string
		}{
			{
				description: "(216) having data difference between volume and snapshot, " +
					"without source snapshot, offset + length == rbd volume size",
				expectedDataName: t.snapshots[0],
				importsBefore:    []string{"/tmp/snapshot0-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", fmt.Sprintf("%d", Quantity2Int("3Mi")-1024),
					"--mid-snap-prefix", "snapshot0",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
				rollbackTo: "snapshot0-offset-1024",
			},
			{
				description: "(217) having data difference between volume and snapshot, " +
					"without source snapshot, offset + length > rbd volume size",
				expectedDataName: t.snapshots[0],
				importsBefore:    []string{"/tmp/snapshot0-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", Quantity2Str("5Mi"),
					"--mid-snap-prefix", "snapshot0",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
				rollbackTo: "snapshot0-offset-1024",
			},
			{
				description:      "(218) export for hole area, without source snapshot, offset + length == rbd volume size",
				expectedDataName: t.snapshots[0],
				importsBefore:    []string{"/tmp/snapshot0-offset-1Mi.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Mi"),
					"--read-length", Quantity2Str("2Mi"),
					"--mid-snap-prefix", "snapshot0",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
				rollbackTo: "snapshot0-offset-1048576",
			},
			{
				description:      "(219) export for hole area, without source snapshot, offset + length < rbd volume size",
				expectedDataName: t.snapshots[0],
				importsBefore:    []string{"/tmp/snapshot0-offset-1Mi.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Mi"),
					"--read-length", Quantity2Str("5Mi"),
					"--mid-snap-prefix", "snapshot0",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
				rollbackTo: "snapshot0-offset-1048576",
			},
			{
				description:      "(220) having the same data between snapshots, offset + length == rbd volume size",
				expectedDataName: t.snapshots[0],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", fmt.Sprintf("%d", Quantity2Int("3Mi")-1024),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: "snapshot1-offset-1024",
			},
			{
				description:      "(221) having the same data between snapshots, offset + length > rbd volume size",
				expectedDataName: t.snapshots[0],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", Quantity2Str("5Mi"),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: "snapshot1-offset-1024",
			},
			{
				description:      "(222) having the same hole area between snapshots, offset + length == rbd volume size",
				expectedDataName: t.snapshots[0],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Mi.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Mi"),
					"--read-length", Quantity2Str("2Mi"),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: "snapshot1-offset-1048576",
			},
			{
				description:      "(223) having the same hole area between snapshots, offset + length > rbd volume size",
				expectedDataName: t.snapshots[0],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Mi.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Mi"),
					"--read-length", Quantity2Str("5Mi"),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: "snapshot1-offset-1048576",
			},
			{
				description:      "(224) add data for the all hole area, offset + length == rbd volume size",
				expectedDataName: t.snapshots[2],
				importsBefore:    []string{"/tmp/snapshot1.bin", "/tmp/snapshot2-offset-1Mi.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Mi"),
					"--read-length", Quantity2Str("2Mi"),
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[1],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[2]),
				},
				rollbackTo: "snapshot2-offset-1048576",
			},
			{
				description:      "(225) add data for the all hole area, offset + length > rbd volume size",
				expectedDataName: t.snapshots[2],
				importsBefore:    []string{"/tmp/snapshot1.bin", "/tmp/snapshot2-offset-1Mi.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Mi"),
					"--read-length", Quantity2Str("5Mi"),
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[1],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[2]),
				},
				rollbackTo: "snapshot2-offset-1048576",
			},
			{
				description:      "(226) expand data area between snapshots, offset + length == rbd volume size",
				expectedDataName: t.snapshots[3],
				importsBefore:    []string{"/tmp/snapshot2.bin", "/tmp/snapshot3-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", fmt.Sprintf("%d", Quantity2Int("3Mi")-1024),
					"--mid-snap-prefix", "snapshot3",
					"--from-snap", t.snapshots[2],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[3]),
				},
				rollbackTo: "snapshot3-offset-1024",
			},
			{
				description:      "(227) expand data area between snapshots, offset + length > rbd volume size",
				expectedDataName: t.snapshots[3],
				importsBefore:    []string{"/tmp/snapshot2.bin", "/tmp/snapshot3-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", Quantity2Str("5Mi"),
					"--mid-snap-prefix", "snapshot3",
					"--from-snap", t.snapshots[2],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[3]),
				},
				rollbackTo: "snapshot3-offset-1024",
			},
			{
				description:      "(228) discard data area between snapshots, offset + length == rbd volume size",
				expectedDataName: t.snapshots[4],
				importsBefore:    []string{"/tmp/snapshot3.bin", "/tmp/snapshot4-offset-1Mi.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Mi"),
					"--read-length", Quantity2Str("2Mi"),
					"--mid-snap-prefix", "snapshot4",
					"--from-snap", t.snapshots[3],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[4]),
				},
				rollbackTo: "snapshot4-offset-1048576",
			},
			{
				description:      "(229) discard data area between snapshots, offset + length > rbd volume size",
				expectedDataName: t.snapshots[4],
				importsBefore:    []string{"/tmp/snapshot3.bin", "/tmp/snapshot4-offset-1Mi.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Mi"),
					"--read-length", Quantity2Str("5Mi"),
					"--mid-snap-prefix", "snapshot4",
					"--from-snap", t.snapshots[3],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[4]),
				},
				rollbackTo: "snapshot4-offset-1048576",
			},
		}

		for _, tt := range tests {
			By(tt.description)

			if len(tt.importsBefore) != 0 {
				for _, file := range tt.importsBefore {
					err = cluster.ImportDiff(file, t.poolName, t.dstImageName,
						rollbackMap[file], t.namespace, t.dstDeployName, t.dstPVCName)
					Expect(err).NotTo(HaveOccurred())
				}
			}

			// export from source snapshot to file
			err := cluster.ExportDiff("/tmp/exported.bin", tt.exportArgs...)
			Expect(err).NotTo(HaveOccurred())

			// rollback & import process
			err = cluster.ImportDiff("/tmp/exported.bin", t.poolName, t.dstImageName,
				tt.rollbackTo, t.namespace, t.dstDeployName, t.dstPVCName)
			Expect(err).NotTo(HaveOccurred())

			err = cluster.CompareBlockWithFile(t.namespace, t.dstDeployName, tt.expectedDataName)
			Expect(err).NotTo(HaveOccurred())

			// cleanup
			err = cluster.SnapRemoveAll(t.poolName, t.dstImageName)
			Expect(err).NotTo(HaveOccurred())
		}
	})
}
