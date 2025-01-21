package test

import (
	"fmt"

	"github.com/cybozu-go/mantle/ceph/test/cluster"
	"github.com/cybozu-go/mantle/test/util"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type extendTest struct {
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

func testExtend() {
	test := &extendTest{
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
		},
	}

	Describe("setup environment", test.setupEnv)
	Describe("create snapshot", test.createSnapshot)
	Describe("test main", test.test)
	Describe("teardown environment", test.teardownEnv)
}

//nolint:dupl
func (t *extendTest) setupEnv() {
	It("create resources", func() {
		err := cluster.CreatePool(t.poolName)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateSC(t.scName, t.poolName)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.CreateNamespace(t.namespace)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.CreatePVC(t.namespace, t.srcPVCName, t.scName, "1Mi", cluster.VolumeModeBlock)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateDeployment(t.namespace, t.srcDeployName, t.srcPVCName, cluster.VolumeModeBlock)
		Expect(err).NotTo(HaveOccurred())
		imageName, err := cluster.GetImageNameByPVC(t.namespace, t.srcPVCName)
		Expect(err).NotTo(HaveOccurred())
		t.srcImageName = imageName

		err = cluster.CreatePVC(t.namespace, t.dstPVCName, t.scName, "1Mi", cluster.VolumeModeBlock)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateDeployment(t.namespace, t.dstDeployName, t.dstPVCName, cluster.VolumeModeBlock)
		Expect(err).NotTo(HaveOccurred())
		imageName, err = cluster.GetImageNameByPVC(t.namespace, t.dstPVCName)
		Expect(err).NotTo(HaveOccurred())
		t.dstImageName = imageName
	})
}

func (t *extendTest) createSnapshot() {
	It("create snapshot", func() {
		// snapshot0 has 0.5MiB data
		err := cluster.WriteRandomBlock(t.namespace, t.srcDeployName, 0, Quantity2Int("512Ki"))
		Expect(err).NotTo(HaveOccurred())
		err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[0])
		Expect(err).NotTo(HaveOccurred())
		err = cluster.GetBlockAsFile(t.namespace, t.srcDeployName, t.snapshots[0])
		Expect(err).NotTo(HaveOccurred())

		// extend volume to 2.0MiB and make snapshot1
		err = cluster.ResizePVC(t.namespace, t.srcPVCName, "2Mi")
		Expect(err).NotTo(HaveOccurred())
		err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[1])
		Expect(err).NotTo(HaveOccurred())
		err = cluster.GetBlockAsFile(t.namespace, t.srcDeployName, t.snapshots[1])
		Expect(err).NotTo(HaveOccurred())

		// snapshot2 has additional 1.0MiB data
		err = cluster.WriteRandomBlock(t.namespace, t.srcDeployName, Quantity2Int("512Ki"), Quantity2Int("1Mi"))
		Expect(err).NotTo(HaveOccurred())
		err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[2])
		Expect(err).NotTo(HaveOccurred())
		err = cluster.GetBlockAsFile(t.namespace, t.srcDeployName, t.snapshots[2])
		Expect(err).NotTo(HaveOccurred())
	})
}

func (t *extendTest) teardownEnv() {
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

func (t *extendTest) test() {
	It("checks the consistency of export/import data when volume size (PV/PVC) is expanded", func() {
		err := cluster.ExportDiff("/tmp/snapshot0.bin",
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

		err = cluster.ExportDiff("/tmp/snapshot1-offset-1Mi+1Ki.bin",
			"--read-offset", "0",
			"--read-length", fmt.Sprintf("%d", Quantity2Int("1Mi")+1024),
			"--mid-snap-prefix", "snapshot1",
			"--from-snap", t.snapshots[0],
			fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot2-offset-1Ki.bin",
			"--read-offset", "0",
			"--read-length", Quantity2Str("1Ki"),
			"--mid-snap-prefix", "snapshot2",
			"--from-snap", t.snapshots[0],
			fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[2]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot2-offset-1Mi+1Ki.bin",
			"--read-offset", "0",
			"--read-length", fmt.Sprintf("%d", Quantity2Int("1Mi")+1024),
			"--mid-snap-prefix", "snapshot2",
			"--from-snap", t.snapshots[0],
			fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[2]),
		)
		Expect(err).NotTo(HaveOccurred())

		rollbackMap := map[string]string{
			"/tmp/snapshot0.bin":                "",
			"/tmp/snapshot1-offset-1Ki.bin":     t.snapshots[0],
			"/tmp/snapshot1-offset-1Mi+1Ki.bin": t.snapshots[0],
			"/tmp/snapshot2-offset-1Ki.bin":     t.snapshots[0],
			"/tmp/snapshot2-offset-1Mi+1Ki.bin": t.snapshots[0],
		}

		tests := []struct {
			description      string
			expectedDataName string
			importsBefore    []string
			exportArgs1      []string
			rollbackTo1      string
			exportArgs2      []string
			rollbackTo2      string
		}{
			{
				description: "(230) expand volume but data is not written, " +
					"the snapshot area contains data, offset + length < rbd volume size",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", fmt.Sprintf("%d", Quantity2Int("1Mi")-1024),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo1: "snapshot1-offset-1024",
				exportArgs2: []string{
					"--read-offset", Quantity2Str("1Mi"),
					"--read-length", Quantity2Str("1Mi"),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo2: "snapshot1-offset-1048576",
			},
			{
				description: "(231) expand volume but data is not written, " +
					"the snapshot area contains data, offset + length == rbd volume size",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", fmt.Sprintf("%d", Quantity2Int("2Mi")-1024),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo1: "snapshot1-offset-1024",
			},
			{
				description: "(232) expand volume but data is not written, " +
					"the snapshot area contains data, offset + length > rbd volume size",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", Quantity2Str("5Mi"),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo1: "snapshot1-offset-1024",
			},

			{
				description: "(233) expand volume but data is not written, " +
					"the snapshot area does not contain data, offset + length < rbd volume size",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Mi+1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", fmt.Sprintf("%d", Quantity2Int("1Mi")+1024),
					"--read-length", Quantity2Str("1Ki"),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo1: "snapshot1-offset-1049600",
				exportArgs2: []string{
					"--read-offset", fmt.Sprintf("%d", Quantity2Int("1Mi")+2048),
					"--read-length", fmt.Sprintf("%d", Quantity2Int("1Mi")-2048),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo2: "snapshot1-offset-1050624",
			},
			{
				description: "(234) expand volume but data is not written, " +
					"the snapshot area does not contain data, offset + length == rbd volume size",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Mi+1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", fmt.Sprintf("%d", Quantity2Int("1Mi")+1024),
					"--read-length", fmt.Sprintf("%d", Quantity2Int("1Mi")-1024),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo1: "snapshot1-offset-1049600",
			},
			{
				description: "(235) expand volume but data is not written, " +
					"the snapshot area does not contain data, offset + length > rbd volume size",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Mi+1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", fmt.Sprintf("%d", Quantity2Int("1Mi")+1024),
					"--read-length", Quantity2Str("5Mi"),
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[1]),
				},
				rollbackTo1: "snapshot1-offset-1049600",
			},

			{
				description: "(236) expand volume and data is written, " +
					"the snapshot area contains data, offset + length < rbd volume size",
				expectedDataName: t.snapshots[2],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot2-offset-1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", fmt.Sprintf("%d", Quantity2Int("1Mi")-1024),
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[2]),
				},
				rollbackTo1: "snapshot2-offset-1024",
				exportArgs2: []string{
					"--read-offset", Quantity2Str("1Mi"),
					"--read-length", Quantity2Str("1Mi"),
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[2]),
				},
				rollbackTo2: "snapshot2-offset-1048576",
			},
			{
				description: "(237) expand volume and data is written, " +
					"the snapshot area contains data, offset + length == rbd volume size",
				expectedDataName: t.snapshots[2],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot2-offset-1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", fmt.Sprintf("%d", Quantity2Int("2Mi")-1024),
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[2]),
				},
				rollbackTo1: "snapshot2-offset-1024",
			},
			{
				description: "(238) expand volume and data is written, " +
					"the snapshot area contains data, offset + length > rbd volume size",
				expectedDataName: t.snapshots[2],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot2-offset-1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", Quantity2Str("5Mi"),
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[2]),
				},
				rollbackTo1: "snapshot2-offset-1024",
			},

			{
				description: "(239) expand volume and data is written, " +
					"the snapshot area does not contain data before expand, offset + length < rbd volume size",
				expectedDataName: t.snapshots[2],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot2-offset-1Mi+1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", fmt.Sprintf("%d", Quantity2Int("1Mi")+1024),
					"--read-length", Quantity2Str("1Ki"),
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[2]),
				},
				rollbackTo1: "snapshot2-offset-1049600",
				exportArgs2: []string{
					"--read-offset", fmt.Sprintf("%d", Quantity2Int("1Mi")+2048),
					"--read-length", fmt.Sprintf("%d", Quantity2Int("1Mi")-2048),
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[2]),
				},
				rollbackTo2: "snapshot2-offset-1050624",
			},
			{
				description: "(240) expand volume and data is written, " +
					"the snapshot area does not contain data before expand, offset + length == rbd volume size",
				expectedDataName: t.snapshots[2],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot2-offset-1Mi+1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", fmt.Sprintf("%d", Quantity2Int("1Mi")+1024),
					"--read-length", fmt.Sprintf("%d", Quantity2Int("1Mi")-1024),
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[2]),
				},
				rollbackTo1: "snapshot2-offset-1049600",
			},
			{
				description: "(241) expand volume and data is written, " +
					"the snapshot area does not contain data before expand, offset + length > rbd volume size",
				expectedDataName: t.snapshots[2],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot2-offset-1Mi+1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", fmt.Sprintf("%d", Quantity2Int("1Mi")+1024),
					"--read-length", Quantity2Str("5Mi"),
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[2]),
				},
				rollbackTo1: "snapshot2-offset-1049600",
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
			err := cluster.ExportDiff("/tmp/exported.bin", tt.exportArgs1...)
			Expect(err).NotTo(HaveOccurred())

			// rollback & import process
			err = cluster.ImportDiff("/tmp/exported.bin", t.poolName, t.dstImageName,
				tt.rollbackTo1, t.namespace, t.dstDeployName, t.dstPVCName)
			Expect(err).NotTo(HaveOccurred())

			// export and import until the tail of the volume if test target is in the middle position data
			if len(tt.exportArgs2) != 0 {
				err := cluster.ExportDiff("/tmp/exported.bin", tt.exportArgs2...)
				Expect(err).NotTo(HaveOccurred())

				err = cluster.ImportDiff("/tmp/exported.bin", t.poolName, t.dstImageName,
					tt.rollbackTo2, t.namespace, t.dstDeployName, t.dstPVCName)
				Expect(err).NotTo(HaveOccurred())
			}

			err = cluster.CompareBlockWithFile(t.namespace, t.dstDeployName, tt.expectedDataName)
			Expect(err).NotTo(HaveOccurred())

			// cleanup
			err = cluster.SnapRemoveAll(t.poolName, t.dstImageName)
			Expect(err).NotTo(HaveOccurred())
		}
	})
}
