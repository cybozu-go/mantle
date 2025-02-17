package ceph

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
	Describe("normal cases", test.normalCases)
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
		// snapshot0 has 0.5MB data
		err := cluster.WriteRandomBlock(t.namespace, t.srcDeployName, 0, 512*1024)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[0])
		Expect(err).NotTo(HaveOccurred())
		err = cluster.GetBlockAsFile(t.namespace, t.srcDeployName, t.snapshots[0])
		Expect(err).NotTo(HaveOccurred())

		// extend volume to 2.0MB and make snapshot1
		err = cluster.ResizePVC(t.namespace, t.srcPVCName, "2Mi")
		Expect(err).NotTo(HaveOccurred())
		err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[1])
		Expect(err).NotTo(HaveOccurred())
		err = cluster.GetBlockAsFile(t.namespace, t.srcDeployName, t.snapshots[1])
		Expect(err).NotTo(HaveOccurred())

		// snapshot2 has additional 1.0MB data
		err = cluster.WriteRandomBlock(t.namespace, t.srcDeployName, 512*1024, 1024*1024)
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

func (t *extendTest) normalCases() {
	It("", func() {
		err := cluster.ExportDiff("/tmp/snapshot0.bin",
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot1-offset-1Ki.bin",
			"--read-offset", "0",
			"--read-length", "1024",
			"--mid-snap-prefix", "snapshot1",
			"--from-snap", t.snapshots[0],
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot1-offset-1Mi.bin",
			"--read-offset", "0",
			"--read-length", "1048576",
			"--mid-snap-prefix", "snapshot1",
			"--from-snap", t.snapshots[0],
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot2-offset-1Ki.bin",
			"--read-offset", "0",
			"--read-length", "1024",
			"--mid-snap-prefix", "snapshot2",
			"--from-snap", t.snapshots[0],
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[2]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot2-offset-1Mi.bin",
			"--read-offset", "0",
			"--read-length", "1048576",
			"--mid-snap-prefix", "snapshot2",
			"--from-snap", t.snapshots[0],
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[2]),
		)
		Expect(err).NotTo(HaveOccurred())

		rollbackMap := map[string]string{
			"/tmp/snapshot0.bin":            "",
			"/tmp/snapshot1-offset-1Ki.bin": t.snapshots[0],
			"/tmp/snapshot1-offset-1Mi.bin": t.snapshots[0],
			"/tmp/snapshot2-offset-1Ki.bin": t.snapshots[0],
			"/tmp/snapshot2-offset-1Mi.bin": t.snapshots[0],
		}

		tests := []struct {
			title            string
			expectedDataName string
			importsBefore    []string
			exportArgs1      []string
			rollbackTo1      string
			exportArgs2      []string
			rollbackTo2      string
		}{
			{
				title:            "(230)",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", "1024", // 1Ki
					"--read-length", "1047552", // 1Mi-1024
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo1: "snapshot1-offset-1024",
				exportArgs2: []string{
					"--read-offset", "1048576", // 1Mi
					"--read-length", "1048576", // 1Mi
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo2: "snapshot1-offset-1048576",
			},
			{
				title:            "(231)",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", "1024", // 1Ki
					"--read-length", "2096128", // 2Mi-1024
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo1: "snapshot1-offset-1024",
			},
			{
				title:            "(232)",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", "1024", // 1Ki
					"--read-length", "5242880", // 5Mi
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo1: "snapshot1-offset-1024",
			},

			{
				title:            "(233)",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Mi.bin"},
				exportArgs1: []string{
					"--read-offset", "1048576", // 1Mi
					"--read-length", "1024", // 1024
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo1: "snapshot1-offset-1048576",
				exportArgs2: []string{
					"--read-offset", "1049600", // 1Mi+1024
					"--read-length", "1047552", // 1Mi-1024
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo2: "snapshot1-offset-1049600",
			},
			{
				title:            "(234)",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Mi.bin"},
				exportArgs1: []string{
					"--read-offset", "1048576", // 1Mi
					"--read-length", "1048576", // 1Mi
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo1: "snapshot1-offset-1048576",
			},
			{
				title:            "(235)",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Mi.bin"},
				exportArgs1: []string{
					"--read-offset", "1048576", // 1Mi
					"--read-length", "5242880", // 5Mi
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo1: "snapshot1-offset-1048576",
			},

			{
				title:            "(236)",
				expectedDataName: t.snapshots[2],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot2-offset-1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", "1024", // 1Ki
					"--read-length", "1047552", // 1Mi-1024
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[2]),
				},
				rollbackTo1: "snapshot2-offset-1024",
				exportArgs2: []string{
					"--read-offset", "1048576", // 1Mi
					"--read-length", "1048576", // 1Mi
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[2]),
				},
				rollbackTo2: "snapshot2-offset-1048576",
			},
			{
				title:            "(237)",
				expectedDataName: t.snapshots[2],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot2-offset-1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", "1024", // 1Ki
					"--read-length", "2096128", // 2Mi-1024
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[2]),
				},
				rollbackTo1: "snapshot2-offset-1024",
			},
			{
				title:            "(238)",
				expectedDataName: t.snapshots[2],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot2-offset-1Ki.bin"},
				exportArgs1: []string{
					"--read-offset", "1024", // 1Ki
					"--read-length", "5242880", // 5Mi
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[2]),
				},
				rollbackTo1: "snapshot2-offset-1024",
			},

			{
				title:            "(239)",
				expectedDataName: t.snapshots[2],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot2-offset-1Mi.bin"},
				exportArgs1: []string{
					"--read-offset", "1048576", // 1Mi
					"--read-length", "1024", // 1024
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[2]),
				},
				rollbackTo1: "snapshot2-offset-1048576",
				exportArgs2: []string{
					"--read-offset", "1049600", // 1Mi+1024
					"--read-length", "1047552", // 1Mi-1024
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[2]),
				},
				rollbackTo2: "snapshot2-offset-1049600",
			},
			{
				title:            "(240)",
				expectedDataName: t.snapshots[2],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot2-offset-1Mi.bin"},
				exportArgs1: []string{
					"--read-offset", "1048576", // 1Mi
					"--read-length", "1048576", // 1Mi
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[2]),
				},
				rollbackTo1: "snapshot2-offset-1048576",
			},
			{
				title:            "(235)",
				expectedDataName: t.snapshots[2],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot2-offset-1Mi.bin"},
				exportArgs1: []string{
					"--read-offset", "1048576", // 1Mi
					"--read-length", "5242880", // 5Mi
					"--mid-snap-prefix", "snapshot2",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[2]),
				},
				rollbackTo1: "snapshot2-offset-1048576",
			},
		}

		for _, tt := range tests {
			By(tt.title)
			if len(tt.importsBefore) != 0 {
				for _, file := range tt.importsBefore {
					err = cluster.ImportDiff(file, t.poolName, t.dstImageName,
						rollbackMap[file], t.namespace, t.dstDeployName, t.dstPVCName)
					Expect(err).NotTo(HaveOccurred())
				}
			}

			err := cluster.ExportDiff("/tmp/exported.bin", tt.exportArgs1...)
			Expect(err).NotTo(HaveOccurred())

			// rollback in import process
			err = cluster.ImportDiff("/tmp/exported.bin", t.poolName, t.dstImageName,
				tt.rollbackTo1, t.namespace, t.dstDeployName, t.dstPVCName)
			Expect(err).NotTo(HaveOccurred())

			if len(tt.exportArgs2) != 0 {
				err := cluster.ExportDiff("/tmp/exported.bin", tt.exportArgs2...)
				Expect(err).NotTo(HaveOccurred())

				// rollback in import process
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
