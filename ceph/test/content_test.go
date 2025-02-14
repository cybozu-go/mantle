package ceph

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
		},
	}

	Describe("setup environment", test.setupEnv)
	Describe("create snapshot", test.createSnapshot)
	Describe("normal cases", test.normalCases)
	Describe("teardown environment", test.teardownEnv)
}

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
		// snapshot0 has 0.5MB data
		err := cluster.WriteRandomBlock(t.namespace, t.srcDeployName, 512*1024)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[0])
		Expect(err).NotTo(HaveOccurred())
		err = cluster.GetBlockAsFile(t.namespace, t.srcDeployName, t.snapshots[0])
		Expect(err).NotTo(HaveOccurred())

		// snapshot1 has the same data as snapshot0
		err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[1])
		Expect(err).NotTo(HaveOccurred())

		// snapshot2 has different 1.5MB data
		err = cluster.WriteRandomBlock(t.namespace, t.srcDeployName, 1536*1024)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[2])
		Expect(err).NotTo(HaveOccurred())
		err = cluster.GetBlockAsFile(t.namespace, t.srcDeployName, t.snapshots[2])
		Expect(err).NotTo(HaveOccurred())

		// snapshot3 has discard volume
		err = cluster.DiscardBlock(t.namespace, t.srcDeployName)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[3])
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

func (t *contentTest) normalCases() {
	It("", func() {
		err := cluster.ExportDiff("/tmp/snapshot0-offset-1Ki.bin",
			"--read-offset", "0",
			"--read-length", "1024",
			"--mid-snap-prefix", "snapshot0",
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot0-offset-1Mi.bin",
			"--read-offset", "0",
			"--read-length", "1048576",
			"--mid-snap-prefix", "snapshot0",
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
		)
		Expect(err).NotTo(HaveOccurred())

		rollbackMap := map[string]string{
			"/tmp/snapshot0-offset-1Ki.bin": "",
			"/tmp/snapshot0-offset-1Mi.bin": "",
		}

		tests := []struct {
			title            string
			expectedDataName string
			importsBefore    []string
			exportArgs       []string
			rollbackTo       string
		}{
			{
				title:            "(216)",
				expectedDataName: t.snapshots[0],
				importsBefore:    []string{"/tmp/snapshot0-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", "1024",
					"--read-length", "3144704", // 3Mi - 1024
					"--mid-snap-prefix", "snapshot0",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
			},
			{
				title:            "(217)",
				expectedDataName: t.snapshots[0],
				importsBefore:    []string{"/tmp/snapshot0-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", "1024",
					"--read-length", "5242880", // 5Mi
					"--mid-snap-prefix", "snapshot0",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
			},
			{
				title:            "(218)",
				expectedDataName: t.snapshots[0],
				importsBefore:    []string{"/tmp/snapshot0-offset-1Mi.bin"},
				exportArgs: []string{
					"--read-offset", "1048576", // 1Mi
					"--read-length", "2097152", // 2Mi
					"--mid-snap-prefix", "snapshot0",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
			},
			{
				title:            "(219)",
				expectedDataName: t.snapshots[0],
				importsBefore:    []string{"/tmp/snapshot0-offset-1Mi.bin"},
				exportArgs: []string{
					"--read-offset", "1048576", // 1Mi
					"--read-length", "5242880", // 5Mi
					"--mid-snap-prefix", "snapshot0",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
			},
		}

		for _, tt := range tests {
			By(tt.title)
			// export from source snapshot to file
			err := cluster.ExportDiff("/tmp/exported.bin", tt.exportArgs...)
			Expect(err).NotTo(HaveOccurred())

			if len(tt.importsBefore) != 0 {
				for _, file := range tt.importsBefore {
					err = cluster.ImportDiff(file, t.poolName, t.dstImageName,
						rollbackMap[file], t.namespace, t.dstDeployName, t.dstPVCName)
					Expect(err).NotTo(HaveOccurred())
				}
			}

			// rollback in import process
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
