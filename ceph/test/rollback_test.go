package ceph

import (
	"fmt"

	"github.com/cybozu-go/mantle/ceph/test/cluster"
	"github.com/cybozu-go/mantle/test/util"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type rollbackTest struct {
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

func testRollback() {
	test := &rollbackTest{
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
		},
	}

	Describe("setup environment", test.setupEnv)
	Describe("normal cases", test.withFromSnap)
	Describe("teardown environment", test.teardownEnv)
}

func (t *rollbackTest) setupEnv() {
	It("create resources", func() {
		err := cluster.CreatePool(t.poolName)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateSC(t.scName, t.poolName)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.CreateNamespace(t.namespace)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.CreatePVC(t.namespace, t.srcPVCName, t.scName, "10Mi", cluster.VolumeModeFilesystem)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateDeployment(t.namespace, t.srcDeployName, t.srcPVCName, cluster.VolumeModeFilesystem)
		Expect(err).NotTo(HaveOccurred())
		imageName, err := cluster.GetImageNameByPVC(t.namespace, t.srcPVCName)
		Expect(err).NotTo(HaveOccurred())
		t.srcImageName = imageName

		err = cluster.CreatePVC(t.namespace, t.dstPVCName, t.scName, "10Mi", cluster.VolumeModeFilesystem)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateDeployment(t.namespace, t.dstDeployName, t.dstPVCName, cluster.VolumeModeFilesystem)
		Expect(err).NotTo(HaveOccurred())
		imageName, err = cluster.GetImageNameByPVC(t.namespace, t.dstPVCName)
		Expect(err).NotTo(HaveOccurred())
		t.dstImageName = imageName

		// creating snapshots
		for i := 0; i < 2; i++ {
			err := cluster.MakeRandomFile(t.snapshots[i], 1*1024*1024)
			Expect(err).NotTo(HaveOccurred())
			err = cluster.PushFileToPod(t.snapshots[i], t.namespace, t.srcDeployName, "/mnt/data")
			Expect(err).NotTo(HaveOccurred())
			err = cluster.SnapCreate(t.poolName, t.srcImageName, t.snapshots[i])
			Expect(err).NotTo(HaveOccurred())
		}
	})
}

func (t *rollbackTest) teardownEnv() {
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

func (t *rollbackTest) withFromSnap() {
	It("normal case", func() {
		err := cluster.ExportDiff("/tmp/snapshot0.bin",
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot0-offset-1Ki.bin",
			"--read-offset", "0",
			"--read-length", "1024",
			"--mid-snap-prefix", "snapshot0",
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

		rollbackMap := map[string]string{
			"/tmp/snapshot0.bin":            "",
			"/tmp/snapshot0-offset-1Ki.bin": "",
			"/tmp/snapshot1-offset-1Ki.bin": t.snapshots[0],
		}

		tests := []struct {
			title            string
			expectedDataName string
			importsBefore    []string
			exportArgs       []string
			rollbackTo       string
		}{
			{
				title:            "(212)",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin"},
				exportArgs: []string{
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					t.srcImageName,
				},
				rollbackTo: t.snapshots[0],
			},
			{
				title:            "(213)",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin"},
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", "0",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					t.srcImageName,
				},
				rollbackTo: t.snapshots[0],
			},
			{
				title:            "(214)",
				expectedDataName: t.snapshots[1],
				exportArgs: []string{
					"-p", t.poolName,
					t.srcImageName,
				},
			},
			{
				title:            "(215)",
				expectedDataName: t.snapshots[1],
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", "0",
					"-p", t.poolName,
					t.srcImageName,
				},
			},
			{
				title:            "extra 1",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", "1024",
					"--read-length", "0",
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: "snapshot1-offset-1024",
			},
			{
				title:            "extra 2",
				expectedDataName: t.snapshots[0],
				importsBefore:    []string{"/tmp/snapshot0-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", "1024",
					"--read-length", "0",
					"--mid-snap-prefix", "snapshot0",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
				rollbackTo: "snapshot0-offset-1024",
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

			// overwrite data in pod before rollback
			err = cluster.MakeRandomFile("dummy", 2*1024*1024)
			Expect(err).NotTo(HaveOccurred())
			err = cluster.PushFileToPod("dummy", t.namespace, t.dstDeployName, "/mnt/data")
			Expect(err).NotTo(HaveOccurred())

			// rollback in import process
			err = cluster.ImportDiff("/tmp/exported.bin", t.poolName, t.dstImageName,
				tt.rollbackTo, t.namespace, t.dstDeployName, t.dstPVCName)
			Expect(err).NotTo(HaveOccurred())

			err = cluster.CompareFilesInPod(tt.expectedDataName, t.namespace, t.dstDeployName, "/mnt/data")
			Expect(err).NotTo(HaveOccurred())

			// cleanup
			err = cluster.SnapRemoveAll(t.poolName, t.dstImageName)
			Expect(err).NotTo(HaveOccurred())
		}
	})
}
