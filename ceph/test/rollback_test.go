package test

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
	Describe("test main", test.test)
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
		for i := range 2 {
			err := cluster.MakeRandomFile(t.snapshots[i], int(Quantity2Int("1Mi")))
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

func (t *rollbackTest) test() {
	It("checks that rollback feature by writing random data to the target area before rollback", func() {
		err := cluster.ExportDiff("/tmp/snapshot0-offset-1Ki.bin",
			"--read-offset", "0",
			"--read-length", Quantity2Str("1Ki"),
			"--mid-snap-prefix", "snapshot0",
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

		err = cluster.ExportDiff("/tmp/snapshot0.bin",
			fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
		)
		Expect(err).NotTo(HaveOccurred())

		rollbackMap := map[string]string{
			"/tmp/snapshot0-offset-1Ki.bin": "",
			"/tmp/snapshot1-offset-1Ki.bin": t.snapshots[0],
			"/tmp/snapshot0.bin":            "",
		}

		tests := []struct {
			description      string
			expectedDataName string
			importsBefore    []string
			exportArgs       []string
			rollbackTo       string
		}{
			{
				description:      "(212) rbd volume and snapshot diff, whole area (1)",
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
				description:      "(213) rbd volume and snapshot diff, whole area (2)",
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
				description:      "(214) export rbd volume without snapshot, whole area (1)",
				expectedDataName: t.snapshots[1],
				exportArgs: []string{
					"-p", t.poolName,
					t.srcImageName,
				},
			},
			{
				description:      "(215) export rbd volume without snapshot, whole area (2)",
				expectedDataName: t.snapshots[1],
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", "0",
					"-p", t.poolName,
					t.srcImageName,
				},
			},
			{
				description:      "diff between snapshots, a part of volume",
				expectedDataName: t.snapshots[1],
				importsBefore:    []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1Ki.bin"},
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
				description:      "rbd volume and snapshot diff, a part of volume",
				expectedDataName: t.snapshots[0],
				importsBefore:    []string{"/tmp/snapshot0-offset-1Ki.bin"},
				exportArgs: []string{
					"--read-offset", Quantity2Str("1Ki"),
					"--read-length", "0",
					"--mid-snap-prefix", "snapshot0",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.srcImageName, t.snapshots[0]),
				},
				rollbackTo: "snapshot0-offset-1024",
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

			// overwrite data in pod before rollback
			err = cluster.MakeRandomFile("overwrite-data", int(Quantity2Int("2Mi")))
			Expect(err).NotTo(HaveOccurred())
			err = cluster.PushFileToPod("overwrite-data", t.namespace, t.dstDeployName, "/mnt/data")
			Expect(err).NotTo(HaveOccurred())

			// rollback & import process
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
