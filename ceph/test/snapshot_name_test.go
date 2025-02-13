package ceph

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
	Describe("test normal cases", test.normalCases)
	Describe("test error cases", test.errorCases)
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

		err = cluster.CreatePVC(t.namespace, t.srcPVCName, t.scName, "10Mi")
		Expect(err).NotTo(HaveOccurred())
		imageName, err := cluster.GetImageNameByPVC(t.namespace, t.srcPVCName)
		Expect(err).NotTo(HaveOccurred())
		t.srcImageName = imageName

		err = cluster.CreatePVC(t.namespace, t.dstPVCName, t.scName, "10Mi")
		Expect(err).NotTo(HaveOccurred())
		imageName, err = cluster.GetImageNameByPVC(t.namespace, t.dstPVCName)
		Expect(err).NotTo(HaveOccurred())
		t.dstImageName = imageName

		// creating snapshots
		for i := 0; i < 2; i++ {
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

func (t *snapshotNameTest) normalCases() {
	It("normal cases", func() {
		err := cluster.ExportDiff("/tmp/snapshot0.bin",
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot0-offset-1024.bin",
			"--read-offset", "0",
			"--read-length", "1024",
			"--mid-snap-prefix", "snapshot0",
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
		)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ExportDiff("/tmp/snapshot1-offset-1024.bin",
			"--read-offset", "0",
			"--read-length", "1024",
			"--mid-snap-prefix", "snapshot1",
			"--from-snap", t.snapshots[0],
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
		)
		Expect(err).NotTo(HaveOccurred())

		rollbackMap := map[string]string{
			"/tmp/snapshot0.bin":             "",
			"/tmp/snapshot0-offset-1024.bin": "",
			"/tmp/snapshot1-offset-1024.bin": t.snapshots[0],
		}

		tests := []struct {
			title                string
			expectedSnapshotName string
			importsBefore        []string
			exportArgs           []string
			rollbackTo           string
		}{
			{
				title:                "(190)",
				expectedSnapshotName: t.snapshots[0],
				exportArgs: []string{
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
			},
			{
				title:                "(191)",
				expectedSnapshotName: t.snapshots[0],
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", "0",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
			},
			{
				title:                "(192)",
				expectedSnapshotName: "mid-snap-offset-1024",
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", "1024",
					"--mid-snap-prefix", "mid-snap",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
			},
			{
				title:                "(193)",
				expectedSnapshotName: t.snapshots[0],
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", "10485760", // 10Mi
					"--mid-snap-prefix", "mid-snap",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
			},
			{
				title:                "(194)",
				expectedSnapshotName: t.snapshots[0],
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", "11534336", // 11Mi
					"--mid-snap-prefix", "mid-snap",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
			},
			{
				title:                "(195)",
				expectedSnapshotName: t.snapshots[0],
				importsBefore:        []string{"/tmp/snapshot0-offset-1024.bin"},
				exportArgs: []string{
					"--read-offset", "1024",
					"--read-length", "0",
					"--mid-snap-prefix", "snapshot0",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
				rollbackTo: "snapshot0-offset-1024",
			},
			{
				title:                "(198)",
				expectedSnapshotName: "snapshot0-offset-2048",
				importsBefore:        []string{"/tmp/snapshot0-offset-1024.bin"},
				exportArgs: []string{
					"--read-offset", "1024",
					"--read-length", "1024",
					"--mid-snap-prefix", "snapshot0",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
				rollbackTo: "snapshot0-offset-1024",
			},
			{
				title:                "(199)",
				expectedSnapshotName: t.snapshots[0],
				importsBefore:        []string{"/tmp/snapshot0-offset-1024.bin"},
				exportArgs: []string{
					"--read-offset", "1024",
					"--read-length", "10484736", // 10Mi - 1024
					"--mid-snap-prefix", "snapshot0",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
				rollbackTo: "snapshot0-offset-1024",
			},
			{
				title:                "(200)",
				expectedSnapshotName: t.snapshots[0],
				importsBefore:        []string{"/tmp/snapshot0-offset-1024.bin"},
				exportArgs: []string{
					"--read-offset", "1024",
					"--read-length", "11534336", // 11Mi
					"--mid-snap-prefix", "snapshot0",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
				rollbackTo: "snapshot0-offset-1024",
			},
			{
				title:                "(203)",
				expectedSnapshotName: t.snapshots[1],
				importsBefore:        []string{"/tmp/snapshot0.bin"},
				exportArgs: []string{
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: t.snapshots[0],
			},
			{
				title:                "(204)",
				expectedSnapshotName: t.snapshots[1],
				importsBefore:        []string{"/tmp/snapshot0.bin"},
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", "0",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: t.snapshots[0],
			},
			{
				title:                "(205)",
				expectedSnapshotName: "snapshot1-offset-1024",
				importsBefore:        []string{"/tmp/snapshot0.bin"},
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", "1024",
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: t.snapshots[0],
			},
			{
				title:                "(206)",
				expectedSnapshotName: t.snapshots[1],
				importsBefore:        []string{"/tmp/snapshot0.bin"},
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", "10485760", // 10Mi
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: t.snapshots[0],
			},
			{
				title:                "(207)",
				expectedSnapshotName: t.snapshots[1],
				importsBefore:        []string{"/tmp/snapshot0.bin"},
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", "11534336", // 11Mi
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: t.snapshots[0],
			},
			{
				title:                "(208)",
				expectedSnapshotName: t.snapshots[1],
				importsBefore:        []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1024.bin"},
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
				title:                "(209)",
				expectedSnapshotName: "snapshot1-offset-2048",
				importsBefore:        []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1024.bin"},
				exportArgs: []string{
					"--read-offset", "1024",
					"--read-length", "1024",
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: "snapshot1-offset-1024",
			},
			{
				title:                "(210)",
				expectedSnapshotName: t.snapshots[1],
				importsBefore:        []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1024.bin"},
				exportArgs: []string{
					"--read-offset", "1024",
					"--read-length", "10484736", // 10Mi - 1024
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: "snapshot1-offset-1024",
			},
			{
				title:                "(211)",
				expectedSnapshotName: t.snapshots[1],
				importsBefore:        []string{"/tmp/snapshot0.bin", "/tmp/snapshot1-offset-1024.bin"},
				exportArgs: []string{
					"--read-offset", "1024",
					"--read-length", "11534336", // 11Mi
					"--mid-snap-prefix", "snapshot1",
					"--from-snap", t.snapshots[0],
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
				rollbackTo: "snapshot1-offset-1024",
			},
		}

		for _, tt := range tests {
			By(tt.title)
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

func (t *snapshotNameTest) errorCases() {
	It("error cases", func() {
		tests := []struct {
			title      string
			exportArgs []string
		}{
			/* 🚧 TODO after fix
			{
				title: "(196)",
				exportArgs: []string{
					"--read-offset", "10485760", // 10Mi
					"--read-length", "0",
					"--mid-snap-prefix", "mid-snap",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
			},
			*/
			{
				title: "(197)",
				exportArgs: []string{
					"--read-offset", "11534336", // 11Mi
					"--read-length", "0",
					"--mid-snap-prefix", "mid-snap",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
			},
			/* 🚧 TODO after fix
			{
				title: "(201)",
				exportArgs: []string{
					"--read-offset", "10485760", // 10Mi
					"--read-length", "1048576", // 1Mi
					"--mid-snap-prefix", "mid-snap",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
			},
			*/
			{
				title: "(202)",
				exportArgs: []string{
					"--read-offset", "11534336", // 11Mi
					"--read-length", "1048576", // 1Mi
					"--mid-snap-prefix", "mid-snap",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
			},
		}

		for _, tt := range tests {
			By(tt.title)
			err := cluster.ExportDiff("/tmp/exported.bin", tt.exportArgs...)
			Expect(err).To(HaveOccurred())
		}
	})
}
