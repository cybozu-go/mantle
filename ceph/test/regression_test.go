package ceph

import (
	"fmt"

	"github.com/cybozu-go/mantle/ceph/test/cluster"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type regressionTest struct {
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

func testRegression() {
	test := &regressionTest{
		namespace: util.GetUniqueName("ns-"),
		poolName:  util.GetUniqueName("pool-"),
		scName:    util.GetUniqueName("sc-"),

		srcDeployName: util.GetUniqueName("pod-"),
		srcPVCName:    util.GetUniqueName("pvc-"),

		dstDeployName: util.GetUniqueName("pod-"),
		dstPVCName:    util.GetUniqueName("pvc-"),

		snapshots: []string{
			util.GetUniqueName("snap-"),
			util.GetUniqueName("snap-"),
			util.GetUniqueName("snap-"),
			util.GetUniqueName("snap-"),
		},
	}

	Describe("setup environment", test.setupEnv)
	Describe("test export-diff for snapshot without from-snap option", test.testWithoutFromSnapMain)
	Describe("test export-diff for snapshot with from-snap option", test.testWithFromSnapMain)
	Describe("test export-diff for RBD image", test.testExportRBDImage)
	// Describe("teardown environment", test.teardownEnv)
}

func (t *regressionTest) setupEnv() {
	It("create resources", func() {
		err := cluster.CreatePool(t.poolName)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateSC(t.scName, t.poolName)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.CreateNamespace(t.namespace)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.CreatePVC(t.namespace, t.srcPVCName, t.scName, "10Mi")
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateDeployment(t.namespace, t.srcDeployName, t.srcPVCName)
		Expect(err).NotTo(HaveOccurred())
		imageName, err := cluster.GetImageNameByPVC(t.namespace, t.srcPVCName)
		Expect(err).NotTo(HaveOccurred())
		t.srcImageName = imageName

		// creating snapshots
		// snapshots[0] and snapshots[1] have diff with the image
		// snapshots[2] and snapshots[2] has no diff with the image
		for i := 0; i < 3; i++ {
			err := cluster.MakeRandomFile(t.snapshots[i], 5*1024*1024)
			Expect(err).NotTo(HaveOccurred())
			err = cluster.PushFileToPod(t.snapshots[i], t.namespace, t.srcDeployName, "/mnt/data")
			Expect(err).NotTo(HaveOccurred())
			err = cluster.SnapCreate(t.poolName, imageName, t.snapshots[i])
			Expect(err).NotTo(HaveOccurred())
		}
		// crate snapshot[3] with the same data as snapshot[2]
		// random file snapshot[3] is not exist
		err = cluster.SnapCreate(t.poolName, imageName, t.snapshots[3])
		Expect(err).NotTo(HaveOccurred())

		err = cluster.CreatePVC(t.namespace, t.dstPVCName, t.scName, "10Mi")
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateDeployment(t.namespace, t.dstDeployName, t.dstPVCName)
		Expect(err).NotTo(HaveOccurred())
		imageName, err = cluster.GetImageNameByPVC(t.namespace, t.dstPVCName)
		Expect(err).NotTo(HaveOccurred())
		t.dstImageName = imageName
	})
}

func (t *regressionTest) teardownEnv() {
	It("delete resources", func() {
		err := cluster.SnapRemove(t.poolName, t.srcImageName, t.snapshots)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CleanupNamespace(t.namespace)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CleanupGlobal()
		Expect(err).NotTo(HaveOccurred())
	})
}

func (t *regressionTest) testWithoutFromSnapMain() {
	It("export-diff for snapshot without from-snap option", func() {
		tests := []struct {
			title            string
			expectedDataName string
			args             []string
		}{
			{
				title:            "(182) specify snapshot name with <image>@<snap> format",
				expectedDataName: t.snapshots[0],
				args: []string{
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]),
				},
			},
			{
				title:            "(183) specify snapshot name with --snap option",
				expectedDataName: t.snapshots[0],
				args: []string{
					"-p", t.poolName,
					"--image", t.srcImageName,
					"--snap", t.snapshots[0],
				},
			},
			{
				title:            "(185) specify snapshot which don't have diff with RBD image",
				expectedDataName: t.snapshots[2],
				args: []string{

					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[2]),
				},
			},
		}

		for _, tt := range tests {
			By(tt.title)
			// export from source snapshot to file
			err := cluster.ExportDiff("/tmp/exported.bin", tt.args...)
			Expect(err).NotTo(HaveOccurred())

			// import to destination image
			err = cluster.ImportDiff("/tmp/exported.bin", t.poolName, t.dstImageName)
			Expect(err).NotTo(HaveOccurred())

			// apply snapshot to the destination image
			err = cluster.SnapRollback(t.namespace, t.dstDeployName, t.poolName, t.dstImageName, tt.expectedDataName)
			Expect(err).NotTo(HaveOccurred())

			// compare the data in the destination image with the expected data
			err = cluster.CompareFilesInPod(tt.expectedDataName, t.namespace, t.dstDeployName, "/mnt/data")
			// expect no difference
			Expect(err).NotTo(HaveOccurred())

			// cleanup
			err = cluster.SnapRemove(t.poolName, t.dstImageName, []string{tt.expectedDataName})
			Expect(err).NotTo(HaveOccurred())
		}
	})
}

func (t *regressionTest) testWithFromSnapMain() {
	It("export-diff for snapshot with from-snap option", func() {
		// export snapshot[0] and import it to the destination image before running the tests
		err := cluster.ExportDiff("/tmp/exported.bin", "-p", t.poolName, fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[0]))
		Expect(err).NotTo(HaveOccurred())
		err = cluster.ImportDiff("/tmp/exported.bin", t.poolName, t.dstImageName)
		Expect(err).NotTo(HaveOccurred())

		tests := []struct {
			title            string
			expectedDataName string
			args             []string
		}{
			{
				title:            "(186) specify snapshot name with <image>@<snap> format",
				expectedDataName: t.snapshots[1],
				args: []string{
					"-p", t.poolName,
					"--from-snap", t.snapshots[0],
					fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[1]),
				},
			},
			{
				title:            "(187) specify snapshot name with --snap option",
				expectedDataName: t.snapshots[1],
				args: []string{
					"-p", t.poolName,
					"--from-snap", t.snapshots[0],
					"--image", t.srcImageName,
					"--snap", t.snapshots[1],
				},
			},
		}
		for _, tt := range tests {
			By(tt.title)
			// export from source snapshot to file
			err = cluster.ExportDiff("/tmp/exported.bin", tt.args...)
			Expect(err).NotTo(HaveOccurred())

			// import to destination image
			err = cluster.ImportDiff("/tmp/exported.bin", t.poolName, t.dstImageName)
			Expect(err).NotTo(HaveOccurred())

			// apply snapshot to the destination image
			err = cluster.SnapRollback(t.namespace, t.dstDeployName, t.poolName, t.dstImageName, tt.expectedDataName)
			Expect(err).NotTo(HaveOccurred())

			// compare the data in the destination image with the expected data
			err = cluster.CompareFilesInPod(tt.expectedDataName, t.namespace, t.dstDeployName, "/mnt/data")
			// expect no difference
			Expect(err).NotTo(HaveOccurred())

			// cleanup
			err = cluster.SnapRemove(t.poolName, t.dstImageName, []string{tt.expectedDataName})
			Expect(err).NotTo(HaveOccurred())
		}

		By("(189) specify snapshot which don't have diff with RBD image")
		for i := 1; i < 4; i++ {
			err := cluster.ExportDiff("/tmp/exported.bin", "-p", t.poolName, fmt.Sprintf("%s@%s", t.srcImageName, t.snapshots[i]))
			Expect(err).NotTo(HaveOccurred())
			err = cluster.ImportDiff("/tmp/exported.bin", t.poolName, t.dstImageName)
			Expect(err).NotTo(HaveOccurred())
		}
		// apply snapshot to the destination image
		err = cluster.SnapRollback(t.namespace, t.dstDeployName, t.poolName, t.dstImageName, t.snapshots[3])
		Expect(err).NotTo(HaveOccurred())
		// compare the data in the destination image with the expected data
		err = cluster.CompareFilesInPod(t.snapshots[2], t.namespace, t.dstDeployName, "/mnt/data")
		// expect no difference
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < 4; i++ {
			err := cluster.SnapRemove(t.poolName, t.dstImageName, []string{t.snapshots[i]})
			Expect(err).NotTo(HaveOccurred())
		}
	})
}

func (t *regressionTest) testExportRBDImage() {
	It("(184) export-diff for RBD image without --from-snapshot", func() {
		err := cluster.ExportDiff("/tmp/exported.bin", "-p", t.poolName, t.srcImageName)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.RemoveFileByPod(t.namespace, t.dstDeployName, "/mnt/data")
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ImportDiff("/tmp/exported.bin", t.poolName, t.dstImageName)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.CompareFilesInPod(t.snapshots[2], t.namespace, t.dstDeployName, "/mnt/data")
		Expect(err).NotTo(HaveOccurred())
	})

	It("(188) export-diff for RBD image with --from-snapshot", func() {
		// skip t.snapshots[0]
		err := cluster.ExportDiff("/tmp/exported.bin",
			"-p", t.poolName, "--image", t.srcImageName, "--snap", t.snapshots[1])
		Expect(err).NotTo(HaveOccurred())

		err = cluster.ImportDiff("/tmp/exported.bin", t.poolName, t.dstImageName)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.SnapRollback(t.namespace, t.dstDeployName, t.poolName, t.dstImageName, t.snapshots[1])
		Expect(err).NotTo(HaveOccurred())

		err = cluster.CompareFilesInPod(t.snapshots[1], t.namespace, t.dstDeployName, "/mnt/data")
		Expect(err).NotTo(HaveOccurred())

		// cleanup
		err = cluster.SnapRemove(t.poolName, t.dstImageName, []string{t.snapshots[1]})
		Expect(err).NotTo(HaveOccurred())
	})
}
