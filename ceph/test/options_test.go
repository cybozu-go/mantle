package test

import (
	"fmt"

	"github.com/cybozu-go/mantle/ceph/test/cluster"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type optionsTest struct {
	namespace  string
	poolName   string
	scName     string
	deployName string
	pvcName    string
	snapName   string

	// image name will be set in setupEnv after creating PVC
	imageName string
}

func testOptions() {
	test := &optionsTest{
		namespace:  util.GetUniqueName("ns-"),
		poolName:   util.GetUniqueName("pool-"),
		scName:     util.GetUniqueName("sc-"),
		deployName: util.GetUniqueName("pod-"),
		pvcName:    util.GetUniqueName("pvc-"),
		snapName:   util.GetUniqueName("snap-"),
	}

	Describe("setup environment", test.setupEnv)
	Describe("test main", test.test)
	Describe("teardown environment", test.teardownEnv)
}

func (t *optionsTest) setupEnv() {
	It("create resources", func() {
		err := cluster.CreatePool(t.poolName)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateSC(t.scName, t.poolName)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.CreateNamespace(t.namespace)
		Expect(err).NotTo(HaveOccurred())

		err = cluster.CreatePVC(t.namespace, t.pvcName, t.scName, "1Gi", cluster.VolumeModeFilesystem)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateDeployment(t.namespace, t.deployName, t.pvcName, cluster.VolumeModeFilesystem)
		Expect(err).NotTo(HaveOccurred())

		imageName, err := cluster.GetImageNameByPVC(t.namespace, t.pvcName)
		Expect(err).NotTo(HaveOccurred())
		t.imageName = imageName

		err = cluster.SnapCreate(t.poolName, imageName, t.snapName)
		Expect(err).NotTo(HaveOccurred())
	})
}

func (t *optionsTest) teardownEnv() {
	It("delete resources", func() {
		err := cluster.SnapRemoveAll(t.poolName, t.imageName)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CleanupNamespace(t.namespace)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CleanupGlobal()
		Expect(err).NotTo(HaveOccurred())
	})
}

func (t *optionsTest) test() {
	It("test export-diff command will be fail", func() {
		tests := []struct {
			description string
			exportArgs  []string
		}{
			{
				description: "(179) specify --read-offset without --read-length",
				exportArgs: []string{
					"--read-offset", "0",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.imageName, t.snapName),
				},
			},
			{
				description: "(180) specify --read-length without --read-offset",
				exportArgs: []string{
					"--read-length", Quantity2Str("1Ki"),
					fmt.Sprintf("%s/%s@%s", t.poolName, t.imageName, t.snapName),
				},
			},
			{

				description: "(181) not specify snapshot name",
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", Quantity2Str("1Ki"),
					"-p", t.poolName,
				},
			},
			{
				description: "(242) specify negative value for --read-offset",
				exportArgs: []string{
					"--read-offset", "-1",
					"--read-length", Quantity2Str("1Ki"),
					fmt.Sprintf("%s/%s@%s", t.poolName, t.imageName, t.snapName),
				},
			},
			{
				description: "(243) specify not number value for --read-offset",
				exportArgs: []string{
					"--read-offset", "a",
					"--read-length", Quantity2Str("1Ki"),
					fmt.Sprintf("%s/%s@%s", t.poolName, t.imageName, t.snapName),
				},
			},
			{
				description: "(244) specify negative value for --read-length",
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", "-1",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.imageName, t.snapName),
				},
			},
			{
				description: "(245) specify not number value for --read-length",
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", "a",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.imageName, t.snapName),
				},
			},
			{
				description: "(246) specify invalid mid-snap-prefix",
				exportArgs: []string{
					"--read-offset", "0",
					"--read-length", Quantity2Str("1Ki"),
					"--mid-snap-prefix", "@invalid",
					fmt.Sprintf("%s/%s@%s", t.poolName, t.imageName, t.snapName),
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
