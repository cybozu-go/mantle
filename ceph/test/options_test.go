package ceph

import (
	"fmt"

	"github.com/cybozu-go/mantle/ceph/test/cluster"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type optionsTest struct {
	namespace string
	poolName  string
	scName    string
	podName   string
	pvcName   string
	snapName  string

	// image name will be set in setupEnv after creating PVC
	imageName string
}

func testOptions() {
	test := &optionsTest{
		namespace: util.GetUniqueName("ns-"),
		poolName:  util.GetUniqueName("pool-"),
		scName:    util.GetUniqueName("sc-"),
		podName:   util.GetUniqueName("pod-"),
		pvcName:   util.GetUniqueName("pvc-"),
		snapName:  util.GetUniqueName("snap-"),
	}

	Describe("setup environment", test.setupEnv)
	Describe("test options", test.testOptions)
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

		err = cluster.CreatePVC(t.namespace, t.pvcName, t.scName, "1Gi")
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CreateDeployment(t.namespace, t.podName, t.pvcName)
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
		err := cluster.SnapRemove(t.poolName, t.imageName, []string{t.snapName})
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CleanupNamespace(t.namespace)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CleanupGlobal()
		Expect(err).NotTo(HaveOccurred())
	})
}

func (t *optionsTest) testOptions() {
	It("test export-diff command will be fail", func() {
		tests := []struct {
			title string
			args  []string
		}{
			{
				title: "(179) only --read-offset",
				args: []string{
					"--read-offset", "0",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.imageName, t.snapName),
				},
			},
			{
				title: "(180) only --read-length",
				args: []string{
					"--read-length", "1024",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.imageName, t.snapName),
				},
			},
			/* TODO: 🚧 implement additional func to rise error this case
			{

				title: "(181) not specify snapshot name",
				args: []string{
					"--read-offset", "0",
					"--read-length", "1024",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.imageName, t.snapName),
				},
			},
			{
				title: "(242) negative value for --read-offset",
				args: []string{
					"--read-offset", "-1",
					"--read-length", "1024",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.imageName, t.snapName),
				},
			},
			{
				title: "(243) not number value for --read-offset",
				args: []string{
					"--read-offset", "a",
					"--read-length", "1024",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.imageName, t.snapName),
				},
			},
			{
				title: "(244) negative value for --read-length",
				args: []string{
					"--read-offset", "0",
					"--read-length", "-1",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.imageName, t.snapName),
				},
			},
			{
				title: "(245) not number value for --read-length",
				args: []string{
					"--read-offset", "0",
					"--read-length", "a",
					"-p", t.poolName,
					fmt.Sprintf("%s@%s", t.imageName, t.snapName),
				},
			},
			{
				title: "(246) invalid mid-snap-prefix",
				args: []string{
					"--read-offset", "0",
					"--read-length", "1024",
					"-p", t.poolName,
					"--mid-snap-prefix", "/invalid",
					fmt.Sprintf("%s@%s", t.imageName, t.snapName),
				},
			},
			*/
		}

		for _, tt := range tests {
			By(tt.title)
			err := cluster.ExportDiff("/tmp/exported.bin", tt.args...)
			Expect(err).To(HaveOccurred())
		}
	})
}
