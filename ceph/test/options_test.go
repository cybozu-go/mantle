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
	test.testMain()
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
		err = cluster.CreatePod(t.namespace, t.podName, t.pvcName)
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
		err := cluster.SnapRemove(t.poolName, t.imageName, t.snapName)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CleanupNamespace(t.namespace)
		Expect(err).NotTo(HaveOccurred())
		err = cluster.CleanupGlobal()
		Expect(err).NotTo(HaveOccurred())
	})
}

func (t *optionsTest) testMain() {
	DescribeTable("test error case for export-diff command",
		func(args ...string) {
			err := cluster.ExportDiff(args...)
			Expect(err).To(HaveOccurred())
		},
		Entry("only --read-offset",
			"--read-offset", "0",
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.imageName, t.snapName), "/dev/null"),
		Entry("only --read-length",
			"--read-length", "1024",
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.imageName, t.snapName), "/dev/null"),
		Entry("not specify snapshot name",
			"--read-offset", "0",
			"--read-length", "1024",
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.imageName, t.snapName), "/dev/null"),
		Entry("negative value for --read-offset",
			"--read-offset", "-1",
			"--read-length", "1024",
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.imageName, t.snapName), "/dev/null"),
		Entry("not number value for --read-offset",
			"--read-offset", "a",
			"--read-length", "1024",
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.imageName, t.snapName), "/dev/null"),
		Entry("negative value for --read-length",
			"--read-offset", "0",
			"--read-length", "-1",
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.imageName, t.snapName), "/dev/null"),
		Entry("not number value for --read-length",
			"--read-offset", "0",
			"--read-length", "a",
			"-p", t.poolName,
			fmt.Sprintf("%s@%s", t.imageName, t.snapName), "/dev/null"),
		Entry("invalid mid-snap-prefix",
			"--read-offset", "0",
			"--read-length", "1024",
			"-p", t.poolName,
			"--mid-snap-prefix", "/invalid",
			fmt.Sprintf("%s@%s", t.imageName, t.snapName), "/dev/null"),
	)
}
