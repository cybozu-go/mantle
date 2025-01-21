package test

import (
	"os"
	"testing"
	"time"

	"github.com/cybozu-go/mantle/ceph/test/cluster"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	envSkipCephCmdTest = "SKIP_CEPH_CMD_TEST"
)

func TestMTest(t *testing.T) {
	if os.Getenv(envSkipCephCmdTest) == "1" {
		t.Skipf("tests for custom rbd export-diff command are skipped by %s is set to 1", envSkipCephCmdTest)
	}
	defer cluster.RemoveWorkDir()

	RegisterFailHandler(Fail)

	SetDefaultEventuallyPollingInterval(time.Second)
	SetDefaultEventuallyTimeout(3 * time.Minute)
	EnforceDefaultTimeoutsWhenUsingContexts()

	RunSpecs(t, "tests for custom rbd export-diff command")
}

var _ = Describe("root of tests", func() {
	Context("content", testContent)
	Context("extend", testExtend)
	Context("options", testOptions)
	Context("regression", testRegression)
	Context("rollback", testRollback)
	Context("snapshot name", testSnapshotName)
})
