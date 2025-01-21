package ceph

import (
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMtest(t *testing.T) {
	if os.Getenv("CEPH_CMD_TEST") == "" {
		t.Skip("Run under ceph/")
	}
	// defer cluster.RemoveWorkDir()

	RegisterFailHandler(Fail)

	SetDefaultEventuallyPollingInterval(time.Second)
	SetDefaultEventuallyTimeout(3 * time.Minute)
	EnforceDefaultTimeoutsWhenUsingContexts()

	RunSpecs(t, "tests for custom rbd export-diff command")
}

var _ = Describe("root of tests", func() {
	Context("regression", testRegression)
	Context("options", testOptions)
})
