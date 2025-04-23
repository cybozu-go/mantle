package testutil

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	//+kubebuilder:scaffold:imports
)

// These tests use Ginkgo (BDD-style Go testing framework). Refer to
// http://onsi.github.io/ginkgo/ to learn more about Ginkgo.

func TestUtils(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Testutils Suite")
}
