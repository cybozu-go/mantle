package infra_test

import (
	"os"
	"path/filepath"
	"testing"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// k8sClient is a client of the Kubernetes API server that envtest runs. It is a
// client.WithWatch so that the tests can wrap it with interceptor.NewClient.
var k8sClient client.WithWatch //nolint:gochecknoglobals

func TestMain(m *testing.M) {
	kubernetesVersion := os.Getenv("ENVTEST_KUBERNETES_VERSION")
	if kubernetesVersion == "" {
		kubernetesVersion = "1.36.0" // Set default value to make VSCode's Go extension work.
	}

	binaryAssetsDirectory := os.Getenv("ENVTEST_BIN_DIR")
	if binaryAssetsDirectory == "" {
		binaryAssetsDirectory = "../../../bin" // Set default value to make VSCode's Go extension work.
	}

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:           []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing:       true,
		DownloadBinaryAssets:        true,
		DownloadBinaryAssetsVersion: "v" + kubernetesVersion,
		BinaryAssetsDirectory:       binaryAssetsDirectory,
	}

	cfg, err := testEnv.Start()
	if err != nil {
		panic(err)
	}

	if cfg == nil {
		panic("cfg is nil")
	}

	defer func() {
		err := testEnv.Stop()
		if err != nil {
			panic(err)
		}
	}()

	err = mantlev1.AddToScheme(scheme.Scheme)
	if err != nil {
		panic(err)
	}

	k8sClient, err = client.NewWithWatch(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		panic(err)
	}

	m.Run()
}
