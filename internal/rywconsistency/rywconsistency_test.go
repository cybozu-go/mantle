// Package rywconsistency_test checks how a manager client with
// EnableReadYourWritesConsistency (controller-runtime v0.25+) behaves
// differently from a client without it. It does not depend on any Mantle code.
package rywconsistency_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

const (
	labelKey   = "rywconsistency.test/job-name"
	labelValue = "job"
)

var (
	cfg       *rest.Config  //nolint:gochecknoglobals
	k8sClient client.Client //nolint:gochecknoglobals
)

func TestMain(m *testing.M) {
	kubernetesVersion := os.Getenv("ENVTEST_KUBERNETES_VERSION")
	if kubernetesVersion == "" {
		kubernetesVersion = "1.35.0" // Set default value to make VSCode's Go extension work.
	}

	binaryAssetsDirectory := os.Getenv("ENVTEST_BIN_DIR")
	if binaryAssetsDirectory == "" {
		binaryAssetsDirectory = "../../bin" // Set default value to make VSCode's Go extension work.
	}

	testEnv := &envtest.Environment{
		DownloadBinaryAssets:        true,
		DownloadBinaryAssetsVersion: "v" + kubernetesVersion,
		BinaryAssetsDirectory:       binaryAssetsDirectory,
	}

	var err error
	cfg, err = testEnv.Start()
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := testEnv.Stop(); err != nil {
			panic(err)
		}
	}()

	// k8sClient talks to kube-apiserver directly. It is used to set up and
	// check the objects without being affected by the client under test.
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		panic(err)
	}

	m.Run()
}

// startManager starts a manager and returns its client. If cacheConfigMapLabel
// is not nil, the cache only holds ConfigMaps that match it.
func startManager(t *testing.T, enableRYW bool, cacheConfigMapLabel labels.Selector) client.Client {
	t.Helper()

	cacheOptions := cache.Options{}
	if cacheConfigMapLabel != nil {
		cacheOptions.ByObject = map[client.Object]cache.ByObject{
			&corev1.ConfigMap{}: {Label: cacheConfigMapLabel},
		}
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
		Client: client.Options{
			Cache: &client.CacheOptions{
				EnableReadYourWritesConsistency: ptr.To(enableRYW),
			},
		},
		Cache:                  cacheOptions,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error)
	go func() {
		errCh <- mgr.Start(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-errCh)
	})

	require.True(t, mgr.GetCache().WaitForCacheSync(t.Context()))

	return mgr.GetClient()
}

func createNamespace(t *testing.T) string {
	t.Helper()

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "ryw-"}}
	require.NoError(t, k8sClient.Create(t.Context(), ns))

	return ns.GetName()
}

func createPod(t *testing.T, namespace string) *corev1.Pod {
	t.Helper()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pod-",
			Namespace:    namespace,
			Labels:       map[string]string{labelKey: labelValue},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
		},
	}
	require.NoError(t, k8sClient.Create(t.Context(), pod))

	return pod
}

func createConfigMap(t *testing.T, namespace string) *corev1.ConfigMap {
	t.Helper()

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "cm-",
			Namespace:    namespace,
		},
	}
	require.NoError(t, k8sClient.Create(t.Context(), cm))

	return cm
}

func exists(t *testing.T, obj client.Object) bool {
	t.Helper()

	err := k8sClient.Get(t.Context(), client.ObjectKeyFromObject(obj), obj.DeepCopyObject().(client.Object))
	if aerrors.IsNotFound(err) {
		return false
	}
	require.NoError(t, err)

	return true
}

// deleteAllPodsOfJob calls DeleteAllOf in the same way as Mantle's
// deletePodsOfJob does.
func deleteAllPodsOfJob(ctx context.Context, c client.Client, namespace string) error {
	return c.DeleteAllOf(
		ctx,
		&corev1.Pod{},
		client.InNamespace(namespace),
		client.MatchingLabels{labelKey: labelValue},
	)
}

func TestDeleteAllOf(t *testing.T) {
	t.Run("without RYW, DeleteAllOf deletes the Pods", func(t *testing.T) {
		c := startManager(t, false, nil)
		ns := createNamespace(t)
		pod := createPod(t, ns)

		require.NoError(t, deleteAllPodsOfJob(t.Context(), c, ns))

		assert.Eventually(t, func() bool { return !exists(t, pod) }, 10*time.Second, 100*time.Millisecond)
	})

	t.Run("with RYW, DeleteAllOf always fails and the Pods remain", func(t *testing.T) {
		c := startManager(t, true, nil)
		ns := createNamespace(t)
		pod := createPod(t, ns)

		err := deleteAllPodsOfJob(t.Context(), c, ns)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DeleteAllOf is not supported by consistentClient")

		// DeleteAllOf returns the error without sending any request to
		// kube-apiserver, so there is no need to wait here.
		assert.True(t, exists(t, pod))
	})
}

// TestDeleteByNameWhileCacheLags checks Delete with an object that has only a
// name and a namespace (no UID), while the object exists in kube-apiserver but
// not yet in the cache. In a real controller, this happens just after the
// object is created. Here the cache is configured with a label selector that
// the ConfigMap does not match, so the ConfigMap never enters the cache. This
// makes the situation deterministic.
func TestDeleteByNameWhileCacheLags(t *testing.T) {
	cacheLabel := labels.SelectorFromSet(labels.Set{labelKey: labelValue})

	t.Run("without RYW, Delete sends the request to kube-apiserver", func(t *testing.T) {
		c := startManager(t, false, cacheLabel)
		ns := createNamespace(t)
		cm := createConfigMap(t, ns)

		target := &corev1.ConfigMap{}
		target.SetName(cm.GetName())
		target.SetNamespace(cm.GetNamespace())
		require.NoError(t, c.Delete(t.Context(), target))

		assert.False(t, exists(t, cm))
	})

	t.Run("with RYW, Delete returns NotFound and the object remains", func(t *testing.T) {
		c := startManager(t, true, cacheLabel)
		ns := createNamespace(t)
		cm := createConfigMap(t, ns)

		target := &corev1.ConfigMap{}
		target.SetName(cm.GetName())
		target.SetNamespace(cm.GetNamespace())
		err := c.Delete(t.Context(), target)

		// Mantle ignores NotFound in this kind of Delete, so the object
		// silently stays.
		require.Error(t, err)
		assert.True(t, aerrors.IsNotFound(err), "unexpected error: %v", err)
		assert.True(t, exists(t, cm))
	})

	t.Run("with RYW, Delete with a UID sends the request to kube-apiserver", func(t *testing.T) {
		c := startManager(t, true, cacheLabel)
		ns := createNamespace(t)
		cm := createConfigMap(t, ns)

		target := &corev1.ConfigMap{}
		target.SetName(cm.GetName())
		target.SetNamespace(cm.GetNamespace())
		target.SetUID(cm.GetUID())
		require.NoError(t, c.Delete(t.Context(), target))

		assert.False(t, exists(t, cm))
	})
}
