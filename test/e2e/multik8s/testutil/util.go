package testutil

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/ceph"
	"github.com/cybozu-go/mantle/internal/controller"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/yaml"
)

const (
	CephCluster1Namespace      = "rook-ceph"
	CephCluster2Namespace      = "rook-ceph2"
	PrimaryK8sCluster          = 1
	SecondaryK8sCluster        = 2
	SCName1                    = "rook-ceph-block"
	SCName2                    = "rook-ceph-block2"
	RgwDeployName              = "rook-ceph-rgw-ceph-object-store-a"
	MantleControllerDeployName = "mantle-controller"
)

var (
	//go:embed testdata/pvc-template.yaml
	testPVCTemplate string
	//go:embed testdata/pod-mount-volume-template.yaml
	testPodMountVolumeTemplate string
	//go:embed testdata/rbd-pool-template.yaml
	testRBDPoolTemplate string
	//go:embed testdata/sc-template.yaml
	testSCTemplate string
	//go:embed testdata/mantlebackup-template.yaml
	testMantleBackupTemplate string
	//go:embed testdata/mantlebackupconfig-template.yaml
	testMantleBackupConfigTemplate string
	//go:embed testdata/mantlerestore-template.yaml
	testMantleRestoreTemplate string
	//go:embed testdata/mount-deploy-template.yaml
	mountDeployTemplate string
	//go:embed testdata/write-job-template.yaml
	writeJobTemplate string

	kubectlPrefixPrimary   = os.Getenv("KUBECTL_PRIMARY")
	kubectlPrefixSecondary = os.Getenv("KUBECTL_SECONDARY")
)

func GetClusterName(clusterNo int) string {
	switch clusterNo {
	case PrimaryK8sCluster:
		return "primary"
	case SecondaryK8sCluster:
		return "secondary"
	}
	panic("invalid clusterNo")
}

func execAtLocal(ctx context.Context, cmd string, input []byte, args ...string) ([]byte, []byte, error) {
	var stdout, stderr bytes.Buffer
	command := exec.CommandContext(ctx, cmd, args...)
	command.Stdout = &stdout
	command.Stderr = &stderr

	if len(input) != 0 {
		command.Stdin = bytes.NewReader(input)
	}

	err := command.Run()

	return stdout.Bytes(), stderr.Bytes(), err
}

func getKubectlInvocation(clusterNo int) ([]string, error) {
	kubectlPrefix := ""
	switch clusterNo {
	case PrimaryK8sCluster:
		kubectlPrefix = kubectlPrefixPrimary
	case SecondaryK8sCluster:
		kubectlPrefix = kubectlPrefixSecondary
	default:
		return nil, fmt.Errorf("invalid clusterNo: %d", clusterNo)
	}
	if len(kubectlPrefix) == 0 {
		return nil, errors.New("either KUBECTL_PRIMARY or KUBECTL_SECONDARY environment variable is not set")
	}

	return strings.Fields(kubectlPrefix), nil
}

// input can be nil.
func Kubectl(clusterNo int, input []byte, args ...string) ([]byte, []byte, error) {
	fields, err := getKubectlInvocation(clusterNo)
	if err != nil {
		panic(err)
	}
	fields = append(fields, args...)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	return execAtLocal(ctx, fields[0], input, fields[1:]...)
}

func runMakeCommand(args ...string) error {
	ctx := context.Background()

	path := os.Getenv("MAKEFILE_DIR")
	if len(path) == 0 {
		return errors.New("MAKEFILE_DIR envvar is not set")
	}
	args = append([]string{"-C", path}, args...)
	stdout, stderr, err := execAtLocal(ctx, "make", nil, args...)
	if err != nil {
		return fmt.Errorf("make failed. stdout: %s, stderr: %s, err: %w",
			string(stdout), string(stderr), err)
	}

	return nil
}

func CheckDeploymentReady(clusterNo int, namespace, name string) error {
	_, stderr, err := Kubectl(
		clusterNo, nil,
		"-n", namespace, "wait", "--for=condition=Available", "deploy", name, "--timeout=1m",
	)
	if err != nil {
		return fmt.Errorf("kubectl wait deploy failed. stderr: %s, err: %w", string(stderr), err)
	}

	return nil
}

func ApplyObject(clusterNo int, obj runtime.Object) error {
	// Populate apiVersion/kind from the scheme so callers don't need to set TypeMeta.
	gvks, _, err := scheme.Scheme.ObjectKinds(obj)
	if err != nil {
		return fmt.Errorf("failed to get GVK: %w", err)
	}
	if len(gvks) != 1 {
		return fmt.Errorf("failed to get GVK: no kinds registered or multiple kinds registered for %T", obj)
	}
	obj.GetObjectKind().SetGroupVersionKind(gvks[0])

	raw, err := yaml.Marshal(obj)
	if err != nil {
		return fmt.Errorf("failed to marshal object: %w", err)
	}
	_, stderr, err := Kubectl(clusterNo, raw, "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply failed. stderr: %s, err: %w", string(stderr), err)
	}

	return nil
}

func ApplyMantleBackupTemplate(clusterNo int, namespace, pvcName, backupName string) error {
	manifest := fmt.Sprintf(testMantleBackupTemplate, backupName, backupName, namespace, pvcName)
	_, _, err := Kubectl(clusterNo, []byte(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply mantlebackup failed. err: %w", err)
	}

	return nil
}

func ApplyMantleBackupConfigTemplate(clusterNo int, namespace, pvcName, backupConfigName string) error {
	manifest := fmt.Sprintf(testMantleBackupConfigTemplate, backupConfigName, namespace, pvcName)
	_, _, err := Kubectl(clusterNo, []byte(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply mantlebackupconfig failed. err: %w", err)
	}

	return nil
}

func ApplyMantleRestoreTemplate(clusterNo int, namespace, restoreName, backupName string) error {
	manifest := fmt.Sprintf(testMantleRestoreTemplate, restoreName, restoreName, namespace, backupName)
	_, _, err := Kubectl(clusterNo, []byte(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply mantlerestore failed. err: %w", err)
	}

	return nil
}

func applyPodMountVolumeTemplate(clusterNo int, namespace, podName, pvcName string) error {
	manifest := fmt.Sprintf(testPodMountVolumeTemplate, podName, namespace, pvcName)
	_, _, err := Kubectl(clusterNo, []byte(manifest), "apply", "-n", namespace, "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply failed. err: %w", err)
	}

	return nil
}

func applyPVCTemplate(clusterNo int, namespace, name, sc string) error {
	manifest := fmt.Sprintf(testPVCTemplate, name, sc)
	_, _, err := Kubectl(clusterNo, []byte(manifest), "apply", "-n", namespace, "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply pvc failed. err: %w", err)
	}

	return nil
}

func ApplyMountDeployTemplate(clusterNo int, namespace, name, pvcName string) error {
	manifest := fmt.Sprintf(mountDeployTemplate, name, namespace, name, name, pvcName)
	_, _, err := Kubectl(clusterNo, []byte(manifest), "apply", "-n", namespace, "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply mount deploy failed. err: %w", err)
	}

	return nil
}

func ApplyWriteJobTemplate(clusterNo int, namespace, name, pvcName string) error {
	manifest := fmt.Sprintf(writeJobTemplate, name, namespace, pvcName)
	_, _, err := Kubectl(clusterNo, []byte(manifest), "apply", "-n", namespace, "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply write job failed. err: %w", err)
	}

	return nil
}

func CreateNamespace(clusterNo int, name string) error {
	_, _, err := Kubectl(clusterNo, nil, "create", "ns", name)
	if err != nil {
		return fmt.Errorf("kubectl create ns failed. err: %w", err)
	}

	return nil
}

func ApplyRBDPoolTemplate(clusterNo int, namespace string) error {
	manifest := fmt.Sprintf(
		testRBDPoolTemplate, namespace)
	_, _, err := Kubectl(clusterNo, []byte(manifest), "apply", "-n", namespace, "-f", "-")
	if err != nil {
		return err
	}

	return nil
}

func ApplySCTemplate(clusterNo int, name, clusterID, namespace string) error {
	manifest := fmt.Sprintf(
		testSCTemplate, name, clusterID, namespace, namespace, namespace)
	_, _, err := Kubectl(clusterNo, []byte(manifest), "apply", "-n", namespace, "-f", "-")
	if err != nil {
		return err
	}

	return nil
}

func GetObject[T any](clusterNo int, kind, namespace, name string) (*T, error) {
	var stdout []byte
	var err error
	if namespace == "" {
		stdout, _, err = Kubectl(clusterNo, nil, "get", kind, name, "-o", "json")
	} else {
		stdout, _, err = Kubectl(clusterNo, nil, "get", kind, "-n", namespace, name, "-o", "json")
	}
	if err != nil {
		return nil, err
	}

	var obj T
	if err := json.Unmarshal(stdout, &obj); err != nil {
		return nil, err
	}

	return &obj, nil
}

func GetCM(clusterNo int, namespace, name string) (*corev1.ConfigMap, error) {
	return GetObject[corev1.ConfigMap](clusterNo, "cm", namespace, name)
}

func GetMB(clusterNo int, namespace, name string) (*mantlev1.MantleBackup, error) {
	return GetObject[mantlev1.MantleBackup](clusterNo, "mantlebackup", namespace, name)
}

func GetPVC(clusterNo int, namespace, name string) (*corev1.PersistentVolumeClaim, error) {
	return GetObject[corev1.PersistentVolumeClaim](clusterNo, "pvc", namespace, name)
}

func GetPV(clusterNo int, name string) (*corev1.PersistentVolume, error) {
	return GetObject[corev1.PersistentVolume](clusterNo, "pv", "", name)
}

func GetMR(clusterNo int, namespace, name string) (*mantlev1.MantleRestore, error) {
	return GetObject[mantlev1.MantleRestore](clusterNo, "mantlerestore", namespace, name)
}

func GetDeploy(clusterNo int, namespace, name string) (*appsv1.Deployment, error) {
	return GetObject[appsv1.Deployment](clusterNo, "deploy", namespace, name)
}

func GetJob(clusterNo int, namespace, name string) (*batchv1.Job, error) {
	return GetObject[batchv1.Job](clusterNo, "job", namespace, name)
}

func GetObjectList[T any](clusterNo int, kind, namespace string) (*T, error) {
	var stdout []byte
	var err error
	if namespace == "" {
		stdout, _, err = Kubectl(clusterNo, nil, "get", kind, "-o", "json")
	} else {
		stdout, _, err = Kubectl(clusterNo, nil, "get", kind, "-n", namespace, "-o", "json")
	}
	if err != nil {
		return nil, err
	}

	var objList T
	if err := json.Unmarshal(stdout, &objList); err != nil {
		return nil, err
	}

	return &objList, nil
}

func GetMBList(clusterNo int, namespace string) (*mantlev1.MantleBackupList, error) {
	return GetObjectList[mantlev1.MantleBackupList](clusterNo, "mantlebackup", namespace)
}

func GetPodList(clusterNo int, namespace string) (*corev1.PodList, error) {
	return GetObjectList[corev1.PodList](clusterNo, "pod", namespace)
}

func GetJobList(clusterNo int, namespace string) (*batchv1.JobList, error) {
	return GetObjectList[batchv1.JobList](clusterNo, "job", namespace)
}

func GetPVCList(clusterNo int, namespace string) (*corev1.PersistentVolumeClaimList, error) {
	return GetObjectList[corev1.PersistentVolumeClaimList](clusterNo, "pvc", namespace)
}

func GetPVList(clusterNo int) (*corev1.PersistentVolumeList, error) {
	return GetObjectList[corev1.PersistentVolumeList](clusterNo, "pv", "")
}

func GetEventList(clusterNo int, namespace string) (*corev1.EventList, error) {
	return GetObjectList[corev1.EventList](clusterNo, "event", namespace)
}

func ChangeClusterRole(clusterNo int, newRole string) error {
	var profileTarget string
	switch clusterNo {
	case PrimaryK8sCluster:
		profileTarget = "minikube-profile-primary"
	case SecondaryK8sCluster:
		profileTarget = "minikube-profile-secondary"
	default:
		return fmt.Errorf("invalid clusterNo: %d", clusterNo)
	}
	if err := runMakeCommand(profileTarget); err != nil {
		return err
	}

	// These role-change tests target the ceph cluster 1 (rook-ceph) controller,
	// so we use the values files generated for CEPH_CLUSTER1_NAMESPACE
	var valuesFile string
	switch newRole {
	case controller.RoleStandalone:
		valuesFile = "testdata/values-mantle1.yaml"
	case controller.RolePrimary:
		valuesFile = "testdata/values-mantle-primary-1.yaml"
	case controller.RoleSecondary:
		valuesFile = "testdata/values-mantle-secondary-1.yaml"
	default:
		return fmt.Errorf("invalid role: %s", newRole)
	}

	return runMakeCommand("install-mantle", "NAMESPACE=rook-ceph", "VALUES_YAML="+valuesFile)
}

type ObjectStorageClient struct {
	cli        *s3.Client
	bucketName string
}

func (c *ObjectStorageClient) listObjects(ctx context.Context) (*s3.ListObjectsV2Output, error) {
	return c.cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &c.bucketName,
	})
}

// CreateObjectStorageClient builds an S3 client for the object storage used by
// the mantle-controller of the given Ceph cluster namespace. The endpoint and
// bucket name are read from that controller's command-line arguments.
func CreateObjectStorageClient(ctx context.Context, cephNamespace string) (*ObjectStorageClient, error) {
	// Find the endpoint of the object storage from the command-line arguments for mantle-controller.
	stdout, _, err := Kubectl(PrimaryK8sCluster, nil,
		"get", "deploy", "-n", cephNamespace, "mantle-controller", "-o", "json")
	if err != nil {
		return nil, fmt.Errorf("failed to get deploy: %w", err)
	}
	var deploy appsv1.Deployment
	if err := json.Unmarshal(stdout, &deploy); err != nil {
		return nil, fmt.Errorf("failed to unmarshal deploy: %w", err)
	}
	args := deploy.Spec.Template.Spec.Containers[0].Args
	endpointIndex := slices.IndexFunc(args, func(s string) bool {
		return strings.HasPrefix(s, "--object-storage-endpoint=")
	})
	if endpointIndex == -1 {
		return nil, errors.New("failed to find object storage endpoint")
	}
	objectStorageEndpoint, _ := strings.CutPrefix(args[endpointIndex], "--object-storage-endpoint=")

	// Find the bucket name from the command-line arguments for mantle-controller.
	bucketNameIndex := slices.IndexFunc(args, func(s string) bool {
		return strings.HasPrefix(s, "--object-storage-bucket-name=")
	})
	if bucketNameIndex == -1 {
		return nil, errors.New("failed to find object storage bucket name")
	}
	objectStorageBucketName, _ := strings.CutPrefix(args[bucketNameIndex], "--object-storage-bucket-name=")

	// Get the credentials from the Secret.
	stdout, _, err = Kubectl(PrimaryK8sCluster, nil,
		"get", "secret", "-n", cephNamespace, "export-data", "-o", "json")
	if err != nil {
		return nil, fmt.Errorf("failed to get export-data secret: %w", err)
	}
	var secret corev1.Secret
	if err := json.Unmarshal(stdout, &secret); err != nil {
		return nil, fmt.Errorf("failed to unmarshal secret: %w", err)
	}
	awsAccessKeyID := secret.Data["AWS_ACCESS_KEY_ID"]
	awsSecretAccessKey := secret.Data["AWS_SECRET_ACCESS_KEY"]

	// Construct a S3 client.
	sdkConfig, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion("ceph"),
		config.WithCredentialsProvider(
			aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     string(awsAccessKeyID),
					SecretAccessKey: string(awsSecretAccessKey),
				}, nil
			}),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load default config: %w", err)
	}
	s3Client := s3.NewFromConfig(sdkConfig, func(o *s3.Options) {
		o.BaseEndpoint = &objectStorageEndpoint
		o.UsePathStyle = true
	})

	return &ObjectStorageClient{cli: s3Client, bucketName: objectStorageBucketName}, nil
}

// IsJobConditionTrue returns true when the conditionType is present and set to
// `metav1.ConditionTrue`.  Otherwise, it returns false.  Note that we can't use
// meta.IsStatusConditionTrue because it doesn't accept []JobCondition.
func IsJobConditionTrue(conditions []batchv1.JobCondition, conditionType batchv1.JobConditionType) bool {
	for _, cond := range conditions {
		if cond.Type == conditionType && cond.Status == corev1.ConditionTrue {
			return true
		}
	}

	return false
}

func GetControllerPodName(clusterNo int) (string, error) {
	stdout, _, err := Kubectl(clusterNo, nil, "get", "pod", "-n", CephCluster1Namespace,
		"-l", "app.kubernetes.io/name=mantle", "-o", "jsonpath={.items[0].metadata.name}")

	return string(stdout), err
}

func WaitControllerToBeReady() {
	GinkgoHelper()
	It("wait for mantle-controller to be ready", func() {
		Eventually(func() error {
			return CheckDeploymentReady(PrimaryK8sCluster, CephCluster1Namespace, "mantle-controller")
		}).Should(Succeed())

		Eventually(func() error {
			return CheckDeploymentReady(SecondaryK8sCluster, CephCluster1Namespace, "mantle-controller")
		}).Should(Succeed())
	})
}

func SetupNamespaces(namespace string) {
	GinkgoHelper()
	By("setting up the environment")
	Eventually(func() error {
		return CreateNamespace(PrimaryK8sCluster, namespace)
	}).Should(Succeed())
	Eventually(func() error {
		return CreateNamespace(SecondaryK8sCluster, namespace)
	}).Should(Succeed())
}

func CleanupMantleBackups(namespace string) {
	GinkgoHelper()
	_, _, err := Kubectl(PrimaryK8sCluster, nil,
		"delete", "mantlebackup", "--all", "-n", namespace, "--timeout=5m", "--ignore-not-found")
	Expect(err).NotTo(HaveOccurred())
	_, _, err = Kubectl(SecondaryK8sCluster, nil,
		"delete", "mantlebackup", "--all", "-n", namespace, "--timeout=5m", "--ignore-not-found")
	Expect(err).NotTo(HaveOccurred())
}

func CreatePod(cluster int, namespace, podName, pvcName string) {
	GinkgoHelper()
	err := applyPodMountVolumeTemplate(cluster, namespace, podName, pvcName)
	Expect(err).NotTo(HaveOccurred())
}

func CreatePVC(ctx SpecContext, cluster int, namespace, name, scName string) {
	GinkgoHelper()
	Eventually(ctx, func() error {
		return applyPVCTemplate(cluster, namespace, name, scName)
	}).Should(Succeed())
}

// SetupFakeClusterID patches the ceph-csi-config on the given cluster so
// that a clusterID is served by the same physical Ceph cluster (rook-ceph2).
func SetupFakeClusterID() {
	GinkgoHelper()
	var wg sync.WaitGroup
	for _, clusterNo := range []int{PrimaryK8sCluster, SecondaryK8sCluster} {
		wg.Add(1)
		go func(clusterNo int) {
			defer GinkgoRecover()
			defer wg.Done()
			setupFakeClusterID(clusterNo)
		}(clusterNo)
	}
	wg.Wait()
}

func setupFakeClusterID(clusterNo int) {
	GinkgoHelper()
	config := make([]*ClusterInfo, 0)
	csiConfig, err := GetCM(clusterNo, CephCluster1Namespace, "ceph-csi-config")
	Expect(err).NotTo(HaveOccurred())
	rawConfig, ok := csiConfig.Data["config.json"]
	Expect(ok).To(BeTrue())
	err = json.Unmarshal([]byte(rawConfig), &config)
	Expect(err).NotTo(HaveOccurred())
	Expect(config).To(HaveLen(1), "ceph-csi-config must contain exactly one cluster info")

	fakeConfig := []*ClusterInfo{
		config[0],
		{
			ClusterID:    CephCluster2Namespace,
			Monitors:     config[0].Monitors,
			CephFS:       config[0].CephFS,
			RBD:          config[0].RBD,
			NFS:          config[0].NFS,
			ReadAffinity: config[0].ReadAffinity,
		},
	}
	rawFakeConfig, err := json.Marshal(fakeConfig)
	Expect(err).NotTo(HaveOccurred())
	newConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ceph-csi-config",
			Namespace: CephCluster1Namespace,
		},
		Data: map[string]string{
			"config.json": string(rawFakeConfig),
		},
	}
	err = ApplyObject(clusterNo, newConfig)
	Expect(err).NotTo(HaveOccurred())

	// Add a `Sleep` to avoid the ceph csi pod reading an outdated ConfigMap.
	time.Sleep(10 * time.Second)
	RestartWorkload(clusterNo, "deployment", CephCluster1Namespace, "rook-ceph.rbd.csi.ceph.com-ctrlplugin")
	RestartWorkload(clusterNo, "daemonset", CephCluster1Namespace, "rook-ceph.rbd.csi.ceph.com-nodeplugin")

	err = ApplySCTemplate(clusterNo, SCName2, CephCluster2Namespace, CephCluster1Namespace)
	Expect(err).NotTo(HaveOccurred())
	WaitStorageClassProvisionable(clusterNo, CephCluster1Namespace, SCName2)
}

// WaitStorageClassProvisionable verifies that the given StorageClass can
// actually provision a volume, by creating a probe PVC and waiting for it to be
// bound.
func WaitStorageClassProvisionable(cluster int, namespace, scName string) {
	GinkgoHelper()
	pvcName := util.GetUniqueName("probe-pvc-")
	By(fmt.Sprintf("probing StorageClass %s is provisionable @%d:%s/%s", scName, cluster, namespace, pvcName))
	Eventually(func() error {
		return applyPVCTemplate(cluster, namespace, pvcName, scName)
	}, "10m", "5s").Should(Succeed())
	Eventually(func(g Gomega) {
		pvc, err := GetPVC(cluster, namespace, pvcName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(pvc.Status.Phase).To(Equal(corev1.ClaimBound))
	}, "10m", "5s").Should(Succeed())

	_, stderr, err := Kubectl(cluster, nil,
		"delete", "pvc", "-n", namespace, pvcName, "--timeout=3m", "--ignore-not-found")
	Expect(err).NotTo(HaveOccurred(), "failed to delete probe PVC. stderr: %s", string(stderr))
}

func WriteRandomDataToPV(ctx SpecContext, cluster int, namespace, pvcName string) string {
	GinkgoHelper()
	By("writing some random data to PV(C)")
	writeJobName := util.GetUniqueName("job-")
	Eventually(ctx, func() error {
		return ApplyWriteJobTemplate(cluster, namespace, writeJobName, pvcName)
	}).Should(Succeed())
	Eventually(ctx, func(g Gomega) {
		job, err := GetJob(cluster, namespace, writeJobName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete)).To(BeTrue())
	}).Should(Succeed())
	stdout, _, err := Kubectl(cluster, nil, "logs", "-n", namespace, "job/"+writeJobName)
	Expect(err).NotTo(HaveOccurred())
	Expect(stdout).NotTo(BeEmpty())

	return string(stdout)
}

func CreateMantleBackup(cluster int, namespace, pvcName, backupName string) {
	GinkgoHelper()
	By(fmt.Sprintf("creating a MantleBackup object @%d:%s/%s", cluster, namespace, backupName))
	Eventually(func() error {
		return ApplyMantleBackupTemplate(cluster, namespace, pvcName, backupName)
	}).Should(Succeed())
}

func CreateMantleBackupConfig(cluster int, namespace, pvcName, backupConfigName string) {
	GinkgoHelper()
	By(fmt.Sprintf("creating a MantleBackupConfig object @%d:%s/%s for %s", cluster, namespace, backupConfigName, pvcName))
	Eventually(func() error {
		return ApplyMantleBackupConfigTemplate(cluster, namespace, pvcName, backupConfigName)
	}).Should(Succeed())
}

func WaitMantleBackupSnapshotCaptured(cluster int, namespace, backupName string) {
	GinkgoHelper()
	By("checking MantleBackup's SnapshotCaptured status")
	Eventually(func() error {
		mb, err := GetMB(cluster, namespace, backupName)
		if err != nil {
			return err
		}
		if !mb.IsSnapshotCaptured() {
			return errors.New("status of SnapshotCaptured condition is not True")
		}

		return nil
	}, "10m", "1s").Should(Succeed())
}

func WaitMantleBackupVerified(cluster int, namespace, backupName string) {
	GinkgoHelper()
	By("checking MantleBackup's Verified status")
	Eventually(func() error {
		mb, err := GetMB(cluster, namespace, backupName)
		if err != nil {
			return err
		}
		if !mb.IsVerifiedTrue() {
			return errors.New("status of Verified condition is not True")
		}

		return nil
	}, "10m", "1s").Should(Succeed())
}

func WaitMantleBackupSynced(namespace, backupName string) {
	GinkgoHelper()
	By(fmt.Sprintf("checking MantleBackup's SyncedToRemote status: %s/%s", namespace, backupName))
	Eventually(func() error {
		mb, err := GetMB(PrimaryK8sCluster, namespace, backupName)
		if err != nil {
			return err
		}
		if !mb.IsSynced() {
			return errors.New("status of SyncedToRemote condition is not True")
		}

		return nil
	}, "10m", "1s").Should(Succeed())
}

func EnsureMantleBackupExists(ctx SpecContext, cluster int, namespace, backupName string) {
	GinkgoHelper()
	By("checking MantleBackup exists")
	_, err := GetMB(cluster, namespace, backupName)
	Expect(err).NotTo(HaveOccurred())
}

func EnsureMantleBackupNotExist(ctx SpecContext, cluster int, namespace, backupName string) {
	GinkgoHelper()
	By("checking MantleBackup doesn't exist")
	Consistently(ctx, func(g Gomega) {
		mbs, err := GetMBList(cluster, namespace)
		g.Expect(err).NotTo(HaveOccurred())
		exist := slices.ContainsFunc(mbs.Items, func(mb mantlev1.MantleBackup) bool {
			return mb.GetName() == backupName
		})
		g.Expect(exist).To(BeFalse())
	}, "10s").Should(Succeed())
}

func EnsureCorrectRestoration(
	clusterNo int,
	ctx SpecContext,
	namespace, backupName, restoreName, writtenDataHash string,
) {
	GinkgoHelper()
	mountDeployName := util.GetUniqueName("deploy-")
	clusterName := GetClusterName(clusterNo)
	By(fmt.Sprintf("%s: %s: creating MantleRestore by using the MantleBackup replicated above",
		clusterName, backupName))
	Eventually(ctx, func() error {
		return ApplyMantleRestoreTemplate(clusterNo, namespace, restoreName, backupName)
	}).Should(Succeed())
	By(fmt.Sprintf("%s: %s: checking the MantleRestore can be ready to use", clusterName, backupName))
	Eventually(ctx, func(g Gomega) {
		mr, err := GetMR(clusterNo, namespace, restoreName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(mr.IsReady()).To(BeTrue())
	}).Should(Succeed())
	By(fmt.Sprintf("%s: %s: checking the MantleRestore has the correct contents", clusterName, backupName))
	Eventually(ctx, func(g Gomega) {
		err := ApplyMountDeployTemplate(clusterNo, namespace, mountDeployName, restoreName)
		g.Expect(err).NotTo(HaveOccurred())
		stdout, stderr, err := Kubectl(clusterNo, nil, "exec", "-n", namespace, "deploy/"+mountDeployName, "--",
			"bash", "-c", "sha256sum /volume/data | awk '{print $1}'")
		g.Expect(err).NotTo(HaveOccurred(), "stderr: %s", string(stderr))
		g.Expect(string(stdout)).To(Equal(writtenDataHash))
	}).Should(Succeed())
}

func ResumeObjectStorage(ctx SpecContext) {
	GinkgoHelper()
	By("resuming the object storage")
	_, _, err := Kubectl(
		SecondaryK8sCluster,
		nil,
		"patch",
		"cephobjectstore",
		"-n",
		CephCluster1Namespace,
		"ceph-object-store",
		"--type",
		"json",
		"-p",
		`[{"op": "replace", "path": "/spec/gateway/placement", "value":{}}]`,
	)
	Expect(err).NotTo(HaveOccurred())

	_, _, err = Kubectl(
		SecondaryK8sCluster,
		nil,
		"delete", "-n", CephCluster1Namespace, "deploy", RgwDeployName,
	)
	Expect(err).NotTo(HaveOccurred())

	By("waiting for the RGW pods to be ready")
	Eventually(ctx, func(g Gomega) {
		pods, err := GetPodList(SecondaryK8sCluster, CephCluster1Namespace)
		g.Expect(err).NotTo(HaveOccurred())
		index := slices.IndexFunc(pods.Items, func(pod corev1.Pod) bool {
			return strings.HasPrefix(pod.GetName(), RgwDeployName)
		})
		g.Expect(index).NotTo(Equal(-1))
		for _, s := range pods.Items[index].Status.ContainerStatuses {
			g.Expect(s.Ready).To(BeTrue())
		}
	}).Should(Succeed())
}

// PauseObjectStorage pauses the object storage by patching the CephObjectStore
// resource, which makes Rook attempt to deploy RGW pods on nodes that don't
// exist. Note that setting /spec/gateway/instances to 0 won't work for this
// purpose, as Rook does not allow it.
// cf. https://github.com/rook/rook/blob/8767bf263e47f4c8c0c72fafccee71e732088b97/pkg/operator/ceph/object/rgw.go#L103-L107
//
//nolint:lll
func PauseObjectStorage(ctx SpecContext) {
	GinkgoHelper()
	By("pausing the object storage")
	_, _, err := Kubectl(
		SecondaryK8sCluster,
		nil,
		"patch",
		"-n",
		CephCluster1Namespace,
		"cephobjectstore",
		"ceph-object-store",
		"--type",
		"json",
		"-p",
		`[{
			"op": "replace",
			"path": "/spec/gateway/placement",
			"value":{
				"nodeAffinity":{
					"requiredDuringSchedulingIgnoredDuringExecution": {
						"nodeSelectorTerms": [{
							"matchExpressions": [{
								"key":"role",
								"operator":"In",
								"values":["non-existing-node"]
							}]
						}]
					}
				}
			}
		}]`,
	)
	Expect(err).NotTo(HaveOccurred())

	By("waiting for the RGW pods to be terminated")
	Eventually(ctx, func(g Gomega) {
		pods, err := GetPodList(SecondaryK8sCluster, CephCluster1Namespace)
		g.Expect(err).NotTo(HaveOccurred())
		exist := slices.ContainsFunc(pods.Items, func(pod corev1.Pod) bool {
			return strings.HasPrefix(pod.GetName(), RgwDeployName)
		})
		g.Expect(exist).To(BeTrue())
	}).Should(Succeed())
}

func ListRBDSnapshotsInPVC(cluster int, namespace, pvcName string) ([]ceph.RBDSnapshot, error) {
	pvc, err := GetPVC(cluster, namespace, pvcName)
	if err != nil {
		return nil, fmt.Errorf("failed to get PVC: %w", err)
	}
	pv, err := GetPV(cluster, pvc.Spec.VolumeName)
	if err != nil {
		return nil, fmt.Errorf("failed to get PV: %w", err)
	}
	cmd := createCephCmd(cluster)
	snaps, err := cmd.RBDSnapLs("rook-ceph-block", pv.Spec.CSI.VolumeAttributes["imageName"])
	if err != nil {
		return nil, fmt.Errorf("failed to create ceph cmd: %w", err)
	}

	return snaps, nil
}

func FindRBDSnapshotInPVC(cluster int, namespace, pvcName, snapName string) (*ceph.RBDSnapshot, error) {
	snaps, err := ListRBDSnapshotsInPVC(SecondaryK8sCluster, namespace, pvcName)
	if err != nil {
		return nil, fmt.Errorf("failed to list RBD snapshots in PVC: %s/%s: %w",
			namespace, pvcName, err)
	}
	index := slices.IndexFunc(snaps, func(snap ceph.RBDSnapshot) bool { return snap.Name == snapName })
	if index == -1 {
		return nil, fmt.Errorf("failed to find the RBD snapshot in PVC: %s/%s: %s: %w", namespace, pvcName, snapName, err)
	}

	return &snaps[index], nil
}

func EnsurePVCHasSnapshot(cluster int, namespace, pvcName, snapName string) {
	GinkgoHelper()
	By("checking the PVC has a snapshot")
	snaps, err := ListRBDSnapshotsInPVC(cluster, namespace, pvcName)
	Expect(err).NotTo(HaveOccurred())
	exists := slices.ContainsFunc(snaps, func(snap ceph.RBDSnapshot) bool {
		return snap.Name == snapName
	})
	Expect(exists).To(BeTrue())
}

func EnsurePVCHasNoSnapshots(cluster int, namespace, pvcName string) {
	GinkgoHelper()
	By("checking PVC has no snapshots")
	snaps, err := ListRBDSnapshotsInPVC(cluster, namespace, pvcName)
	Expect(err).NotTo(HaveOccurred())
	Expect(snaps).To(BeEmpty())
}

func createCephCmd(cluster int) ceph.CephCmd {
	kubectl, err := getKubectlInvocation(cluster)
	if err != nil {
		panic(err)
	}

	return ceph.NewCephCmdWithToolsAndCustomKubectl(kubectl, CephCluster1Namespace)
}

func WaitUploadJobCreated(ctx SpecContext, cluster int, namespace, backupName string, partNum int) {
	GinkgoHelper()
	By("waiting for an upload job to be created")
	Eventually(ctx, func(g Gomega) {
		mb, err := GetMB(cluster, namespace, backupName)
		g.Expect(err).NotTo(HaveOccurred())
		cephNS, err := cephNamespaceOf(mb)
		g.Expect(err).NotTo(HaveOccurred())
		jobs, err := GetJobList(cluster, cephNS)
		g.Expect(err).NotTo(HaveOccurred())
		exist := slices.ContainsFunc(jobs.Items, func(job batchv1.Job) bool {
			return job.GetName() == controller.MakeUploadJobName(mb, partNum)
		})
		g.Expect(exist).To(BeTrue())
	}).Should(Succeed())
}

func CheckJobExist(clusterNo int, namespace, jobName string) bool {
	GinkgoHelper()
	jobs, err := GetJobList(clusterNo, namespace)
	Expect(err).NotTo(HaveOccurred())

	return slices.ContainsFunc(jobs.Items, func(job batchv1.Job) bool {
		return job.GetName() == jobName
	})
}

// cephNamespaceOf returns the Ceph cluster namespace that manages the given
// MantleBackup. It is identified by the cluster-id label, which equals
// the managing controller's managedCephClusterID.
func cephNamespaceOf(backup *mantlev1.MantleBackup) (string, error) {
	clusterID, ok := backup.GetLabels()["mantle.cybozu.io/cluster-id"]
	if !ok {
		return "", fmt.Errorf("MantleBackup %s/%s does not have the mantle.cybozu.io/cluster-id label yet",
			backup.GetNamespace(), backup.GetName())
	}

	return clusterID, nil
}

func WaitComponentJobsDeleted(
	ctx SpecContext,
	cluster int,
	namespace,
	componentPrefix string,
	backup *mantlev1.MantleBackup,
) {
	GinkgoHelper()
	By("waiting for jobs to be deleted")
	Eventually(ctx, func(g Gomega) {
		jobs, err := GetJobList(cluster, namespace)
		g.Expect(err).NotTo(HaveOccurred())
		exist := slices.ContainsFunc(jobs.Items, func(job batchv1.Job) bool {
			_, ok := controller.ExtractPartNumFromComponentJobName(componentPrefix, job.GetName(), backup)

			return ok
		})
		g.Expect(exist).To(BeFalse())
	}).Should(Succeed())
}

func WaitJobDeleted(ctx SpecContext, cluster int, namespace, jobName string) {
	GinkgoHelper()
	By("waiting for a Job to be deleted")
	Eventually(ctx, func(g Gomega) {
		jobs, err := GetJobList(cluster, namespace)
		g.Expect(err).NotTo(HaveOccurred())
		exist := slices.ContainsFunc(jobs.Items, func(job batchv1.Job) bool {
			return job.GetName() == jobName
		})
		g.Expect(exist).To(BeFalse())
	}).Should(Succeed())
}

func WaitTemporaryPrimaryJobsDeleted(ctx SpecContext, primaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	cephNS, err := cephNamespaceOf(primaryMB)
	Expect(err).NotTo(HaveOccurred())
	WaitComponentJobsDeleted(ctx, PrimaryK8sCluster, cephNS,
		controller.MantleExportJobPrefix, primaryMB)
	WaitComponentJobsDeleted(ctx, PrimaryK8sCluster, cephNS,
		controller.MantleUploadJobPrefix, primaryMB)
	WaitJobDeleted(ctx, PrimaryK8sCluster, cephNS, controller.MakeVerifyJobName(primaryMB))
}

func WaitTemporarySecondaryJobsDeleted(ctx SpecContext, secondaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	cephNS, err := cephNamespaceOf(secondaryMB)
	Expect(err).NotTo(HaveOccurred())
	WaitComponentJobsDeleted(ctx, SecondaryK8sCluster, cephNS,
		controller.MantleImportJobPrefix, secondaryMB)
	WaitComponentJobsDeleted(ctx, SecondaryK8sCluster, cephNS,
		controller.MantleZeroOutJobPrefix, secondaryMB)
	WaitJobDeleted(ctx, SecondaryK8sCluster, cephNS, controller.MakeVerifyJobName(secondaryMB))
}

func WaitTemporaryJobsDeleted(ctx SpecContext, primaryMB, secondaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitTemporaryPrimaryJobsDeleted(ctx, primaryMB)
	WaitTemporarySecondaryJobsDeleted(ctx, secondaryMB)
}

func WaitPVCDeleted(ctx SpecContext, cluster int, namespace, pvcName string) {
	GinkgoHelper()
	By("waiting for a PVC to be deleted")
	Eventually(ctx, func(g Gomega) {
		pvcs, err := GetPVCList(cluster, namespace)
		g.Expect(err).NotTo(HaveOccurred())
		exist := slices.ContainsFunc(pvcs.Items, func(pvc corev1.PersistentVolumeClaim) bool {
			return pvc.GetName() == pvcName
		})
		g.Expect(exist).To(BeFalse())
	}).Should(Succeed())
}

func WaitPVCsDeleted(ctx SpecContext, cluster int, namespace, pvcNamePrefix string) {
	GinkgoHelper()
	By("waiting for PVCs to be deleted")
	Eventually(ctx, func(g Gomega) {
		pvcs, err := GetPVCList(cluster, namespace)
		g.Expect(err).NotTo(HaveOccurred())
		exist := slices.ContainsFunc(pvcs.Items, func(pvc corev1.PersistentVolumeClaim) bool {
			return strings.HasPrefix(pvc.GetName(), pvcNamePrefix)
		})
		g.Expect(exist).To(BeFalse())
	}).Should(Succeed())
}

func WaitTemporaryPrimaryPVCsDeleted(ctx SpecContext, primaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	cephNS, err := cephNamespaceOf(primaryMB)
	Expect(err).NotTo(HaveOccurred())
	WaitPVCsDeleted(ctx, PrimaryK8sCluster, cephNS,
		fmt.Sprintf("%s%s-", controller.MantleExportDataPVCPrefix, string(primaryMB.GetUID())))
	WaitPVCDeleted(ctx, PrimaryK8sCluster, cephNS, controller.MakeVerifyPVCName(primaryMB))
}

func WaitTemporarySecondaryPVCsDeleted(ctx SpecContext, secondaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	cephNS, err := cephNamespaceOf(secondaryMB)
	Expect(err).NotTo(HaveOccurred())
	WaitPVCDeleted(ctx, SecondaryK8sCluster, cephNS, controller.MakeZeroOutPVCName(secondaryMB))
	WaitPVCDeleted(ctx, SecondaryK8sCluster, cephNS, controller.MakeVerifyPVCName(secondaryMB))
}

func WaitTemporaryPVCsDeleted(ctx SpecContext, primaryMB, secondaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitTemporaryPrimaryPVCsDeleted(ctx, primaryMB)
	WaitTemporarySecondaryPVCsDeleted(ctx, secondaryMB)
}

func WaitPVDeleted(ctx SpecContext, cluster int, pvName string) {
	GinkgoHelper()
	By("waiting for a PV to be deleted")
	Eventually(ctx, func(g Gomega) {
		pvs, err := GetPVList(cluster)
		g.Expect(err).NotTo(HaveOccurred())
		exist := slices.ContainsFunc(pvs.Items, func(pv corev1.PersistentVolume) bool {
			return pv.GetName() == pvName
		})
		g.Expect(exist).To(BeFalse())
	}).Should(Succeed())
}

func WaitTemporaryPrimaryPVsDeleted(ctx SpecContext, primaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitPVDeleted(ctx, PrimaryK8sCluster, controller.MakeVerifyPVName(primaryMB))
}

func WaitTemporarySecondaryPVsDeleted(ctx SpecContext, secondaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitPVDeleted(ctx, SecondaryK8sCluster, controller.MakeZeroOutPVName(secondaryMB))
	WaitPVDeleted(ctx, SecondaryK8sCluster, controller.MakeVerifyPVName(secondaryMB))
}

func WaitTemporaryPVsDeleted(ctx SpecContext, primaryMB, secondaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitTemporaryPrimaryPVsDeleted(ctx, primaryMB)
	WaitTemporarySecondaryPVsDeleted(ctx, secondaryMB)
}

func WaitTemporaryS3ObjectsDeleted(ctx SpecContext, primaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	By("waiting for the temporary s3 objects to be deleted")
	expectedObjectNamePrefix := fmt.Sprintf("%s-%s-", primaryMB.GetName(), string(primaryMB.GetUID()))
	cephNamespace, err := cephNamespaceOf(primaryMB)
	Expect(err).NotTo(HaveOccurred())
	Eventually(ctx, func(g Gomega) {
		objectStorageClient, err := CreateObjectStorageClient(ctx, cephNamespace)
		g.Expect(err).NotTo(HaveOccurred())
		listOutput, err := objectStorageClient.listObjects(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		for _, c := range listOutput.Contents {
			g.Expect(c.Key).NotTo(BeNil())
			g.Expect(strings.HasPrefix(*c.Key, expectedObjectNamePrefix)).To(BeFalse())
		}
	}).Should(Succeed())
}

func WaitVerifyRBDImageDeleted(ctx SpecContext, cluster int, backup *mantlev1.MantleBackup) {
	GinkgoHelper()
	Eventually(ctx, func(g Gomega) {
		cmd := createCephCmd(cluster)
		images, err := cmd.RBDLs("rook-ceph-block")
		g.Expect(err).NotTo(HaveOccurred())
		image := controller.MakeVerifyImageName(backup)
		g.Expect(slices.Contains(images, image)).To(BeFalse())
	}).Should(Succeed())
}

func WaitTemporaryResourcesDeleted(ctx SpecContext, primaryMB, secondaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitTemporaryJobsDeleted(ctx, primaryMB, secondaryMB)
	WaitTemporaryPVCsDeleted(ctx, primaryMB, secondaryMB)
	WaitTemporaryPVsDeleted(ctx, primaryMB, secondaryMB)
	WaitVerifyRBDImageDeleted(ctx, PrimaryK8sCluster, primaryMB)
	WaitVerifyRBDImageDeleted(ctx, SecondaryK8sCluster, secondaryMB)
	WaitTemporaryS3ObjectsDeleted(ctx, primaryMB)
}

func DeleteMantleBackup(cluster int, namespace, backupName string) {
	GinkgoHelper()
	By("deleting MantleBackup")
	stdout, stderr, err := Kubectl(cluster, nil, "delete", "-n", namespace, "mantlebackup", backupName, "--timeout=3m")
	Expect(err).NotTo(HaveOccurred(), "stdout: %s, stderr: %s", string(stdout), string(stderr))
}

func GetBackupTransferPartSize() (*resource.Quantity, error) {
	deployMC, err := GetDeploy(PrimaryK8sCluster, CephCluster1Namespace, MantleControllerDeployName)
	if err != nil {
		return nil, fmt.Errorf("failed to get mantle-controller deploy: %w", err)
	}
	args := deployMC.Spec.Template.Spec.Containers[0].Args
	backupTransferPartSizeIndex := slices.IndexFunc(
		args,
		func(arg string) bool { return strings.HasPrefix(arg, "--backup-transfer-part-size=") },
	)
	if backupTransferPartSizeIndex == -1 {
		return nil, errors.New("failed to find --backup-transfer-part-size= argument")
	}
	qty, ok := strings.CutPrefix(args[backupTransferPartSizeIndex], "--backup-transfer-part-size=")
	if !ok {
		return nil, errors.New("failed to parse --backup-transfer-part-size= argument")
	}
	qtyParsed, err := resource.ParseQuantity(qty)
	if err != nil {
		return nil, fmt.Errorf("failed to parse --backup-transfer-part-size= argument: %w", err)
	}

	return &qtyParsed, nil
}

func GetNumberOfBackupParts(snapshotSize *resource.Quantity) (int, error) {
	backupTransferPartSize, err := GetBackupTransferPartSize()
	if err != nil {
		return 0, fmt.Errorf("failed to get backup transfer part size :%w", err)
	}
	var expectedNumOfParts int64 = 1
	if snapshotSize.Cmp(*backupTransferPartSize) > 0 { // pvcSize > backupTransferPartSize
		backupTransferPartSize, ok := backupTransferPartSize.AsInt64()
		if !ok {
			return 0, errors.New("failed to convert backup transfer part size to i64")
		}
		pvcSizeI64, ok := snapshotSize.AsInt64()
		if !ok {
			return 0, errors.New("failed to convert pvc size to i64")
		}
		expectedNumOfParts = pvcSizeI64 / backupTransferPartSize
		if pvcSizeI64%backupTransferPartSize != 0 {
			expectedNumOfParts++
		}
	}

	return int(expectedNumOfParts), nil
}

// changeMantleControllerArg sets the CLI argument named flag (e.g.,
// "--max-export-data-pvcs") of the mantle-controller in the primary cluster to
// value. If value is nil, the argument is removed so that the controller falls
// back to its default value. It waits until the rollout completes, so the new
// controller Pod is guaranteed to run with the updated arguments when this
// function returns.
func changeMantleControllerArg(flag string, value *string) {
	GinkgoHelper()

	deployMC, err := GetDeploy(PrimaryK8sCluster, CephCluster1Namespace, MantleControllerDeployName)
	Expect(err).NotTo(HaveOccurred())

	args := deployMC.Spec.Template.Spec.Containers[0].Args
	argIndex := slices.IndexFunc(
		args,
		func(arg string) bool { return strings.HasPrefix(arg, flag+"=") },
	)

	var patch string
	switch {
	case value == nil && argIndex == -1:
		return
	case value == nil:
		patch = fmt.Sprintf(
			`[{"op": "remove", "path": "/spec/template/spec/containers/0/args/%d"}]`,
			argIndex,
		)
	case argIndex == -1:
		patch = fmt.Sprintf(
			`[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", `+
				`"value":"%s=%s"}]`,
			flag,
			*value,
		)
	default:
		patch = fmt.Sprintf(
			`[{"op": "replace", "path": "/spec/template/spec/containers/0/args/%d", `+
				`"value":"%s=%s"}]`,
			argIndex,
			flag,
			*value,
		)
	}

	_, _, err = Kubectl(
		PrimaryK8sCluster, nil,
		"patch", "deploy", "-n", CephCluster1Namespace, MantleControllerDeployName, "--type=json", "-p="+patch,
	)
	Expect(err).NotTo(HaveOccurred())

	_, _, err = Kubectl(
		PrimaryK8sCluster, nil,
		"rollout", "status", "-n", CephCluster1Namespace,
		"deploy/"+MantleControllerDeployName, "--timeout=3m",
	)
	Expect(err).NotTo(HaveOccurred())
}

// ChangeBackupTransferPartSize sets --backup-transfer-part-size of the
// mantle-controller in the primary cluster to size, and waits until the
// rollout completes.
func ChangeBackupTransferPartSize(size string) {
	GinkgoHelper()

	changeMantleControllerArg("--backup-transfer-part-size", &size)
}

// ChangeMaxExportDataPVCs sets --max-export-data-pvcs of the mantle-controller
// in the primary cluster to count. If count is nil, the argument is removed so
// that the controller falls back to its default value. It waits until the
// rollout completes.
func ChangeMaxExportDataPVCs(count *int) {
	GinkgoHelper()

	var value *string
	if count != nil {
		v := strconv.Itoa(*count)
		value = &v
	}
	changeMantleControllerArg("--max-export-data-pvcs", value)
}

func ChangeComponentJobScript(
	ctx SpecContext,
	cluster int,
	envName,
	namespace,
	backupName string,
	partNum int,
	script *string,
) {
	GinkgoHelper()

	deployMC, err := GetDeploy(cluster, CephCluster1Namespace, MantleControllerDeployName)
	Expect(err).NotTo(HaveOccurred())

	env := deployMC.Spec.Template.Spec.Containers[0].Env
	envIndex := slices.IndexFunc(
		env,
		func(e corev1.EnvVar) bool { return e.Name == envName },
	)

	type jsonPatch struct {
		OP    string        `json:"op"`
		Path  string        `json:"path"`
		Value corev1.EnvVar `json:"value"`
	}
	var patch []jsonPatch

	switch {
	case envIndex == -1 && script == nil:
		// nothing to do
		return

	case envIndex == -1 && script != nil:
		patch = append(patch, jsonPatch{
			OP:   "add",
			Path: "/spec/template/spec/containers/0/env/-",
			Value: corev1.EnvVar{
				Name:  envName,
				Value: *script,
			},
		})

	case envIndex != -1 && script == nil:
		patch = append(patch, jsonPatch{
			OP:   "remove",
			Path: fmt.Sprintf("/spec/template/spec/containers/0/env/%d", envIndex),
		})

	case envIndex != -1 && script != nil:
		patch = append(patch, jsonPatch{
			OP:   "replace",
			Path: fmt.Sprintf("/spec/template/spec/containers/0/env/%d", envIndex),
			Value: corev1.EnvVar{
				Name:  envName,
				Value: *script,
			},
		})
	}

	marshalledPatch, err := json.Marshal(patch)
	Expect(err).NotTo(HaveOccurred())

	By("patching the controller manifest for " + envName)
	_, _, err = Kubectl(
		cluster, nil,
		"patch", "deploy", "-n", CephCluster1Namespace, MantleControllerDeployName, "--type=json",
		fmt.Sprintf("--patch=%s", marshalledPatch),
	)
	Expect(err).NotTo(HaveOccurred())

	By("waiting until the controller Pod starts running")
	Eventually(ctx, func(g Gomega) {
		stdout, _, err := Kubectl(cluster, nil, "get", "pod", "-n", CephCluster1Namespace, "-o", "json")
		g.Expect(err).NotTo(HaveOccurred())
		var pods corev1.PodList
		err = json.Unmarshal(stdout, &pods)
		g.Expect(err).NotTo(HaveOccurred())

		for _, pod := range pods.Items {
			if !strings.HasPrefix(pod.GetName(), MantleControllerDeployName) {
				continue
			}
			index := slices.IndexFunc(pod.Spec.Containers[0].Env, func(e corev1.EnvVar) bool {
				return e.Name == envName
			})
			if script == nil {
				g.Expect(index).To(Equal(-1))
			} else {
				g.Expect(index).NotTo(Equal(-1))
				g.Expect(pod.Spec.Containers[0].Env[index].Value).To(Equal(*script))
			}
		}
	}).Should(Succeed())
}

func WaitControllerLog(ctx SpecContext, clusterNo int, pattern string, duration time.Duration) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	matcher := regexp.MustCompile(pattern)

	fields, err := getKubectlInvocation(clusterNo)
	if err != nil {
		panic(err)
	}
	fields = append(fields, "logs", "-n", CephCluster1Namespace, "deployment/"+MantleControllerDeployName, "-f")

	command := exec.CommandContext(timeoutCtx, fields[0], fields[1:]...)
	stdoutPipe, err := command.StdoutPipe()
	if err != nil {
		panic(err)
	}
	err = command.Start()
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	}()

	// read stdout line by line until the pattern is found
	scanner := bufio.NewScanner(stdoutPipe)
	found := make(chan struct{})
	go func() {
		for scanner.Scan() {
			select {
			case <-timeoutCtx.Done():
				return
			default:
			}
			line := scanner.Text()
			if matcher.MatchString(line) {
				close(found)

				return
			}
		}
		if scanner.Err() != nil {
			panic(scanner.Err())
		}
	}()

	select {
	case <-timeoutCtx.Done():
		return timeoutCtx.Err()
	case <-found:
		return nil
	}
}

func CountMantleControllerPods(cluster int) int {
	GinkgoHelper()
	pods, err := GetPodList(cluster, CephCluster1Namespace)
	Expect(err).NotTo(HaveOccurred())
	count := 0
	for _, pod := range pods.Items {
		if pod.Labels["app.kubernetes.io/name"] == "mantle" &&
			pod.Labels["app.kubernetes.io/component"] == "controller" {
			count++
		}
	}

	return count
}

func RestartWorkload(cluster int, kind, ns, name string) {
	GinkgoHelper()
	_, stderr, err := Kubectl(cluster, nil, "rollout", "restart", kind, "-n", ns, name)
	Expect(err).NotTo(HaveOccurred(), "failed to restart %s(%s/%s) stderr: %s", kind, ns, name, string(stderr))
}
