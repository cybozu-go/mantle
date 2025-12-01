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
	"strings"
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
)

const (
	CephClusterNamespace       = "rook-ceph"
	PrimaryK8sCluster          = 1
	SecondaryK8sCluster        = 2
	RgwDeployName              = "rook-ceph-rgw-ceph-object-store-a"
	MantleControllerDeployName = "mantle-controller"
)

var (
	//go:embed testdata/pvc-template.yaml
	testPVCTemplate string
	//go:embed testdata/pod-mount-volume-template.yaml
	testPodMountVolumeTemplate string
	//go:embed testdata/rbd-pool-sc-template.yaml
	testRBDPoolSCTemplate string
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

// input can be nil
func Kubectl(clusterNo int, input []byte, args ...string) ([]byte, []byte, error) {
	fields, err := getKubectlInvocation(clusterNo)
	if err != nil {
		panic(err)
	}
	fields = append(fields, args...)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

func applyPVCTemplate(clusterNo int, namespace, name string) error {
	manifest := fmt.Sprintf(testPVCTemplate, name)
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

func ApplyRBDPoolAndSCTemplate(clusterNo int, namespace string) error { //nolint:unparam
	manifest := fmt.Sprintf(
		testRBDPoolSCTemplate, namespace,
		namespace, namespace, namespace, namespace)
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

	var valuesFile string
	switch newRole {
	case controller.RoleStandalone:
		valuesFile = "testdata/values-mantle1.yaml"
	case controller.RolePrimary:
		valuesFile = "testdata/values-mantle-primary.yaml"
	case controller.RoleSecondary:
		valuesFile = "testdata/values-mantle-secondary.yaml"
	default:
		return fmt.Errorf("invalid role: %s", newRole)
	}

	return runMakeCommand("install-mantle", "NAMESPACE=rook-ceph", "HELM_RELEASE=mantle", "VALUES_YAML="+valuesFile)
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

func CreateObjectStorageClient(ctx context.Context) (*ObjectStorageClient, error) {
	// Find the endpoint of the object storage from the command-line arguments for mantle-controller.
	stdout, _, err := Kubectl(PrimaryK8sCluster, nil,
		"get", "deploy", "-n", CephClusterNamespace, "mantle-controller", "-o", "json")
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

	// Get the bucket name from the OBC.
	stdout, _, err = Kubectl(SecondaryK8sCluster, nil,
		"get", "obc", "-n", CephClusterNamespace, "export-data", "-o", "json")
	if err != nil {
		return nil, fmt.Errorf("failed to get obc: %w", err)
	}
	var obc struct {
		Spec struct {
			BucketName string `json:"bucketName"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(stdout, &obc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal obc: %w", err)
	}

	// Get the credentials from the Secret.
	stdout, _, err = Kubectl(SecondaryK8sCluster, nil,
		"get", "secret", "-n", CephClusterNamespace, "export-data", "-o", "json")
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

	return &ObjectStorageClient{cli: s3Client, bucketName: obc.Spec.BucketName}, nil
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
	stdout, _, err := Kubectl(clusterNo, nil, "get", "pod", "-n", CephClusterNamespace,
		"-l", "app.kubernetes.io/name=mantle", "-o", "jsonpath={.items[0].metadata.name}")
	return string(stdout), err
}

func WaitControllerToBeReady() {
	GinkgoHelper()
	It("wait for mantle-controller to be ready", func() {
		Eventually(func() error {
			return CheckDeploymentReady(PrimaryK8sCluster, CephClusterNamespace, "mantle-controller")
		}).Should(Succeed())

		Eventually(func() error {
			return CheckDeploymentReady(SecondaryK8sCluster, CephClusterNamespace, "mantle-controller")
		}).Should(Succeed())
	})
}

func SetupEnvironment(namespace string) {
	GinkgoHelper()
	By("setting up the environment")
	Eventually(func() error {
		return CreateNamespace(PrimaryK8sCluster, namespace)
	}).Should(Succeed())
	Eventually(func() error {
		return CreateNamespace(SecondaryK8sCluster, namespace)
	}).Should(Succeed())
	Eventually(func() error {
		return ApplyRBDPoolAndSCTemplate(PrimaryK8sCluster, CephClusterNamespace)
	}).Should(Succeed())
	Eventually(func() error {
		return ApplyRBDPoolAndSCTemplate(SecondaryK8sCluster, CephClusterNamespace)
	}).Should(Succeed())
}

func CreatePod(cluster int, namespace, podName, pvcName string) {
	GinkgoHelper()
	err := applyPodMountVolumeTemplate(cluster, namespace, podName, pvcName)
	Expect(err).NotTo(HaveOccurred())
}

func CreatePVC(ctx SpecContext, cluster int, namespace, name string) {
	Eventually(ctx, func() error {
		return applyPVCTemplate(cluster, namespace, name)
	}).Should(Succeed())
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
	Expect(len(stdout)).NotTo(Equal(0))
	return string(stdout)
}

func CreateMantleBackup(cluster int, namespace, pvcName, backupName string) {
	GinkgoHelper()
	By("creating a MantleBackup object")
	Eventually(func() error {
		return ApplyMantleBackupTemplate(cluster, namespace, pvcName, backupName)
	}).Should(Succeed())
}

func CreateMantleBackupConfig(cluster int, namespace, pvcName, backupConfigName string) {
	GinkgoHelper()
	By("creating a MantleBackupConfig object")
	Eventually(func() error {
		return ApplyMantleBackupConfigTemplate(cluster, namespace, pvcName, backupConfigName)
	}).Should(Succeed())
}

func WaitMantleBackupReadyToUse(cluster int, namespace, backupName string) {
	GinkgoHelper()
	By("checking MantleBackup's ReadyToUse status")
	Eventually(func() error {
		mb, err := GetMB(cluster, namespace, backupName)
		if err != nil {
			return err
		}
		if !mb.IsReady() {
			return errors.New("status of ReadyToUse condition is not True")
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
	clusterName := "primary"
	if clusterNo == SecondaryK8sCluster {
		clusterName = "secondary"
	}
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
		stdout, _, err := Kubectl(clusterNo, nil, "exec", "-n", namespace, "deploy/"+mountDeployName, "--",
			"bash", "-c", "sha256sum /volume/data | awk '{print $1}'")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(string(stdout)).To(Equal(writtenDataHash))
	}).Should(Succeed())
}

func WaitMantleBackupDeleted(ctx SpecContext, cluster int, namespace, backupName string) {
	GinkgoHelper()
	By("waiting for MantleBackup to be removed")
	Eventually(ctx, func(g Gomega) {
		mbs, err := GetMBList(cluster, namespace)
		g.Expect(err).NotTo(HaveOccurred())
		exists := slices.ContainsFunc(mbs.Items, func(mb mantlev1.MantleBackup) bool {
			return mb.GetName() == backupName
		})
		g.Expect(exists).To(BeFalse())
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
		CephClusterNamespace,
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
		"delete", "-n", CephClusterNamespace, "deploy", RgwDeployName,
	)
	Expect(err).NotTo(HaveOccurred())

	By("waiting for the RGW pods to be ready")
	Eventually(ctx, func(g Gomega) {
		pods, err := GetPodList(SecondaryK8sCluster, CephClusterNamespace)
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
		CephClusterNamespace,
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
		pods, err := GetPodList(SecondaryK8sCluster, CephClusterNamespace)
		g.Expect(err).NotTo(HaveOccurred())
		exist := slices.ContainsFunc(pods.Items, func(pod corev1.Pod) bool {
			return strings.HasPrefix(pod.GetName(), RgwDeployName)
		})
		g.Expect(exist).NotTo(BeFalse())
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
	return ceph.NewCephCmdWithToolsAndCustomKubectl(kubectl, CephClusterNamespace)
}

func WaitUploadJobCreated(ctx SpecContext, cluster int, namespace, backupName string, partNum int) {
	GinkgoHelper()
	By("waiting for an upload job to be created")
	Eventually(ctx, func(g Gomega) {
		mb, err := GetMB(cluster, namespace, backupName)
		g.Expect(err).NotTo(HaveOccurred())
		jobs, err := GetJobList(cluster, CephClusterNamespace)
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
		jobs, err := GetJobList(cluster, CephClusterNamespace)
		g.Expect(err).NotTo(HaveOccurred())
		exist := slices.ContainsFunc(jobs.Items, func(job batchv1.Job) bool {
			_, ok := controller.ExtractPartNumFromComponentJobName(componentPrefix, job.GetName(), backup)
			return ok
		})
		g.Expect(exist).To(BeFalse())
	}).Should(Succeed())
}

func WaitTemporaryPrimaryJobsDeleted(ctx SpecContext, primaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitComponentJobsDeleted(ctx, PrimaryK8sCluster, CephClusterNamespace,
		controller.MantleExportJobPrefix, primaryMB)
	WaitComponentJobsDeleted(ctx, PrimaryK8sCluster, CephClusterNamespace,
		controller.MantleUploadJobPrefix, primaryMB)
}

func WaitTemporarySecondaryJobsDeleted(ctx SpecContext, secondaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitComponentJobsDeleted(ctx, SecondaryK8sCluster, CephClusterNamespace,
		controller.MantleImportJobPrefix, secondaryMB)
	WaitComponentJobsDeleted(ctx, SecondaryK8sCluster, CephClusterNamespace,
		controller.MantleZeroOutJobPrefix, secondaryMB)
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
		pvcs, err := GetPVCList(cluster, CephClusterNamespace)
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
		pvcs, err := GetPVCList(cluster, CephClusterNamespace)
		g.Expect(err).NotTo(HaveOccurred())
		exist := slices.ContainsFunc(pvcs.Items, func(pvc corev1.PersistentVolumeClaim) bool {
			return strings.HasPrefix(pvc.GetName(), pvcNamePrefix)
		})
		g.Expect(exist).To(BeFalse())
	}).Should(Succeed())
}

func WaitTemporaryPrimaryPVCsDeleted(ctx SpecContext, primaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitPVCsDeleted(ctx, PrimaryK8sCluster, CephClusterNamespace,
		fmt.Sprintf("%s%s-", controller.MantleExportDataPVCPrefix, string(primaryMB.GetUID())))
}

func WaitTemporarySecondaryPVCsDeleted(ctx SpecContext, secondaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitPVCDeleted(ctx, SecondaryK8sCluster, CephClusterNamespace, controller.MakeZeroOutPVCName(secondaryMB))
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

func WaitTemporarySecondaryPVsDeleted(ctx SpecContext, secondaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitPVDeleted(ctx, SecondaryK8sCluster, controller.MakeZeroOutPVName(secondaryMB))
}

func WaitTemporaryS3ObjectsDeleted(ctx SpecContext, primaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	By("waiting for the temporary s3 objects to be deleted")
	expectedObjectNamePrefix := fmt.Sprintf("%s-%s-", primaryMB.GetName(), string(primaryMB.GetUID()))
	Eventually(ctx, func(g Gomega) {
		objectStorageClient, err := CreateObjectStorageClient(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		listOutput, err := objectStorageClient.listObjects(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		for _, c := range listOutput.Contents {
			g.Expect(c.Key).NotTo(BeNil())
			g.Expect(strings.HasPrefix(*c.Key, expectedObjectNamePrefix)).To(BeFalse())
		}
	}).Should(Succeed())
}

func WaitTemporaryResourcesDeleted(ctx SpecContext, primaryMB, secondaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitTemporaryJobsDeleted(ctx, primaryMB, secondaryMB)
	WaitTemporaryPVCsDeleted(ctx, primaryMB, secondaryMB)
	WaitTemporarySecondaryPVsDeleted(ctx, secondaryMB)
	WaitTemporaryS3ObjectsDeleted(ctx, primaryMB)
}

func DeleteMantleBackup(cluster int, namespace, backupName string) {
	GinkgoHelper()
	By("deleting MantleBackup")
	_, _, err := Kubectl(cluster, nil, "delete", "-n", namespace, "mantlebackup", backupName, "--wait=false")
	Expect(err).NotTo(HaveOccurred())
}

func GetBackupTransferPartSize() (*resource.Quantity, error) {
	deployMC, err := GetDeploy(PrimaryK8sCluster, CephClusterNamespace, MantleControllerDeployName)
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

func ChangeBackupTransferPartSize(size string) {
	GinkgoHelper()

	deployMC, err := GetDeploy(PrimaryK8sCluster, CephClusterNamespace, MantleControllerDeployName)
	Expect(err).NotTo(HaveOccurred())

	args := deployMC.Spec.Template.Spec.Containers[0].Args
	backupTransferPartSizeIndex := slices.IndexFunc(
		args,
		func(arg string) bool { return strings.HasPrefix(arg, "--backup-transfer-part-size=") },
	)
	Expect(backupTransferPartSizeIndex).NotTo(Equal(-1))

	_, _, err = Kubectl(
		PrimaryK8sCluster, nil,
		"patch", "deploy", "-n", CephClusterNamespace, MantleControllerDeployName, "--type=json",
		fmt.Sprintf(
			`-p=[{"op": "replace", "path": "/spec/template/spec/containers/0/args/%d", `+
				`"value":"--backup-transfer-part-size=%s"}]`,
			backupTransferPartSizeIndex,
			size,
		),
	)
	Expect(err).NotTo(HaveOccurred())
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

	deployMC, err := GetDeploy(cluster, CephClusterNamespace, MantleControllerDeployName)
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
		"patch", "deploy", "-n", CephClusterNamespace, MantleControllerDeployName, "--type=json",
		fmt.Sprintf("--patch=%s", marshalledPatch),
	)
	Expect(err).NotTo(HaveOccurred())

	By("waiting until the controller Pod starts running")
	Eventually(ctx, func(g Gomega) {
		stdout, _, err := Kubectl(cluster, nil, "get", "pod", "-n", CephClusterNamespace, "-o", "json")
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
	fields = append(fields, "logs", "-n", CephClusterNamespace, "deployment/"+MantleControllerDeployName, "-f")

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
