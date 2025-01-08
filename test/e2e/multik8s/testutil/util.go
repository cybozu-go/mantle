package testutil

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
)

const (
	CephClusterNamespace = "rook-ceph"
	PrimaryK8sCluster    = 1
	SecondaryK8sCluster  = 2
)

var (
	//go:embed testdata/pvc-template.yaml
	testPVCTemplate string
	//go:embed testdata/rbd-pool-sc-template.yaml
	testRBDPoolSCTemplate string
	//go:embed testdata/mantlebackup-template.yaml
	testMantleBackupTemplate string
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

// input can be nil
func Kubectl(clusterNo int, input []byte, args ...string) ([]byte, []byte, error) {
	kubectlPrefix := ""
	switch clusterNo {
	case PrimaryK8sCluster:
		kubectlPrefix = kubectlPrefixPrimary
	case SecondaryK8sCluster:
		kubectlPrefix = kubectlPrefixSecondary
	default:
		panic(fmt.Sprintf("invalid clusterNo: %d", clusterNo))
	}
	if len(kubectlPrefix) == 0 {
		panic("Either KUBECTL_PRIMARY or KUBECTL_SECONDARY environment variable is not set")
	}
	fields := strings.Fields(kubectlPrefix)
	fields = append(fields, args...)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return execAtLocal(ctx, fields[0], input, fields[1:]...)
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

func ApplyMantleRestoreTemplate(clusterNo int, namespace, restoreName, backupName string) error {
	manifest := fmt.Sprintf(testMantleRestoreTemplate, restoreName, restoreName, namespace, backupName)
	_, _, err := Kubectl(clusterNo, []byte(manifest), "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("kubectl apply mantlerestore failed. err: %w", err)
	}
	return nil
}

func ApplyPVCTemplate(clusterNo int, namespace, name string) error {
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
	stdout, _, err := Kubectl(clusterNo, nil, "get", kind, "-n", namespace, name, "-o", "json")
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

func ChangeClusterRole(clusterNo int, newRole string) error {
	deployName := "mantle-controller"
	deploy, err := GetDeploy(clusterNo, CephClusterNamespace, deployName)
	if err != nil {
		return fmt.Errorf("failed to get mantle-controller deploy: %w", err)
	}

	roleIndex := slices.IndexFunc(
		deploy.Spec.Template.Spec.Containers[0].Args,
		func(arg string) bool { return strings.HasPrefix(arg, "--role=") },
	)
	if roleIndex == -1 {
		return errors.New("failed to find --role= argument")
	}

	_, _, err = Kubectl(
		clusterNo, nil, "patch", "deploy", "-n", CephClusterNamespace, deployName, "--type=json",
		fmt.Sprintf(
			`-p=[{"op": "replace", "path": "/spec/template/spec/containers/0/args/%d", "value":"--role=%s"}]`,
			roleIndex,
			newRole,
		),
	)
	if err != nil {
		return fmt.Errorf("failed to patch mantle-controller deploy: %w", err)
	}

	// Wait for the new controller to start
	numRetries := 10
	for i := 0; i < numRetries; i++ {
		stdout, _, err := Kubectl(clusterNo, nil, "get", "pod", "-n", CephClusterNamespace, "-o", "json")
		if err != nil {
			return fmt.Errorf("failed to get pod: %w", err)
		}
		var pods corev1.PodList
		err = json.Unmarshal(stdout, &pods)
		if err != nil {
			return fmt.Errorf("failed to unmarshal pod list: %w", err)
		}
		ready := true
		for _, pod := range pods.Items {
			if strings.HasPrefix(pod.GetName(), deployName) {
				for _, container := range pod.Spec.Containers {
					if !slices.Contains(container.Args, fmt.Sprintf("--role=%s", newRole)) {
						ready = false
					}
				}
			}
		}
		if ready {
			break
		}
		time.Sleep(10 * time.Second)
	}

	return nil
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

func WaitControllerToBeReady() {
	GinkgoHelper()
	It("wait for mantle-controller to be ready", func() {
		Eventually(func() error {
			return CheckDeploymentReady(PrimaryK8sCluster, "rook-ceph", "mantle-controller")
		}).Should(Succeed())

		Eventually(func() error {
			return CheckDeploymentReady(PrimaryK8sCluster, "rook-ceph", "mantle-controller")
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

func CreatePVC(ctx context.Context, cluster int, namespace, name string) {
	Eventually(ctx, func() error {
		return ApplyPVCTemplate(cluster, namespace, name)
	}).Should(Succeed())
}

func WriteRandomDataToPV(ctx context.Context, cluster int, namespace, pvcName string) string {
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

func WaitMantleBackupReadyToUse(cluster int, namespace, backupName string) {
	GinkgoHelper()
	By("checking MantleBackup's ReadyToUse status")
	Eventually(func() error {
		mb, err := GetMB(cluster, namespace, backupName)
		if err != nil {
			return err
		}
		if !meta.IsStatusConditionTrue(mb.Status.Conditions, mantlev1.BackupConditionReadyToUse) {
			return errors.New("status of ReadyToUse condition is not True")
		}
		return nil
	}, "10m", "1s").Should(Succeed())
}

func WaitMantleBackupSynced(namespace, backupName string) {
	GinkgoHelper()
	By("checking MantleBackup's SyncedToRemote status")
	Eventually(func() error {
		mb, err := GetMB(PrimaryK8sCluster, namespace, backupName)
		if err != nil {
			return err
		}
		if !meta.IsStatusConditionTrue(mb.Status.Conditions, mantlev1.BackupConditionSyncedToRemote) {
			return errors.New("status of SyncedToRemote condition is not True")
		}
		return nil
	}, "10m", "1s").Should(Succeed())
}

func EnsureMantleBackupNotExist(ctx context.Context, cluster int, namespace, backupName string) {
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

func EnsureTemporaryResourcesDeleted(ctx context.Context) {
	GinkgoHelper()
	By("checking all temporary Jobs related to export and import of RBD images are removed")
	primaryJobList, err := GetObjectList[batchv1.JobList](PrimaryK8sCluster, "job", CephClusterNamespace)
	Expect(err).NotTo(HaveOccurred())
	Expect(slices.ContainsFunc(primaryJobList.Items, func(job batchv1.Job) bool {
		n := job.GetName()
		return strings.HasPrefix(n, controller.MantleExportJobPrefix) ||
			strings.HasPrefix(n, controller.MantleUploadJobPrefix)
	})).To(BeFalse())
	secondaryJobList, err := GetObjectList[batchv1.JobList](SecondaryK8sCluster, "job", CephClusterNamespace)
	Expect(err).NotTo(HaveOccurred())
	Expect(slices.ContainsFunc(secondaryJobList.Items, func(job batchv1.Job) bool {
		n := job.GetName()
		return strings.HasPrefix(n, controller.MantleImportJobPrefix) ||
			strings.HasPrefix(n, controller.MantleDiscardJobPrefix)
	})).To(BeFalse())

	By("checking all temporary PVCs related to export and import of RBD images are removed")
	primaryPVCList, err := GetObjectList[corev1.PersistentVolumeClaimList](
		PrimaryK8sCluster, "pvc", CephClusterNamespace)
	Expect(err).NotTo(HaveOccurred())
	Expect(slices.ContainsFunc(primaryPVCList.Items, func(pvc corev1.PersistentVolumeClaim) bool {
		n := pvc.GetName()
		return strings.HasPrefix(n, controller.MantleExportDataPVCPrefix)
	})).To(BeFalse())
	secondaryPVCList, err := GetObjectList[corev1.PersistentVolumeClaimList](
		SecondaryK8sCluster, "pvc", CephClusterNamespace)
	Expect(err).NotTo(HaveOccurred())
	Expect(slices.ContainsFunc(secondaryPVCList.Items, func(pvc corev1.PersistentVolumeClaim) bool {
		n := pvc.GetName()
		return strings.HasPrefix(n, controller.MantleDiscardPVCPrefix)
	})).To(BeFalse())

	By("checking all temporary PVs related to export and import of RBD images are removed")
	secondaryPVList, err := GetObjectList[corev1.PersistentVolumeList](SecondaryK8sCluster, "pv", CephClusterNamespace)
	Expect(err).NotTo(HaveOccurred())
	Expect(slices.ContainsFunc(secondaryPVList.Items, func(pv corev1.PersistentVolume) bool {
		n := pv.GetName()
		return strings.HasPrefix(n, controller.MantleDiscardPVPrefix)
	})).To(BeFalse())

	By("checking all temporary objects in the object storage related to export and import of RBD images are removed")
	objectStorageClient, err := CreateObjectStorageClient(ctx)
	Expect(err).NotTo(HaveOccurred())
	listOutput, err := objectStorageClient.listObjects(ctx)
	Expect(err).NotTo(HaveOccurred())
	Expect(len(listOutput.Contents)).To(Equal(0))
}

func EnsureCorrectRestoration(
	clusterNo int,
	ctx context.Context,
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
		g.Expect(meta.IsStatusConditionTrue(mr.Status.Conditions, "ReadyToUse")).To(BeTrue())
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
