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
	"github.com/cybozu-go/mantle/internal/ceph"
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
	RgwDeployName        = "rook-ceph-rgw-ceph-object-store-a"
	RBDPoolName          = "rook-ceph-block"
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
		return nil, errors.New("Either KUBECTL_PRIMARY or KUBECTL_SECONDARY environment variable is not set")
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
			return CheckDeploymentReady(PrimaryK8sCluster, CephClusterNamespace, "mantle-controller")
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
	By(fmt.Sprintf("checking MantleBackup's SyncedToRemote status: %s/%s", namespace, backupName))
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
	rbdImageName, err := getRBDImageName(cluster, namespace, pvcName)
	if err != nil {
		return nil, fmt.Errorf("failed to get rbd image name: %w", err)
	}
	cmd := createCephCmd(cluster)
	snaps, err := cmd.RBDSnapLs(RBDPoolName, rbdImageName)
	if err != nil {
		return nil, fmt.Errorf("failed to create ceph cmd: %w", err)
	}
	return snaps, nil
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

func WaitUploadJobCreated(ctx SpecContext, cluster int, namespace, backupName string) {
	GinkgoHelper()
	By("waiting for an upload job to be created")
	Eventually(ctx, func(g Gomega) {
		mb, err := GetMB(cluster, namespace, backupName)
		g.Expect(err).NotTo(HaveOccurred())
		jobs, err := GetJobList(cluster, CephClusterNamespace)
		g.Expect(err).NotTo(HaveOccurred())
		exist := slices.ContainsFunc(jobs.Items, func(job batchv1.Job) bool {
			return job.GetName() == controller.MakeUploadJobName(mb)
		})
		g.Expect(exist).To(BeTrue())
	}).Should(Succeed())
}

func WaitJobDeleted(ctx SpecContext, cluster int, namespace, jobName string) {
	GinkgoHelper()
	By("waiting for a job to be deleted")
	Eventually(ctx, func(g Gomega) {
		jobs, err := GetJobList(cluster, CephClusterNamespace)
		g.Expect(err).NotTo(HaveOccurred())
		exist := slices.ContainsFunc(jobs.Items, func(job batchv1.Job) bool {
			return job.GetName() == jobName
		})
		g.Expect(exist).To(BeFalse())
	}).Should(Succeed())
}

func WaitTemporaryPrimaryJobsDeleted(ctx SpecContext, primaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitJobDeleted(ctx, PrimaryK8sCluster, CephClusterNamespace, controller.MakeExportJobName(primaryMB))
	WaitJobDeleted(ctx, PrimaryK8sCluster, CephClusterNamespace, controller.MakeUploadJobName(primaryMB))
}

func WaitTemporarySecondaryJobsDeleted(ctx SpecContext, secondaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitJobDeleted(ctx, SecondaryK8sCluster, CephClusterNamespace, controller.MakeImportJobName(secondaryMB))
	WaitJobDeleted(ctx, SecondaryK8sCluster, CephClusterNamespace, controller.MakeDiscardJobName(secondaryMB))
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

func WaitTemporaryPrimaryPVCsDeleted(ctx SpecContext, primaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitPVCDeleted(ctx, PrimaryK8sCluster, CephClusterNamespace, controller.MakeExportDataPVCName(primaryMB))
}

func WaitTemporarySecondaryPVCsDeleted(ctx SpecContext, secondaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitPVCDeleted(ctx, SecondaryK8sCluster, CephClusterNamespace, controller.MakeDiscardPVCName(secondaryMB))
}

func WaitTemporaryPVCsDeleted(ctx SpecContext, primaryMB, secondaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitTemporaryPrimaryPVCsDeleted(ctx, primaryMB)
	WaitTemporarySecondaryPVCsDeleted(ctx, secondaryMB)
}

func WaitPVDeleted(ctx SpecContext, cluster int, namespace, pvName string) {
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
	WaitPVDeleted(ctx, SecondaryK8sCluster, CephClusterNamespace, controller.MakeDiscardPVName(secondaryMB))
}

func WaitTemporaryS3ObjectDeleted(ctx SpecContext, primaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	By("waiting for the temporary s3 object to be deleted")
	expectedObjectName := controller.MakeObjectNameOfExportedData(
		primaryMB.GetName(), string(primaryMB.GetUID()))
	Eventually(ctx, func(g Gomega) {
		objectStorageClient, err := CreateObjectStorageClient(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		listOutput, err := objectStorageClient.listObjects(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		for _, c := range listOutput.Contents {
			g.Expect(c.Key).NotTo(BeNil())
			g.Expect(*c.Key).NotTo(Equal(expectedObjectName))
		}
	}).Should(Succeed())
}

func WaitTemporaryResourcesDeleted(ctx SpecContext, primaryMB, secondaryMB *mantlev1.MantleBackup) {
	GinkgoHelper()
	WaitTemporaryJobsDeleted(ctx, primaryMB, secondaryMB)
	WaitTemporaryPVCsDeleted(ctx, primaryMB, secondaryMB)
	WaitTemporarySecondaryPVsDeleted(ctx, secondaryMB)
	WaitTemporaryS3ObjectDeleted(ctx, primaryMB)
}

func DeleteMantleBackup(cluster int, namespace, backupName string) {
	GinkgoHelper()
	By("deleting MantleBackup")
	_, _, err := Kubectl(cluster, nil, "delete", "-n", namespace, "mantlebackup", backupName, "--wait=false")
	Expect(err).NotTo(HaveOccurred())
}

func GetBothMBs(namespace, backupName string) (*mantlev1.MantleBackup, *mantlev1.MantleBackup, error) {
	primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
	if err != nil {
		return nil, nil, err
	}
	secondaryMB, err := GetMB(SecondaryK8sCluster, namespace, backupName)
	if err != nil {
		return nil, nil, err
	}
	return primaryMB, secondaryMB, nil
}

func getRBDImageName(cluster int, namespace, pvcName string) (string, error) {
	pvc, err := GetPVC(cluster, namespace, pvcName)
	if err != nil {
		return "", fmt.Errorf("failed to get PVC: %w", err)
	}
	pv, err := GetPV(cluster, pvc.Spec.VolumeName)
	if err != nil {
		return "", fmt.Errorf("failed to get PV: %w", err)
	}
	return pv.Spec.CSI.VolumeAttributes["imageName"], nil
}

func GetRBDImageSize(cluster int, namespace, pvcName string) (int, error) {
	rbdImageName, err := getRBDImageName(cluster, namespace, pvcName)
	if err != nil {
		return 0, fmt.Errorf("failed to get rbd image name: %w", err)
	}
	cmd := createCephCmd(cluster)
	info, err := cmd.RBDInfo(RBDPoolName, rbdImageName)
	if err != nil {
		return 0, fmt.Errorf("failed to create ceph cmd: %w", err)
	}
	return info.Size, nil
}
