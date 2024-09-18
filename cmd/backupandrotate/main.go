package backupandrotate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/kube-openapi/pkg/validation/strfmt"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	mbcName, mbcNamespace string
	expireOffset          string
	zapOpts               zap.Options

	scheme                = runtime.NewScheme()
	MantleBackupConfigUID = "mantle.cybozu.io/mbc-uid"
	MantleRetainIfExpired = "mantle.cybozu.io/retainIfExpired"
	logger                = ctrl.Log.WithName("backup-and-rotate")

	BackupAndRotateCmd = &cobra.Command{
		Use: "backup-and-rotate",
		RunE: func(cmd *cobra.Command, args []string) error {
			return subMain(cmd.Context())
		},
	}
)

func init() {
	flags := BackupAndRotateCmd.Flags()
	flags.StringVar(&mbcName, "name", "", "MantleBackupConfig resource's name")
	flags.StringVar(&mbcNamespace, "namespace", "", "MantleBackupConfig resource's namespace")
	flags.StringVar(&expireOffset, "expire-offset", "0s",
		"An offset for MantleBackupConfig's .spec.expire field. A MantleBackup will expire after "+
			"it has been active for (.spec.expire - expire-offset) time. This option is intended for testing purposes only.")

	goflags := flag.NewFlagSet("goflags", flag.ExitOnError)
	zapOpts.Development = true
	zapOpts.BindFlags(goflags)
	flags.AddGoFlagSet(goflags)

	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(mantlev1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func getMBName(mbc *mantlev1.MantleBackupConfig, jobID string) string {
	name := mbc.GetName()
	if len(name) > 43 {
		name = name[:43]
	}

	hasher := sha256.New()
	_, _ = io.WriteString(hasher, name+"\000"+jobID)
	hash := hex.EncodeToString(hasher.Sum(nil))

	return fmt.Sprintf("%s-%s-%s", name, jobID, hash[:6])
}

func fetchJobID() (string, error) {
	jobName, ok := os.LookupEnv("JOB_NAME")
	if !ok {
		return "", fmt.Errorf("JOB_NAME not found")
	}
	if len(jobName) < 8 {
		return "", fmt.Errorf("the length of JOB_NAME must be >= 8")
	}
	return jobName[len(jobName)-8:], nil
}

func subMain(ctx context.Context) error {
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	parsedExpireOffset, err := strfmt.ParseDuration(expireOffset)
	if err != nil {
		return fmt.Errorf("couldn't parse the expire offset: %w", err)
	}

	cli, err := client.New(config.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("couldn't create a new client: %w", err)
	}

	// Get the target mbc.
	var mbc mantlev1.MantleBackupConfig
	if err := cli.Get(ctx, types.NamespacedName{Name: mbcName, Namespace: mbcNamespace}, &mbc); err != nil {
		return fmt.Errorf("couldn't get mbc: %s: %s: %w", mbcName, mbcNamespace, err)
	}

	if err := createMantleBackup(ctx, cli, &mbc); err != nil {
		return fmt.Errorf("backup failed: %s: %s: %w", mbcName, mbcNamespace, err)
	}

	if err := rotateMantleBackup(ctx, cli, &mbc, parsedExpireOffset); err != nil {
		return fmt.Errorf("rotation failed: %s: %s: %w", mbcName, mbcNamespace, err)
	}

	return nil
}

// createMantleBackup creates a new MantleBackup (mb) resource. If mb already exists, it
// checks its label and decides if we can keep on reconciling.
func createMantleBackup(ctx context.Context, cli client.Client, mbc *mantlev1.MantleBackupConfig) error {
	jobID, err := fetchJobID()
	if err != nil {
		return fmt.Errorf("couldn't fetch job id: %w", err)
	}

	mbName := getMBName(mbc, jobID)
	mbNamespace := mbcNamespace

	err = cli.Create(ctx, &mantlev1.MantleBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mbName,
			Namespace: mbNamespace,
			Labels:    map[string]string{MantleBackupConfigUID: string(mbc.GetUID())},
		},
		Spec: mantlev1.MantleBackupSpec{
			Expire: mbc.Spec.Expire,
			PVC:    mbc.Spec.PVC,
		},
	})
	if err == nil {
		logger.Info("a new MantleBackup resource created",
			"mbName", mbName, "mbNamespace", mbNamespace,
			"mbcName", mbcName, "mbcNamespace", mbcNamespace)
		return nil
	}
	if !errors.IsAlreadyExists(err) {
		return fmt.Errorf("couldn't create a MantleBackup: %s: %s: %w", mbName, mbNamespace, err)
	}

	var mb mantlev1.MantleBackup
	if err := cli.Get(ctx, types.NamespacedName{Name: mbName, Namespace: mbNamespace}, &mb); err != nil {
		return fmt.Errorf("couldn't get MantleBackup: %s: %s: %w", mbName, mbNamespace, err)
	}
	uid, ok := mb.GetLabels()[MantleBackupConfigUID]
	if !ok {
		return fmt.Errorf("label %s not found: %s: %s", MantleBackupConfigUID, mb.Name, mb.Namespace)
	}
	if uid != string(mbc.GetUID()) {
		return fmt.Errorf("this MantleBackup was created by other MantleBackupConfig: expectedUID %s: actualUID %s: %s: %s",
			string(mbc.GetUID()), uid, mb.Name, mb.Namespace)
	}

	// At this point we know that mb was created by the previous run of the current job and we're retrying it.
	// Thus, it is safe to ignore this "already exists" error.
	logger.Info("MantleBackup already exists",
		"mbName", mbName, "mbNamespace", mbNamespace,
		"mbcName", mbcName, "mbcNamespace", mbcNamespace)
	return nil
}

func rotateMantleBackup(
	ctx context.Context,
	cli client.Client,
	mbc *mantlev1.MantleBackupConfig,
	expireOffset time.Duration,
) error {
	// List all MantleBackup objects associated with the mbc.
	var mbList mantlev1.MantleBackupList
	if err := cli.List(ctx, &mbList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{MantleBackupConfigUID: string(mbc.GetUID())}),
	}); err != nil {
		return fmt.Errorf("couldn't list MantleBackups: %s: %w", string(mbc.UID), err)
	}

	// Delete the MantleBackup objects that are already expired and don't have the retainIfExpired label.
	expire, err := strfmt.ParseDuration(mbc.Spec.Expire)
	if err != nil {
		return fmt.Errorf("couldn't parse spec.expire: %s: %w", mbc.Spec.Expire, err)
	}
	if expire >= expireOffset {
		expire -= expireOffset
	} else {
		expire = 0
	}
	for _, mb := range mbList.Items {
		if mb.Status.CreatedAt == (metav1.Time{}) {
			// mb is not created yet (at least from the cache's perspective), so let's ignore it.
			continue
		}
		elapsed := time.Since(mb.Status.CreatedAt.Time)
		if elapsed <= expire {
			continue
		}
		retain, ok := mb.GetAnnotations()[MantleRetainIfExpired]
		if ok && retain == "true" {
			continue
		}

		if err := cli.Delete(ctx, &mb, &client.DeleteOptions{
			Preconditions: &metav1.Preconditions{
				UID:             &mb.UID,
				ResourceVersion: &mb.ResourceVersion,
			},
		}); err == nil || errors.IsNotFound(err) {
			logger.Info("MantleBackup deleted",
				"mb.Name", mb.Name, "mb.Namespace", mb.Namespace,
				"mb.UID", mb.UID, "mb.ResourceVersion", mb.ResourceVersion,
				"mbcName", mbcName, "mbcNamespace", mbcNamespace,
				"elapsed", elapsed, "expire", expire,
			)
		} else {
			return fmt.Errorf("couldn't delete MantleBackup: %s: %s: %s: %s: %w",
				mb.Name, mb.Namespace, mb.UID, mb.ResourceVersion, err)
		}
	}

	return nil
}
