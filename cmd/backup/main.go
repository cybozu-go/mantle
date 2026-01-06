package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	mbcName, mbcNamespace string
	zapOpts               zap.Options

	scheme                = runtime.NewScheme()
	MantleBackupConfigUID = "mantle.cybozu.io/mbc-uid"
	logger                = ctrl.Log.WithName("backup")

	BackupCmd = &cobra.Command{
		Use: "backup",
		RunE: func(cmd *cobra.Command, args []string) error {
			return subMain(cmd.Context())
		},
	}
)

func init() {
	flags := BackupCmd.Flags()
	flags.StringVar(&mbcName, "name", "", "MantleBackupConfig resource's name")
	flags.StringVar(&mbcNamespace, "namespace", "", "MantleBackupConfig resource's namespace")

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
