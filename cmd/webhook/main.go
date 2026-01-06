package webhook

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"strconv"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	webhookv1 "github.com/cybozu-go/mantle/internal/webhook/v1"
)

var WebhookCmd = &cobra.Command{
	Use:   "webhook",
	Short: "Start the VolumeAttachment validating webhook server",
	RunE: func(cmd *cobra.Command, args []string) error {
		return subMain()
	},
}

var (
	metricsAddr     string
	probeAddr       string
	webhookAddr     string
	webhookCertPath string
	webhookKeyPath  string
	zapOpts         zap.Options

	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	flags := WebhookCmd.Flags()
	flags.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flags.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flags.StringVar(&webhookAddr, "webhook-bind-address", ":9443", "The address the webhook server binds to.")
	flags.StringVar(&webhookCertPath, "webhook-cert-path", "",
		"The file path of the webhook certificate file. (required)")
	flags.StringVar(&webhookKeyPath, "webhook-key-path", "",
		"The file path of the webhook key file. (required)")

	goflags := flag.NewFlagSet("goflags", flag.ExitOnError)
	zapOpts.Development = true
	zapOpts.BindFlags(goflags)
	flags.AddGoFlagSet(goflags)

	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(mantlev1.AddToScheme(scheme))
}

func checkCommandlineArgs() error {
	if webhookCertPath == "" {
		return fmt.Errorf("--webhook-cert-path must be specified")
	}
	if webhookKeyPath == "" {
		return fmt.Errorf("--webhook-key-path must be specified")
	}

	return nil
}

func subMain() error {
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	if err := checkCommandlineArgs(); err != nil {
		setupLog.Error(err, "invalid command line arguments")

		return err
	}

	webhookTLSOpts := []func(*tls.Config){}
	setupLog.Info("Initializing webhook certificate watcher using provided certificates",
		"webhook-cert-path", webhookCertPath, "webhook-key-path", webhookKeyPath)

	webhookCertWatcher, err := certwatcher.New(webhookCertPath, webhookKeyPath)
	if err != nil {
		setupLog.Error(err, "Failed to initialize webhook certificate watcher")

		return err
	}

	webhookTLSOpts = append(webhookTLSOpts, func(config *tls.Config) {
		config.GetCertificate = webhookCertWatcher.GetCertificate
	})

	webhookHost, webhookPortStr, err := net.SplitHostPort(webhookAddr)
	if err != nil {
		setupLog.Error(err, "Failed to parse webhook-bind-address")

		return err
	}
	webhookPort, err := strconv.Atoi(webhookPortStr)
	if err != nil {
		setupLog.Error(err, "Failed to parse webhook port")

		return err
	}

	mgrOptions := manager.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		WebhookServer: webhook.NewServer(webhook.Options{
			Host:    webhookHost,
			Port:    webhookPort,
			TLSOpts: webhookTLSOpts,
		}),
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), mgrOptions)
	if err != nil {
		setupLog.Error(err, "unable to start manager")

		return err
	}

	if err := webhookv1.SetupVolumeAttachmentWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "failed to setup VolumeAttachment webhook")

		return err
	}

	setupLog.Info("Adding webhook certificate watcher to manager")
	if err := mgr.Add(webhookCertWatcher); err != nil {
		setupLog.Error(err, "unable to add webhook certificate watcher to manager")

		return err
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")

		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")

		return err
	}

	setupLog.Info("starting webhook server")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running webhook server")

		return err
	}

	return nil
}
