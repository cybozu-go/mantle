package controller

import (
	"errors"
	"flag"
	"os"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	//+kubebuilder:scaffold:imports
)

var ControllerCmd = &cobra.Command{
	Use: "controller",
	RunE: func(cmd *cobra.Command, args []string) error {
		return subMain()
	},
}

var (
	metricsAddr          string
	enableLeaderElection bool
	probeAddr            string
	zapOpts              zap.Options
	expireOffset         string
	overwriteMBCSchedule string

	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	flags := ControllerCmd.Flags()
	flags.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flags.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flags.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flags.StringVar(&expireOffset, "expire-offset", "",
		"An offset for MantleBackupConfig's .spec.expire field. A MantleBackup will expire after "+
			"it has been active for (.spec.expire - expire-offset) time. This option is intended for testing purposes only.")
	flags.StringVar(&overwriteMBCSchedule, "overwrite-mbc-schedule", "",
		"By setting this option, every CronJob created by this controller for every MantleBackupConfig "+
			"will use its value as .spec.schedule. This option is intended for testing purposes only.")

	goflags := flag.NewFlagSet("goflags", flag.ExitOnError)
	zapOpts.Development = true
	zapOpts.BindFlags(goflags)
	flags.AddGoFlagSet(goflags)

	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(mantlev1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func subMain() error {
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "cfdaa833.cybozu.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		return err
	}

	managedCephClusterID := os.Getenv("POD_NAMESPACE")
	if managedCephClusterID == "" {
		setupLog.Error(errors.New("POD_NAMESPACE is empty"), "POD_NAMESPACE is empty")
		return err
	}

	backupReconciler := controller.NewMantleBackupReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		managedCephClusterID,
	)

	if err = backupReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MantleBackup")
		return err
	}

	restoreReconciler := controller.NewMantleRestoreReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		managedCephClusterID,
	)
	if err = restoreReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MantleRestore")
		return err
	}
	if err = controller.NewMantleBackupConfigReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		managedCephClusterID,
		expireOffset,
		overwriteMBCSchedule,
	).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MantleBackupConfig")
		return err
	}
	//+kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		return err
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		return err
	}

	return nil
}
