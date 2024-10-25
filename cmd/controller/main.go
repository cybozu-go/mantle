package controller

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	"github.com/cybozu-go/mantle/pkg/controller/proto"
	//+kubebuilder:scaffold:imports
)

var ControllerCmd = &cobra.Command{
	Use: "controller",
	RunE: func(cmd *cobra.Command, args []string) error {
		return subMain()
	},
}

var (
	metricsAddr           string
	enableLeaderElection  bool
	probeAddr             string
	zapOpts               zap.Options
	overwriteMBCSchedule  string
	role                  string
	mantleServiceEndpoint string
	maxExportJobs         int

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
	flags.StringVar(&overwriteMBCSchedule, "overwrite-mbc-schedule", "",
		"By setting this option, every CronJob created by this controller for every MantleBackupConfig "+
			"will use its value as .spec.schedule. This option is intended for testing purposes only.")
	flags.StringVar(&role, "role", "",
		"Specifies how this controller should behave. This option is required. The value should be one of 'standalone' "+
			"(no replication), 'primary' (serve as a primary mantle), or 'secondary' (serve as a secondary mantle).")
	flags.StringVar(&mantleServiceEndpoint, "mantle-service-endpoint", "",
		"The gRPC endpoint of the secondary mantle. "+
			"This option has different meanings depending on the value of --role. "+
			"(i) If --role is 'standalone', this option is ignored. (ii) If --role is 'primary', this option is required "+
			"and is interpreted as the address that the primary mantle should connect to. (iii) If --role is 'secondary', "+
			"this option is required and is interpreted as the address that the secondary mantle should listen to.")
	flags.IntVar(&maxExportJobs, "max-export-jobs", 8,
		"The maximum number of export jobs that can run simultaneously. If you set this to 0, there is no limit.")

	goflags := flag.NewFlagSet("goflags", flag.ExitOnError)
	zapOpts.Development = true
	zapOpts.BindFlags(goflags)
	flags.AddGoFlagSet(goflags)

	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(mantlev1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func checkCommandlineArgs() error {
	switch role {
	case controller.RoleStandalone:
		// nothing to do
	case controller.RolePrimary:
		if mantleServiceEndpoint == "" {
			return errors.New("--mantle-service-endpoint must be specified if --role is 'primary'")
		}
	case controller.RoleSecondary:
		if mantleServiceEndpoint == "" {
			return errors.New("--mantle-service-endpoint must be specified if --role is 'secondary'")
		}
	default:
		return fmt.Errorf("role should be one of 'standalone', 'primary', or 'secondary': %s", role)
	}
	return nil
}

func setupReconcilers(mgr manager.Manager, primarySettings *controller.PrimarySettings) error {
	managedCephClusterID := os.Getenv("POD_NAMESPACE")
	if managedCephClusterID == "" {
		setupLog.Error(errors.New("POD_NAMESPACE is empty"), "POD_NAMESPACE is empty")
		return errors.New("POD_NAMESPACE is empty")
	}

	backupReconciler := controller.NewMantleBackupReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		managedCephClusterID,
		role,
		primarySettings,
	)
	if err := backupReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MantleBackup")
		return err
	}

	restoreReconciler := controller.NewMantleRestoreReconciler(
		mgr.GetClient(),
		mgr.GetAPIReader(),
		mgr.GetScheme(),
		managedCephClusterID,
		role,
	)
	if err := restoreReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MantleRestore")
		return err
	}

	if err := controller.NewMantleBackupConfigReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		managedCephClusterID,
		overwriteMBCSchedule,
		role,
	).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MantleBackupConfig")
		return err
	}
	//+kubebuilder:scaffold:builder

	return nil
}

func setupStandalone(mgr manager.Manager) error {
	return setupReconcilers(mgr, nil)
}

func setupPrimary(ctx context.Context, mgr manager.Manager, wg *sync.WaitGroup) error {
	conn, err := grpc.NewClient(
		mantleServiceEndpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: 1 * time.Minute,
		}),
	)
	if err != nil {
		setupLog.Error(err, "failed to create a new client for the secondary mantle")
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		_ = conn.Close()
	}()

	primarySettings := &controller.PrimarySettings{
		ServiceEndpoint: mantleServiceEndpoint,
		Conn:            conn,
		Client:          proto.NewMantleServiceClient(conn),
		MaxExportJobs:   maxExportJobs,
	}

	return setupReconcilers(mgr, primarySettings)
}

func setupSecondary(ctx context.Context, mgr manager.Manager, wg *sync.WaitGroup, cancel context.CancelFunc) error {
	logger := ctrl.Log.WithName("grpc")

	serv := grpc.NewServer(grpc.ChainUnaryInterceptor(
		logging.UnaryServerInterceptor(
			logging.LoggerFunc(func(_ context.Context, _ logging.Level, msg string, fields ...any) {
				logger.Info(msg, fields...)
			}),
			logging.WithLogOnEvents(logging.StartCall, logging.FinishCall),
		),
	))

	proto.RegisterMantleServiceServer(serv, controller.NewSecondaryServer(mgr.GetClient(), mgr.GetAPIReader()))

	l, err := net.Listen("tcp", mantleServiceEndpoint)
	if err != nil {
		return fmt.Errorf("failed to listen %s: %w", mantleServiceEndpoint, err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := serv.Serve(l)
		if err != nil {
			logger.Error(err, "gRPC server failed")
		}
		cancel()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		serv.GracefulStop()
	}()

	return setupReconcilers(mgr, nil)
}

func subMain() error {
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	if err := checkCommandlineArgs(); err != nil {
		setupLog.Error(err, "invalid command line arguments")
		return err
	}

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

	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch role {
	case controller.RoleStandalone:
		if err := setupStandalone(mgr); err != nil {
			return err
		}
	case controller.RolePrimary:
		if err := setupPrimary(ctx, mgr, &wg); err != nil {
			return err
		}
	case controller.RoleSecondary:
		if err := setupSecondary(ctx, mgr, &wg, cancel); err != nil {
			return err
		}
	}

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
