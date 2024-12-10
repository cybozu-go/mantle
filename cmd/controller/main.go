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
	metricsAddr             string
	enableLeaderElection    bool
	probeAddr               string
	zapOpts                 zap.Options
	overwriteMBCSchedule    string
	role                    string
	mantleServiceEndpoint   string
	maxExportJobs           int
	exportDataStorageClass  string
	envSecret               string
	objectStorageBucketName string
	objectStorageEndpoint   string
	caCertConfigMapSrc      string
	caCertKeySrc            string
	gcInterval              string
	httpProxy               string
	httpsProxy              string
	noProxy                 string

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
	flags.StringVar(&exportDataStorageClass, "export-data-storage-class", "",
		"The storage class of PVCs used to store exported data temporarily.")
	flags.StringVar(&envSecret, "env-secret", "",
		"The name of the Secret resource that contains environment variables related to the controller and Jobs.")
	flags.StringVar(&objectStorageBucketName, "object-storage-bucket-name", "",
		"The bucket name of the object storage which should be used to store backups.")
	flags.StringVar(&objectStorageEndpoint, "object-storage-endpoint", "",
		"The endpoint URL to access the object storage.")
	flags.StringVar(&caCertConfigMapSrc, "ca-cert-configmap", "",
		"The name of the ConfigMap resource that contains the intermediate certificate used to access the object storage.")
	flags.StringVar(&caCertKeySrc, "ca-cert-key", "ca.crt",
		"The key of the ConfigMap specified by --ca-cert-config-map that contains the intermediate certificate. "+
			"The default value is ca.crt. This option is just ignored if --ca-cert-configmap isn't specified.")
	flags.StringVar(&gcInterval, "gc-interval", "1h",
		"The time period between each garbage collection for orphaned resources.")
	flags.StringVar(&httpProxy, "http-proxy", "",
		"The proxy URL for HTTP requests to the object storage and the gRPC endpoint of secondary mantle.")
	flags.StringVar(&httpsProxy, "https-proxy", "",
		"The proxy URL for HTTPS requests to the object storage and the gRPC endpoint of secondary mantle.")
	flags.StringVar(&noProxy, "no-proxy", "",
		"A string that contains comma-separated values specifying hosts that should be excluded from proxying.")

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
		fallthrough
	case controller.RoleSecondary:
		if mantleServiceEndpoint == "" {
			return errors.New("--mantle-service-endpoint must be specified if --role is 'primary' or 'secondary'")
		}
		if caCertConfigMapSrc != "" && caCertKeySrc == "" {
			return errors.New("--ca-cert-key must be specified if --role is 'primary' or 'secondary', " +
				"and --ca-cert-configmap is specified")
		}
		if objectStorageBucketName == "" {
			return errors.New("--object-storage-bucket-name must be specified if --role is 'primary' or 'secondary'")
		}
		if objectStorageEndpoint == "" {
			return errors.New("--object-storage-endpoint must be specified if --role is 'primary' or 'secondary'")
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

	podImage := os.Getenv("POD_IMAGE")
	if podImage == "" {
		setupLog.Error(errors.New("POD_IMAGE must not be empty"), "POD_IMAGE must not be empty")
		return errors.New("POD_IMAGE is empty")
	}

	var caCertConfigMap *string
	if caCertConfigMapSrc != "" {
		caCertConfigMap = &caCertConfigMapSrc
	}

	parsedGCInterval, err := time.ParseDuration(gcInterval)
	if err != nil {
		setupLog.Error(err, "faield to parse gc interval", "gcInterval", gcInterval)
		return err
	}
	if parsedGCInterval < 1*time.Second {
		err := fmt.Errorf("the specified gc interval is too short: %s", parsedGCInterval.String())
		setupLog.Error(err, "failed to validate gc interval", "gcInterval", gcInterval)
		return err
	}

	backupReconciler := controller.NewMantleBackupReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		managedCephClusterID,
		role,
		primarySettings,
		podImage,
		envSecret,
		&controller.ObjectStorageSettings{
			BucketName:      objectStorageBucketName,
			Endpoint:        objectStorageEndpoint,
			CACertConfigMap: caCertConfigMap,
			CACertKey:       &caCertKeySrc,
		},
		&controller.ProxySettings{
			HttpProxy:  httpProxy,
			HttpsProxy: httpsProxy,
			NoProxy:    noProxy,
		},
	)
	if err := backupReconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MantleBackup")
		return err
	}

	restoreReconciler := controller.NewMantleRestoreReconciler(
		mgr.GetClient(),
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

	if err := controller.NewPersistentVolumeReconciler(
		mgr.GetClient(),
		mgr.GetScheme(),
		managedCephClusterID,
	).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "PersistentVolumeReconciler")
		return err
	}

	//+kubebuilder:scaffold:builder

	if err := mgr.Add(
		controller.NewGarbageCollectorRunner(mgr.GetClient(), parsedGCInterval, managedCephClusterID),
	); err != nil {
		setupLog.Error(err, "unable to create runner", "runner", "GarbageCollectorRunner")
		return err
	}

	return nil
}

func setupStandalone(mgr manager.Manager) error {
	return setupReconcilers(mgr, nil)
}

func setupPrimary(ctx context.Context, mgr manager.Manager, wg *sync.WaitGroup) error {
	// Setup environment variables related to proxies before creating a gRPC client.
	// cf. https://github.com/grpc/grpc-go/blob/adad26df1826bf2fb66ad56ff32a62b98bf5cb3a/Documentation/proxy.md
	// cf. https://pkg.go.dev/golang.org/x/net/http/httpproxy
	if err := os.Setenv("HTTP_PROXY", httpProxy); err != nil {
		setupLog.Error(err, "failed to set HTTP_PROXY environment variable")
		return err
	}
	if err := os.Setenv("HTTPS_PROXY", httpsProxy); err != nil {
		setupLog.Error(err, "failed to set HTTPS_PROXY environment variable")
		return err
	}
	if err := os.Setenv("NO_PROXY", noProxy); err != nil {
		setupLog.Error(err, "failed to set NO_PROXY environment variable")
		return err
	}

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
		ServiceEndpoint:        mantleServiceEndpoint,
		Conn:                   conn,
		Client:                 proto.NewMantleServiceClient(conn),
		MaxExportJobs:          maxExportJobs,
		ExportDataStorageClass: exportDataStorageClass,
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
