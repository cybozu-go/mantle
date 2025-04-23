package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"sync"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/resource"
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
	"github.com/cybozu-go/mantle/internal/controller"
	webhookv1 "github.com/cybozu-go/mantle/internal/webhook/v1"
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
	metricsAddr                  string
	enableLeaderElection         bool
	probeAddr                    string
	zapOpts                      zap.Options
	overwriteMBCSchedule         string
	role                         string
	mantleServiceEndpoint        string
	maxExportJobs                int
	maxUploadJobs                int
	exportDataStorageClass       string
	envSecret                    string
	objectStorageBucketName      string
	objectStorageEndpoint        string
	caCertConfigMapSrc           string
	caCertKeySrc                 string
	gcInterval                   string
	httpProxy                    string
	httpsProxy                   string
	noProxy                      string
	backupTransferPartSize       string
	replicationTLSClientCertPath string
	replicationTLSClientKeyPath  string
	replicationTLSClientCAPath   string
	replicationTLSServerCertPath string
	replicationTLSServerKeyPath  string
	replicationTLSServerCAPath   string
	webhookCertPath              string
	webhookKeyPath               string

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
	flags.IntVar(&maxUploadJobs, "max-upload-jobs", 8,
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
	flags.StringVar(&backupTransferPartSize, "backup-transfer-part-size", "200Gi",
		"The size of each backup data chunk to be transferred at a time.")
	flags.StringVar(&replicationTLSClientCertPath, "replication-tls-client-cert-path", "",
		"The file path of a TLS certificate used for client authentication of gRPC.")
	flags.StringVar(&replicationTLSClientKeyPath, "replication-tls-client-key-path", "",
		"The file path of a TLS key used for client authentication of gRPC.")
	flags.StringVar(&replicationTLSClientCAPath, "replication-tls-client-ca-path", "",
		"The file path of a TLS CA certificate that issues a certificate used for client authentication of gRPC.")
	flags.StringVar(&replicationTLSServerCertPath, "replication-tls-server-cert-path", "",
		"The file path of a TLS certificate used for server authentication of gRPC.")
	flags.StringVar(&replicationTLSServerKeyPath, "replication-tls-server-key-path", "",
		"The file path of a TLS key used for server authentication of gRPC.")
	flags.StringVar(&replicationTLSServerCAPath, "replication-tls-server-ca-path", "",
		"The file path of a TLS CA certificate that issues a certificate used for server authentication of gRPC.")
	flags.StringVar(&webhookCertPath, "webhook-cert-path", "",
		"The file path of the webhook certificate file.")
	flags.StringVar(&webhookKeyPath, "webhook-key-path", "",
		"The file path of the webhook key file.")

	goflags := flag.NewFlagSet("goflags", flag.ExitOnError)
	zapOpts.Development = true
	zapOpts.BindFlags(goflags)
	flags.AddGoFlagSet(goflags)

	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(mantlev1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func loadTLSCredentials(certPath, keyPath, caPath string) ([]tls.Certificate, *x509.CertPool, error) {
	var certs []tls.Certificate
	if certPath != "" || keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load X509 key pair: %w", err)
		}
		certs = []tls.Certificate{cert}
	}

	var ca *x509.CertPool
	if caPath != "" {
		ca = x509.NewCertPool()
		caBytes, err := os.ReadFile(caPath)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read server ca certificate: %s: %w", caPath, err)
		}
		if ok := ca.AppendCertsFromPEM(caBytes); !ok {
			return nil, nil, fmt.Errorf("faield to parse: %s", caPath)
		}
	}

	return certs, ca, nil
}

func checkCommandlineArgs() error {
	switch role {
	case controller.RoleStandalone:
		// nothing to do
		return nil

	case controller.RolePrimary:
		// checks specific to primary
		if (replicationTLSClientCertPath == "" && replicationTLSClientKeyPath != "") ||
			(replicationTLSClientCertPath != "" && replicationTLSClientKeyPath == "") {
			return errors.New("--replication-tls-client-cert-path and --replication-tls-client-key-path " +
				"must be specified together")
		}
		if replicationTLSClientCertPath != "" && replicationTLSServerCAPath == "" {
			return errors.New("--replication-tls-client-cert-path and --replication-tls-client-key-path " +
				"must be specified with --replication-tls-server-ca-path")
		}
		if replicationTLSServerCertPath != "" || replicationTLSServerKeyPath != "" || replicationTLSClientCAPath != "" {
			return errors.New("--replication-tls-server-cert-path, --replication-tls-server-key-path, " +
				"or --replication-tls-client-ca-path must not be specified if --role is 'primary'")
		}

	case controller.RoleSecondary:
		// checks specific to secondary
		if (replicationTLSServerCertPath == "" && replicationTLSServerKeyPath != "") ||
			(replicationTLSServerCertPath != "" && replicationTLSServerKeyPath == "") {
			return errors.New("--replication-tls-server-cert-path and --replication-tls-server-key-path " +
				"must be specified together")
		}
		if replicationTLSClientCAPath != "" && replicationTLSServerCertPath == "" {
			return errors.New("--replication-tls-client-ca-path must be specified with " +
				"--replication-tls-server-cert-path and --replication-tls-server-key-path")
		}
		if replicationTLSClientCertPath != "" || replicationTLSClientKeyPath != "" || replicationTLSServerCAPath != "" {
			return errors.New("--replication-tls-client-cert-path, --replication-tls-client-key-path, " +
				"or --replication-tls-server-ca-path must not be specified if --role is 'secondary'")
		}
		if webhookCertPath == "" {
			return errors.New("--webhook-cert-path must be specified if --role is 'secondary'")
		}
		if webhookKeyPath == "" {
			return errors.New("--webhook-key-path must be specified if --role is 'secondary'")
		}

	default:
		return fmt.Errorf("role must be one of 'standalone', 'primary', or 'secondary': %s", role)
	}

	// checks common to primary and secondary
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

	parsedBackupTransferPartSize, err := resource.ParseQuantity(backupTransferPartSize)
	if err != nil {
		setupLog.Error(err, "failed to parse backup-transfer-part-size", "value", backupTransferPartSize)
		return fmt.Errorf("failed to parse backup-transfer-part-size: %w", err)
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
		parsedBackupTransferPartSize,
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

	// Construct a gRPC client.
	clientCerts, serverCACertPool, err := loadTLSCredentials(
		replicationTLSClientCertPath, replicationTLSClientKeyPath, replicationTLSServerCAPath)
	if err != nil {
		return fmt.Errorf("failed to load TLS credentials for gRPC client: %w", err)
	}
	creds := insecure.NewCredentials()
	if clientCerts != nil || serverCACertPool != nil {
		url, err := url.ParseRequestURI("http://" + mantleServiceEndpoint)
		if err != nil {
			return fmt.Errorf("failed to parse mantle service endpoint: %s: %w", mantleServiceEndpoint, err)
		}
		creds = credentials.NewTLS(&tls.Config{
			ServerName:   url.Hostname(),
			Certificates: clientCerts,
			RootCAs:      serverCACertPool,
		})
	}
	conn, err := grpc.NewClient(
		mantleServiceEndpoint,
		grpc.WithTransportCredentials(creds),
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
		MaxUploadJobs:          maxUploadJobs,
		ExportDataStorageClass: exportDataStorageClass,
	}

	return setupReconcilers(mgr, primarySettings)
}

func setupSecondary(ctx context.Context, mgr manager.Manager, wg *sync.WaitGroup, cancel context.CancelFunc) error {
	logger := ctrl.Log.WithName("grpc")

	// Construct a gRPC server.
	grpcArgs := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			logging.UnaryServerInterceptor(
				logging.LoggerFunc(func(_ context.Context, _ logging.Level, msg string, fields ...any) {
					logger.Info(msg, fields...)
				}),
				logging.WithLogOnEvents(logging.StartCall, logging.FinishCall),
			),
		),
	}
	serverCerts, clientCACertPool, err := loadTLSCredentials(
		replicationTLSServerCertPath, replicationTLSServerKeyPath, replicationTLSClientCAPath)
	if err != nil {
		return fmt.Errorf("failed to load TLS credentials for gRPC server: %w", err)
	}
	if serverCerts != nil || clientCACertPool != nil {
		tlsConfig := &tls.Config{
			ClientCAs:    clientCACertPool,
			Certificates: serverCerts,
		}
		if clientCACertPool != nil {
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
		grpcArgs = append(grpcArgs, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	serv := grpc.NewServer(grpcArgs...)

	proto.RegisterMantleServiceServer(serv, controller.NewSecondaryServer(mgr.GetClient()))

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

	err = webhookv1.SetupVolumeAttachmentWebhookWithManager(mgr)
	if err != nil {
		return fmt.Errorf("failed to setup VolumeAttachment webhook: %w", err)
	}
	return setupReconcilers(mgr, nil)
}

func subMain() error {
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	if err := checkCommandlineArgs(); err != nil {
		setupLog.Error(err, "invalid command line arguments")
		return err
	}

	mgrOptions := ctrl.Options{
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
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mgr manager.Manager
	var err error
	switch role {
	case controller.RoleStandalone:
		mgr, err = ctrl.NewManager(ctrl.GetConfigOrDie(), mgrOptions)
		if err != nil {
			setupLog.Error(err, "unable to start manager")
			return err
		}

		if err := setupStandalone(mgr); err != nil {
			return err
		}
	case controller.RolePrimary:
		mgr, err = ctrl.NewManager(ctrl.GetConfigOrDie(), mgrOptions)
		if err != nil {
			setupLog.Error(err, "unable to start manager")
			return err
		}

		if err := setupPrimary(ctx, mgr, &wg); err != nil {
			return err
		}
	case controller.RoleSecondary:
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
		mgrOptions.WebhookServer = webhook.NewServer(webhook.Options{
			TLSOpts: webhookTLSOpts,
		})
		mgr, err = ctrl.NewManager(ctrl.GetConfigOrDie(), mgrOptions)
		if err != nil {
			setupLog.Error(err, "unable to start manager")
			return err
		}
		if err := setupSecondary(ctx, mgr, &wg, cancel); err != nil {
			return err
		}
		setupLog.Info("Adding webhook certificate watcher to manager")
		if err := mgr.Add(webhookCertWatcher); err != nil {
			setupLog.Error(err, "unable to add webhook certificate watcher to manager")
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
