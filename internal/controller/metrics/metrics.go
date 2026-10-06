package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	runtimemetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const namespace = "mantle"

// Label names of the metrics. Callers must use them as the keys of prometheus.Labels.
const (
	LabelPersistentVolumeClaim = "persistentvolumeclaim"
	LabelResourceNamespace     = "resource_namespace"
	LabelMantleBackup          = "mantlebackup"
	LabelMantleBackupConfig    = "mantlebackupconfig"
)

var (
	BackupExportedDiffSizeBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "backup_exported_diff_size_bytes",
			Help:      "The size of the uploaded backup diff data",
		},
		[]string{LabelPersistentVolumeClaim, LabelResourceNamespace, LabelMantleBackup},
	)

	BackupConfigInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "mantlebackupconfig_info",
			Help:      "Information about the backup configuration.",
		},
		[]string{LabelPersistentVolumeClaim, LabelResourceNamespace, LabelMantleBackupConfig},
	)

	BackupConfigSuspend = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "mantlebackupconfig_suspend",
			Help:      "Indicates whether the backup configuration is suspended.",
		},
		[]string{LabelResourceNamespace, LabelMantleBackupConfig},
	)

	BackupDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "backup_duration_seconds",
			Help:      "The time from the creationTimestamp to the completion of the backup.",
			Buckets:   []float64{100, 200, 400, 800, 1600, 3200, 9600, 28800, 86400, 259200},
		},
		[]string{LabelPersistentVolumeClaim, LabelResourceNamespace},
	)
)

func init() {
	runtimemetrics.Registry.MustRegister(BackupExportedDiffSizeBytes)
	runtimemetrics.Registry.MustRegister(BackupConfigInfo)
	runtimemetrics.Registry.MustRegister(BackupConfigSuspend)
	runtimemetrics.Registry.MustRegister(BackupDurationSeconds)
}
