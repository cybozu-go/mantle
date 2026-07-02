package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	runtimemetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const namespace = "mantle"

var (
	BackupExportedDiffSizeBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "backup_exported_diff_size_bytes",
			Help:      "Total bytes of the uploaded diff parts.",
		},
		[]string{"persistentvolumeclaim", "resource_namespace", "mantlebackup"},
	)

	BackupConfigInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "mantlebackupconfig_info",
			Help:      "Information about the backup configuration.",
		},
		[]string{"persistentvolumeclaim", "resource_namespace", "mantlebackupconfig"},
	)

	BackupDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "backup_duration_seconds",
			Help:      "The time from the creationTimestamp to the completion of the backup.",
			Buckets:   []float64{100, 200, 400, 800, 1600, 3200, 9600, 28800, 86400, 259200},
		},
		[]string{"persistentvolumeclaim", "resource_namespace"},
	)
)

func init() {
	runtimemetrics.Registry.MustRegister(BackupExportedDiffSizeBytes)
	runtimemetrics.Registry.MustRegister(BackupConfigInfo)
	runtimemetrics.Registry.MustRegister(BackupDurationSeconds)
}
