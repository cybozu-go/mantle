package metrics

import (
	"time"

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
			Help:      "The time from the creationTimestamp of the MantleBackup until it is synced to the secondary.",
			Buckets:   []float64{100, 200, 400, 800, 1600, 3200, 9600, 28800, 86400, 259200},
		},
		[]string{LabelPersistentVolumeClaim, LabelResourceNamespace},
	)

	BackupSnapshotDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "backup_snapshot_duration_seconds",
			Help:      "The time from the creationTimestamp of the MantleBackup until its snapshot is captured.",
			Buckets:   []float64{100, 200, 400, 800, 1600, 3200, 9600, 28800, 86400, 259200},
		},
		[]string{LabelPersistentVolumeClaim, LabelResourceNamespace},
	)

	BackupLastSnapshotCreationTimestampSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "backup_last_snapshot_creation_timestamp_seconds",
			Help:      "The time of the last snapshot creation (.status.createdAt).",
		},
		[]string{LabelPersistentVolumeClaim, LabelResourceNamespace},
	)

	BackupOldestUnsyncedCreationTimestampSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "backup_oldest_unsynced_creation_timestamp_seconds",
			Help:      "The creationTimestamp of the oldest MantleBackup whose snapshot is captured but not synced yet.",
		},
		[]string{LabelPersistentVolumeClaim, LabelResourceNamespace},
	)

	BackupLastCreationTimestampSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "backup_last_creation_timestamp_seconds",
			Help:      "The creationTimestamp of the newest MantleBackup.",
		},
		[]string{LabelPersistentVolumeClaim, LabelResourceNamespace},
	)
)

func init() {
	runtimemetrics.Registry.MustRegister(BackupExportedDiffSizeBytes)
	runtimemetrics.Registry.MustRegister(BackupConfigInfo)
	runtimemetrics.Registry.MustRegister(BackupConfigSuspend)
	runtimemetrics.Registry.MustRegister(BackupDurationSeconds)
	runtimemetrics.Registry.MustRegister(BackupSnapshotDurationSeconds)
	runtimemetrics.Registry.MustRegister(BackupLastSnapshotCreationTimestampSeconds)
	runtimemetrics.Registry.MustRegister(BackupOldestUnsyncedCreationTimestampSeconds)
	runtimemetrics.Registry.MustRegister(BackupLastCreationTimestampSeconds)
}

// SetOrDeleteTimestamp sets the series of gauge identified by labels to t in
// Unix time. It deletes the series instead of setting 0 when t is zero.
func SetOrDeleteTimestamp(gauge *prometheus.GaugeVec, labels prometheus.Labels, t time.Time) {
	if t.IsZero() {
		_ = gauge.Delete(labels)

		return
	}
	gauge.With(labels).Set(float64(t.Unix()))
}
