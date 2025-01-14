package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	runtimemetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const subsystem = "mantle"

var (
	BackupCreationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: subsystem,
			Name:      "backup_creation_duration_seconds",
			Help:      "Duration in seconds of backup creation.",
			Buckets:   []float64{100, 250, 500, 750, 1_000, 2_500, 5_000, 7_500, 10_000, 25_000, 50_000, 75_000, 100_000, 250_000},
		},
		[]string{"cluster_namespace", "pvc_namespace", "pvc", "source"},
	)
)

func init() {
	runtimemetrics.Registry.MustRegister(BackupCreationDuration)
}
