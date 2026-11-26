package controller

import (
	"context"
	"fmt"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	runtimemetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// backupMetricsCollector builds the per-MantleBackup metrics from the cache at scrape time,
// so that a series never outlives its MantleBackup.
type backupMetricsCollector struct {
	reader               client.Reader
	managedCephClusterID string
}

func newBackupMetricsCollector(reader client.Reader, managedCephClusterID string) *backupMetricsCollector {
	return &backupMetricsCollector{
		reader:               reader,
		managedCephClusterID: managedCephClusterID,
	}
}

// Start registers the collector until ctx is canceled.
func (c *backupMetricsCollector) Start(ctx context.Context) error {
	if err := runtimemetrics.Registry.Register(c); err != nil {
		return fmt.Errorf("failed to register the backup metrics collector: %w", err)
	}
	<-ctx.Done()
	runtimemetrics.Registry.Unregister(c)

	return nil
}

func (c *backupMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- metrics.BackupCreationTimestampSecondsDesc
	ch <- metrics.BackupSyncCompletionTimestampSecondsDesc
}

func (c *backupMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	ctx := context.Background()

	var backups mantlev1.MantleBackupList
	if err := c.reader.List(ctx, &backups, client.MatchingLabels{domain.LabelClusterID: c.managedCephClusterID}); err != nil {
		log.FromContext(ctx).Error(err, "failed to list MantleBackups")

		return
	}

	for i := range backups.Items {
		backup := &backups.Items[i]

		// MantleBackups received as secondary or being deleted are not exported.
		if _, ok := backup.GetAnnotations()[domain.AnnotRemoteUID]; ok || !backup.DeletionTimestamp.IsZero() {
			continue
		}

		labelValues := []string{backup.Spec.PVC, backup.GetNamespace(), backup.GetName()}
		ch <- prometheus.MustNewConstMetric(metrics.BackupCreationTimestampSecondsDesc,
			prometheus.GaugeValue, float64(backup.CreationTimestamp.Unix()), labelValues...)

		cond := meta.FindStatusCondition(backup.Status.Conditions, mantlev1.BackupConditionSyncedToRemote)
		if cond != nil && cond.Status == metav1.ConditionTrue {
			ch <- prometheus.MustNewConstMetric(metrics.BackupSyncCompletionTimestampSecondsDesc,
				prometheus.GaugeValue, float64(cond.LastTransitionTime.Unix()), labelValues...)
		}
	}
}
