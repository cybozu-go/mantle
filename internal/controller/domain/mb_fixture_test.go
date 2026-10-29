package domain_test

import (
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type optionMB func(*mantlev1.MantleBackup)

func mbWithRemoteUID() optionMB {
	return func(backup *mantlev1.MantleBackup) {
		backup.Annotations[domain.AnnotRemoteUID] = "remote-uid"
	}
}

func mbWithClusterIDLabel(clusterID string) optionMB {
	return func(backup *mantlev1.MantleBackup) {
		backup.Labels[domain.LabelClusterID] = clusterID
	}
}

func mbWithAnnotation(key, value string) optionMB {
	return func(backup *mantlev1.MantleBackup) {
		backup.Annotations[key] = value
	}
}

func mbWithExpire(expire string) optionMB {
	return func(backup *mantlev1.MantleBackup) {
		backup.Spec.Expire = expire
	}
}

func mbWithCreatedAt(createdAt time.Time) optionMB {
	return func(backup *mantlev1.MantleBackup) {
		backup.Status.CreatedAt = metav1.NewTime(createdAt)
	}
}

func mbBeingDeleted() optionMB {
	return func(backup *mantlev1.MantleBackup) {
		now := metav1.Now()
		backup.DeletionTimestamp = &now
		backup.Finalizers = append(backup.Finalizers, "mantlebackup.mantle.cybozu.io/finalizer")
	}
}

func newMB(opts ...optionMB) *mantlev1.MantleBackup {
	backup := &mantlev1.MantleBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mb-name",
			Namespace:   "mb-namespace",
			UID:         "mb-uid",
			Labels:      map[string]string{},
			Annotations: map[string]string{},
		},
		Spec: mantlev1.MantleBackupSpec{
			PVC:    "pvc-name",
			Expire: "2w",
		},
	}
	for _, opt := range opts {
		opt(backup)
	}

	return backup
}
