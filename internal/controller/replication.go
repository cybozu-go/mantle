package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/pkg/controller/proto"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	RoleStandalone = "standalone"
	RolePrimary    = "primary"
	RoleSecondary  = "secondary"
)

type PrimarySettings struct {
	ServiceEndpoint        string
	Conn                   *grpc.ClientConn
	Client                 proto.MantleServiceClient
	MaxExportJobs          int
	MaxUploadJobs          int
	ExportDataStorageClass string
}

type SecondaryServer struct {
	client client.Client
	reader client.Reader // reader should not have cache.
	proto.UnimplementedMantleServiceServer
}

var _ proto.MantleServiceServer = &SecondaryServer{}

func NewSecondaryServer(client client.Client, reader client.Reader) *SecondaryServer {
	return &SecondaryServer{client: client, reader: reader}
}

func (s *SecondaryServer) CreateOrUpdatePVC(
	ctx context.Context,
	req *proto.CreateOrUpdatePVCRequest,
) (*proto.CreateOrUpdatePVCResponse, error) {
	// Unmarshal the request
	var pvcReceived corev1.PersistentVolumeClaim
	if err := json.Unmarshal(req.Pvc, &pvcReceived); err != nil {
		return nil, fmt.Errorf("failed to unmarshal the requested PVC: %w", err)
	}

	// Get the remote-uid of the PVC in the request.
	remoteUIDReceived, ok := pvcReceived.Annotations[annotRemoteUID]
	if !ok {
		return nil, fmt.Errorf("annotation not found in the received PVC: %s: %s: %s",
			annotRemoteUID, pvcReceived.GetName(), pvcReceived.GetNamespace())
	}

	// Create or update the requested PVC.
	var pvc corev1.PersistentVolumeClaim
	pvc.SetName(pvcReceived.GetName())
	pvc.SetNamespace(pvcReceived.GetNamespace())
	if _, err := ctrl.CreateOrUpdate(ctx, s.client, &pvc, func() error {
		if !pvc.CreationTimestamp.IsZero() {
			// Make sure the remote-uids are equal.
			errMsg := ""
			if pvc.Annotations == nil {
				errMsg = "annotations field is nil in pvc"
			} else {
				remoteUID, ok := pvc.Annotations[annotRemoteUID]
				if !ok {
					errMsg = "annotation not found in pvc"
				} else if remoteUID != remoteUIDReceived {
					errMsg = "annotation not matched"
				}
			}
			if errMsg != "" {
				return fmt.Errorf("%s: %s: %s: %s",
					errMsg, annotRemoteUID, pvcReceived.GetName(), pvcReceived.GetNamespace())
			}
		}

		pvc.ObjectMeta.Annotations = pvcReceived.Annotations
		pvc.ObjectMeta.Labels = pvcReceived.Labels

		// We should NOT use pvcReceived.Spec.VolumeName. It's a PV name in the primary k8s server.
		volumeName := pvc.Spec.VolumeName
		pvc.Spec = pvcReceived.Spec
		pvc.Spec.VolumeName = volumeName
		return nil
	}); err != nil {
		return nil, fmt.Errorf("CreateOrUpdate failed: %w", err)
	}

	// Get the created or updated PVC to fetch its UID.
	// The client's cache may not be updated, so use the reader without cache instead.
	if err := s.reader.Get(
		ctx, types.NamespacedName{Name: pvcReceived.GetName(), Namespace: pvcReceived.GetNamespace()}, &pvc,
	); err != nil {
		return nil, fmt.Errorf("failed to get PVC that should exist: %w", err)
	}

	return &proto.CreateOrUpdatePVCResponse{Uid: string(pvc.GetUID())}, nil
}

func (s *SecondaryServer) CreateOrUpdateMantleBackup(
	ctx context.Context,
	req *proto.CreateOrUpdateMantleBackupRequest,
) (*proto.CreateOrUpdateMantleBackupResponse, error) {
	var backupReceived mantlev1.MantleBackup
	if err := json.Unmarshal(req.MantleBackup, &backupReceived); err != nil {
		return nil, err
	}

	if backupReceived.Labels == nil || backupReceived.Annotations == nil {
		return nil, fmt.Errorf("both labels and annotations must not be nil in the received MantleBackup: %s: %s",
			backupReceived.GetName(), backupReceived.GetNamespace())
	}

	_, ok := backupReceived.Labels[labelRemoteBackupTargetPVCUID]
	if !ok {
		return nil, fmt.Errorf("label not found in the received MantleBackup: %s: %s: %s",
			labelRemoteBackupTargetPVCUID, backupReceived.GetName(), backupReceived.GetNamespace())
	}
	pvcUIDReceived, ok := backupReceived.Labels[labelLocalBackupTargetPVCUID]
	if !ok {
		return nil, fmt.Errorf("label not found in the received MantleBackup: %s: %s: %s",
			labelLocalBackupTargetPVCUID, backupReceived.GetName(), backupReceived.GetNamespace())
	}
	remoteUIDReceived, ok := backupReceived.Annotations[annotRemoteUID]
	if !ok {
		return nil, fmt.Errorf("annotation not found in the received MantleBackup: %s: %s: %s",
			annotRemoteUID, backupReceived.GetName(), backupReceived.GetNamespace())
	}

	var backup mantlev1.MantleBackup
	backup.SetName(backupReceived.GetName())
	backup.SetNamespace(backupReceived.GetNamespace())
	if _, err := ctrl.CreateOrUpdate(ctx, s.client, &backup, func() error {
		if !backup.CreationTimestamp.IsZero() {
			errMsg := ""
			if backup.Labels == nil {
				errMsg = "labels field is nil in backup"
			} else {
				pvcUID, ok := backup.Labels[labelLocalBackupTargetPVCUID]
				if !ok {
					errMsg = "label not found"
				} else if pvcUID != pvcUIDReceived {
					errMsg = "label not matched"
				}
			}
			if errMsg != "" {
				return fmt.Errorf("%s: %s: %s: %s",
					errMsg, labelLocalBackupTargetPVCUID, backupReceived.GetName(), backupReceived.GetNamespace())
			}

			if backup.Annotations == nil {
				errMsg = "annotation field is nil in backup"
			} else {
				remoteUID, ok := backup.Annotations[annotRemoteUID]
				if !ok {
					errMsg = "annotation not found in backup"
				} else if remoteUID != remoteUIDReceived {
					errMsg = "annotation not matched"
				}
			}
			if errMsg != "" {
				return fmt.Errorf("%s: %s: %s: %s",
					errMsg, annotRemoteUID, backupReceived.GetName(), backupReceived.GetNamespace())
			}
		}

		backup.ObjectMeta.Finalizers = backupReceived.Finalizers
		backup.ObjectMeta.Annotations = backupReceived.Annotations
		backup.ObjectMeta.Labels = backupReceived.Labels
		backup.Spec = backupReceived.Spec
		return nil
	}); err != nil {
		return nil, fmt.Errorf("CreateOrUpdate failed: %w", err)
	}

	// Use Patch here because updateStatus is likely to fail due to "the object has been modified" error.
	newBackup := backup.DeepCopy()
	newBackup.Status.CreatedAt = backupReceived.Status.CreatedAt
	newBackup.Status.SnapSize = backupReceived.Status.SnapSize
	newBackup.Status.TransferPartSize = backupReceived.Status.TransferPartSize
	if err := s.client.Status().Patch(ctx, newBackup, client.MergeFrom(&backup)); err != nil {
		return nil, fmt.Errorf("status patch failed: %w", err)
	}

	return &proto.CreateOrUpdateMantleBackupResponse{}, nil
}

func (s *SecondaryServer) ListMantleBackup(
	ctx context.Context,
	req *proto.ListMantleBackupRequest,
) (*proto.ListMantleBackupResponse, error) {
	var backupList mantlev1.MantleBackupList
	err := s.reader.List(ctx, &backupList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{labelRemoteBackupTargetPVCUID: req.PvcUID}),
		Namespace:     req.Namespace,
	})
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(backupList.Items)
	if err != nil {
		return nil, err
	}
	return &proto.ListMantleBackupResponse{MantleBackupList: data}, nil
}

func (s *SecondaryServer) SetSynchronizing(
	ctx context.Context,
	req *proto.SetSynchronizingRequest,
) (*proto.SetSynchronizingResponse, error) {
	target := mantlev1.MantleBackup{}
	if err := s.client.Get(
		ctx,
		types.NamespacedName{Name: req.Name, Namespace: req.Namespace},
		&target,
	); err != nil {
		return nil, err
	}

	if meta.IsStatusConditionTrue(target.Status.Conditions, mantlev1.BackupConditionReadyToUse) {
		return nil, errors.New("ReadyToUse is true")
	}

	// make sure sync-mode is correct.
	if syncMode, ok := target.GetAnnotations()[annotSyncMode]; ok {
		if syncMode == syncModeFull && req.DiffFrom != nil {
			return nil, fmt.Errorf("annotated sync-mode is full but req.DiffFrom is not nil: %s", *req.DiffFrom)
		}
		if syncMode == syncModeIncremental && req.DiffFrom == nil {
			return nil, fmt.Errorf("annotated sync-mode is incremental but req.DiffFrom is nil")
		}
	}

	// make sure diff-from is correct.
	if diffFrom, ok := target.GetAnnotations()[annotDiffFrom]; ok {
		if req.DiffFrom == nil {
			return nil, errors.New("annotated diff-from is not nil but req.DiffFrom is nil")
		}
		if *req.DiffFrom != diffFrom {
			return nil, fmt.Errorf("annotated diff-from is not equal to req.DiffFrom: %s: %s", diffFrom, *req.DiffFrom)
		}
	}

	if req.DiffFrom != nil {
		source := mantlev1.MantleBackup{}
		if err := s.client.Get(
			ctx,
			types.NamespacedName{Name: *req.DiffFrom, Namespace: req.Namespace},
			&source,
		); err != nil {
			return nil, err
		}

		if diffTo, ok := source.GetAnnotations()[annotDiffTo]; ok && diffTo != req.Name {
			return nil, errors.New("diffTo is invalid")
		}

		if _, err := ctrl.CreateOrUpdate(ctx, s.client, &source, func() error {
			annot := source.GetAnnotations()
			if annot == nil {
				annot = map[string]string{}
			}
			annot[annotDiffTo] = req.Name
			source.SetAnnotations(annot)
			return nil
		}); err != nil {
			return nil, err
		}
	}

	if _, err := ctrl.CreateOrUpdate(ctx, s.client, &target, func() error {
		annot := target.GetAnnotations()
		if annot == nil {
			annot = map[string]string{}
		}
		if req.DiffFrom == nil {
			annot[annotSyncMode] = syncModeFull
		} else {
			annot[annotSyncMode] = syncModeIncremental
			annot[annotDiffFrom] = *req.DiffFrom
		}
		target.SetAnnotations(annot)
		return nil
	}); err != nil {
		return nil, err
	}

	return &proto.SetSynchronizingResponse{}, nil
}
