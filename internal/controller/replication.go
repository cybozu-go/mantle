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
	aerrors "k8s.io/apimachinery/pkg/api/errors"
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
	proto.UnimplementedMantleServiceServer
}

var _ proto.MantleServiceServer = &SecondaryServer{}

func NewSecondaryServer(client client.Client) *SecondaryServer {
	return &SecondaryServer{client: client}
}

func (s *SecondaryServer) CreateOrUpdatePVC(
	ctx context.Context,
	req *proto.CreateOrUpdatePVCRequest,
) (*proto.CreateOrUpdatePVCResponse, error) {
	// Unmarshal the request
	var pvcReceived corev1.PersistentVolumeClaim
	if err := json.Unmarshal(req.GetPvc(), &pvcReceived); err != nil {
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
		isCreate := pvc.CreationTimestamp.IsZero()
		if isCreate {
			// Copy only essential fields from the received PVC.
			pvc.Spec.AccessModes = pvcReceived.Spec.AccessModes
			pvc.Spec.StorageClassName = pvcReceived.Spec.StorageClassName
			pvc.Spec.VolumeAttributesClassName = pvcReceived.Spec.VolumeAttributesClassName
			pvc.Spec.VolumeMode = pvcReceived.Spec.VolumeMode
			pvc.Spec.Resources.Requests = pvcReceived.Spec.Resources.Requests
		} else {
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

		pvc.Annotations = pvcReceived.Annotations
		pvc.Labels = pvcReceived.Labels

		return nil
	}); err != nil {
		return nil, fmt.Errorf("CreateOrUpdate failed: %w", err)
	}

	// Get the created or updated PVC to fetch its UID.
	if err := s.client.Get(
		ctx, types.NamespacedName{Name: pvcReceived.GetName(), Namespace: pvcReceived.GetNamespace()}, &pvc,
	); err != nil {
		return nil, fmt.Errorf("failed to get PVC that should exist: %w", err)
	}

	return &proto.CreateOrUpdatePVCResponse{Uid: string(pvc.GetUID())}, nil
}

func (s *SecondaryServer) CreateMantleBackup(
	ctx context.Context,
	req *proto.CreateMantleBackupRequest,
) (*proto.CreateMantleBackupResponse, error) {
	var backupReceived mantlev1.MantleBackup
	if err := json.Unmarshal(req.GetMantleBackup(), &backupReceived); err != nil {
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

	var backupExists mantlev1.MantleBackup
	if err := s.client.Get(ctx, types.NamespacedName{
		Namespace: backupReceived.GetNamespace(),
		Name:      backupReceived.GetName(),
	}, &backupExists); err != nil {
		if !aerrors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get MantleBackup: %w", err)
		}
		// Not found, create a new one.
		// Note that s.client.Create clears the status field, so we preserve it via DeepCopy before creating.
		originalBackupReceived := backupReceived.DeepCopy()
		if err := s.client.Create(ctx, &backupReceived); err != nil {
			return nil, fmt.Errorf("failed to create MantleBackup: %w", err)
		}
		backupReceived.Status = originalBackupReceived.Status
		if err := s.client.Status().Update(ctx, &backupReceived); err != nil {
			return nil, fmt.Errorf("failed to update status of MantleBackup: %w", err)
		}

		return &proto.CreateMantleBackupResponse{}, nil
	}

	// Found, check it.
	if backupExists.Labels == nil || backupExists.Annotations == nil {
		return nil, fmt.Errorf("both labels and annotations must not be nil in the existing MantleBackup: %s/%s",
			backupReceived.GetNamespace(), backupReceived.GetName())
	}

	if pvcUID, ok := backupExists.Labels[labelLocalBackupTargetPVCUID]; !ok {
		return nil, fmt.Errorf("label %s not found in the existing MantleBackup: %s/%s",
			labelLocalBackupTargetPVCUID, backupReceived.GetNamespace(), backupReceived.GetName())
	} else if pvcUID != pvcUIDReceived {
		return nil, fmt.Errorf("label %s not matched in the existing MantleBackup: %s/%s",
			labelLocalBackupTargetPVCUID, backupReceived.GetNamespace(), backupReceived.GetName())
	}

	if remoteUID, ok := backupExists.Annotations[annotRemoteUID]; !ok {
		return nil, fmt.Errorf("annotation %s not found in the existing MantleBackup: %s/%s",
			annotRemoteUID, backupReceived.GetNamespace(), backupReceived.GetName())
	} else if remoteUID != remoteUIDReceived {
		return nil, fmt.Errorf("annotation %s not matched in the existing MantleBackup: %s/%s",
			annotRemoteUID, backupReceived.GetNamespace(), backupReceived.GetName())
	}

	// Update status if not set.
	if backupExists.Status.CreatedAt.IsZero() {
		newBackup := backupExists.DeepCopy()
		newBackup.Status.CreatedAt = backupReceived.Status.CreatedAt
		newBackup.Status.SnapSize = backupReceived.Status.SnapSize
		newBackup.Status.TransferPartSize = backupReceived.Status.TransferPartSize
		if err := s.client.Status().Patch(ctx, newBackup, client.MergeFrom(&backupExists)); err != nil {
			return nil, fmt.Errorf("status patch failed: %w", err)
		}
	}

	// Already exists with matching pvc-uid and remote-uid, do nothing.
	return &proto.CreateMantleBackupResponse{}, nil
}

func (s *SecondaryServer) ListMantleBackup(
	ctx context.Context,
	req *proto.ListMantleBackupRequest,
) (*proto.ListMantleBackupResponse, error) {
	var backupList mantlev1.MantleBackupList
	err := s.client.List(ctx, &backupList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{labelRemoteBackupTargetPVCUID: req.GetPvcUID()}),
		Namespace:     req.GetNamespace(),
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
		types.NamespacedName{Name: req.GetName(), Namespace: req.GetNamespace()},
		&target,
	); err != nil {
		return nil, err
	}

	if target.IsSnapshotCaptured() {
		return nil, errors.New("SnapshotCaptured is true")
	}

	// make sure sync-mode is correct.
	if syncMode, ok := target.GetAnnotations()[annotSyncMode]; ok {
		if syncMode == syncModeFull && req.DiffFrom != nil {
			return nil, fmt.Errorf("annotated sync-mode is full but req.DiffFrom is not nil: %s", req.GetDiffFrom())
		}
		if syncMode == syncModeIncremental && req.DiffFrom == nil {
			return nil, errors.New("annotated sync-mode is incremental but req.DiffFrom is nil")
		}
	}

	// make sure diff-from is correct.
	if diffFrom, ok := target.GetAnnotations()[annotDiffFrom]; ok {
		if req.DiffFrom == nil {
			return nil, errors.New("annotated diff-from is not nil but req.DiffFrom is nil")
		}
		if req.GetDiffFrom() != diffFrom {
			return nil, fmt.Errorf("annotated diff-from is not equal to req.DiffFrom: %s: %s", diffFrom, req.GetDiffFrom())
		}
	}

	if req.DiffFrom != nil {
		source := mantlev1.MantleBackup{}
		if err := s.client.Get(
			ctx,
			types.NamespacedName{Name: req.GetDiffFrom(), Namespace: req.GetNamespace()},
			&source,
		); err != nil {
			return nil, err
		}

		if diffTo, ok := source.GetAnnotations()[annotDiffTo]; ok && diffTo != req.GetName() {
			return nil, errors.New("diffTo is invalid")
		}

		annot := source.GetAnnotations()
		if annot == nil {
			annot = map[string]string{}
		}
		annot[annotDiffTo] = req.GetName()
		source.SetAnnotations(annot)

		if err := s.client.Update(ctx, &source); err != nil {
			return nil, err
		}
	}

	annot := target.GetAnnotations()
	if annot == nil {
		annot = map[string]string{}
	}
	if req.DiffFrom == nil {
		annot[annotSyncMode] = syncModeFull
	} else {
		annot[annotSyncMode] = syncModeIncremental
		annot[annotDiffFrom] = req.GetDiffFrom()
	}
	target.SetAnnotations(annot)

	if err := s.client.Update(ctx, &target); err != nil {
		return nil, err
	}

	return &proto.SetSynchronizingResponse{}, nil
}
