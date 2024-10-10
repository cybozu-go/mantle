package controller

import (
	"context"
	"io"
	"os/exec"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func getCephClusterIDFromPVC(ctx context.Context, k8sClient client.Client, pvc *corev1.PersistentVolumeClaim) (string, error) {
	logger := log.FromContext(ctx)
	storageClassName := pvc.Spec.StorageClassName
	if storageClassName == nil {
		logger.Info("not managed storage class", "namespace", pvc.Namespace, "pvc", pvc.Name)
		return "", nil
	}
	var storageClass storagev1.StorageClass
	err := k8sClient.Get(ctx, types.NamespacedName{Name: *storageClassName}, &storageClass)
	if err != nil {
		return "", err
	}

	// Check if the MantleBackup resource being reconciled is managed by the CephCluster we are in charge of.
	if !strings.HasSuffix(storageClass.Provisioner, ".rbd.csi.ceph.com") {
		logger.Info("SC is not managed by RBD", "namespace", pvc.Namespace, "pvc", pvc.Name, "storageClassName", *storageClassName)
		return "", nil
	}
	clusterID, ok := storageClass.Parameters["clusterID"]
	if !ok {
		logger.Info("clusterID not found", "namespace", pvc.Namespace, "pvc", pvc.Name, "storageClassName", *storageClassName)
		return "", nil
	}

	return clusterID, nil
}

func updateStatus(ctx context.Context, client client.Client, obj client.Object, mutator func() error) error {
	if err := client.Get(ctx, types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}, obj); err != nil {
		return err
	}
	if err := mutator(); err != nil {
		return err
	}
	if err := client.Status().Update(ctx, obj); err != nil {
		return err
	}
	return nil
}

func executeCommandImpl(ctx context.Context, command []string, input io.Reader) ([]byte, error) {
	logger := log.FromContext(ctx)
	cmd := exec.Command(command[0], command[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	defer func() {
		err := stdout.Close()
		if err != nil {
			logger.Error(err, "failed to stdout.Close")
		}
	}()

	if input != nil {
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return nil, err
		}
		go func() {
			defer func() {
				err := stdin.Close()
				if err != nil {
					logger.Error(err, "failed to stdin.Close")
				}
			}()
			if _, err = io.Copy(stdin, input); err != nil {
				logger.Error(err, "failed to io.Copy")
			}
		}()
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	r, err := io.ReadAll(stdout)
	if err != nil {
		return r, err
	}

	if err := cmd.Wait(); err != nil {
		return r, err
	}

	return r, nil
}

// It will replaced on tests.
var executeCommand = executeCommandImpl
