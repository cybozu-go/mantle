package cluster

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/cybozu-go/mantle/test/util"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func Rbd(args ...string) ([]byte, error) {
	return Kubectl(append([]string{"exec", "-n", ROOK_NAMESPACE, "deploy/rook-ceph-tools", "--", "rbd"}, args...)...)
}

func ExportDiff(filename string, args ...string) error {
	_, err := Kubectl("exec", "-n", ROOK_NAMESPACE, "deploy/rook-ceph-tools", "--", "rm", "-f", filename)
	if err != nil {
		return err
	}
	args = append([]string{"export-diff"}, args...)
	args = append(args, filename)
	stdout, err := Rbd(args...)
	if err != nil {
		return fmt.Errorf("failed to run rbd export-diff command: %w, %s", err, string(stdout))
	}
	return nil
}

func ImportDiff(filename, pool, image, rollbackTo, namespace, deployName, pvcName string) error {
	return RunWithStopPod(namespace, deployName, func() error {
		if rollbackTo == "" {
			if len(pvcName) == 0 {
				return fmt.Errorf("rollbackTo or pvcName must be specified")
			}
			err := zeroOutVolume(namespace, pvcName)
			if err != nil {
				return fmt.Errorf("failed to zero out volume: %w", err)
			}
		} else {
			stdout, err := Rbd("snap", "rollback", pool+"/"+image+"@"+rollbackTo)
			if err != nil {
				return fmt.Errorf("failed to rollback snapshot: %w, %s", err, string(stdout))
			}
		}

		stdout, err := Kubectl("exec", "-n", ROOK_NAMESPACE, "deploy/rook-ceph-tools", "--",
			"sh", "-c", fmt.Sprintf("cat %s | rbd import-diff -p %s - %s", filename, pool, image))
		if err != nil {
			return fmt.Errorf("failed to import diff: %w, %s", err, string(stdout))
		}
		return nil
	})
}

var mtxZeroOutVolume = &sync.Mutex{}

func zeroOutVolume(namespace, pvcName string) error {
	mtxZeroOutVolume.Lock()
	defer mtxZeroOutVolume.Unlock()

	origPVCRaw, err := Kubectl("get", "-n", namespace, "pvc", pvcName, "-o", "json")
	if err != nil {
		return fmt.Errorf("failed to get PVC: %w", err)
	}
	var origPVC corev1.PersistentVolumeClaim
	if err := json.Unmarshal(origPVCRaw, &origPVC); err != nil {
		return fmt.Errorf("failed to unmarshal PVC: %w", err)
	}

	origPVRaw, err := Kubectl("get", "pv", origPVC.Spec.VolumeName, "-o", "json")
	if err != nil {
		return fmt.Errorf("failed to get PV: %w", err)
	}
	var origPV corev1.PersistentVolume
	if err := json.Unmarshal(origPVRaw, &origPV); err != nil {
		return fmt.Errorf("failed to unmarshal PV: %w", err)
	}

	zeroOutPVName := util.GetUniqueName("zeroout-pv-")
	zeroOutPVCName := util.GetUniqueName("zeroout-pvc-")
	zeroOutDeployName := util.GetUniqueName("zeroout-pod-")

	zeroOutPV := corev1.PersistentVolume{
		TypeMeta: origPV.TypeMeta,
		ObjectMeta: v1.ObjectMeta{
			Name: zeroOutPVName,
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Capacity:                      origPV.Spec.Capacity,
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			VolumeMode:                    ptr.To(corev1.PersistentVolumeBlock),
			StorageClassName:              "",
		},
	}
	zeroOutPV.Spec.CSI = &corev1.CSIPersistentVolumeSource{
		Driver:                    origPV.Spec.CSI.Driver,
		ControllerExpandSecretRef: origPV.Spec.CSI.ControllerExpandSecretRef,
		NodeStageSecretRef:        origPV.Spec.CSI.NodeStageSecretRef,
		VolumeAttributes: map[string]string{
			"clusterID":     origPV.Spec.CSI.VolumeAttributes["clusterID"],
			"imageFeatures": origPV.Spec.CSI.VolumeAttributes["imageFeatures"],
			"imageFormat":   origPV.Spec.CSI.VolumeAttributes["imageFormat"],
			"pool":          origPV.Spec.CSI.VolumeAttributes["pool"],
			"staticVolume":  "true",
		},
		VolumeHandle: origPV.Spec.CSI.VolumeAttributes["imageName"],
	}
	zeroOutPVRaw, err := json.Marshal(zeroOutPV)
	if err != nil {
		return fmt.Errorf("failed to marshal PV: %w", err)
	}
	_, err = KubectlWithInput(zeroOutPVRaw, "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("failed to create PV: %w", err)
	}

	zeroOutPVC := corev1.PersistentVolumeClaim{
		TypeMeta: origPVC.TypeMeta,
		ObjectMeta: v1.ObjectMeta{
			Name:      zeroOutPVCName,
			Namespace: namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: ptr.To(""),
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources:  origPVC.Spec.Resources,
			VolumeMode: ptr.To(corev1.PersistentVolumeBlock),
			VolumeName: zeroOutPVName,
		},
	}
	zeroOutPVCRaw, err := json.Marshal(zeroOutPVC)
	if err != nil {
		return fmt.Errorf("failed to marshal PVC: %w", err)
	}
	_, err = KubectlWithInput(zeroOutPVCRaw, "apply", "-f", "-")
	if err != nil {
		return fmt.Errorf("failed to create PVC: %w", err)
	}

	err = CreateDeployment(namespace, zeroOutDeployName, zeroOutPVCName, VolumeModeBlock)
	if err != nil {
		return fmt.Errorf("failed to create pod: %w", err)
	}

	if err := ZeroOutBlock(namespace, zeroOutDeployName); err != nil {
		return err
	}

	err = DeleteObject("deployment", namespace, zeroOutDeployName)
	if err != nil {
		return fmt.Errorf("failed to delete pod: %w", err)
	}

	err = DeleteObject("pvc", namespace, zeroOutPVCName)
	if err != nil {
		return fmt.Errorf("failed to delete PVC: %w", err)
	}

	err = DeleteObject("pv", "", zeroOutPVName)
	if err != nil {
		return fmt.Errorf("failed to delete PV: %w", err)
	}

	return nil
}

func SnapCreate(pool, image, snap string) error {
	stdout, err := Rbd("snap", "create", pool+"/"+image+"@"+snap)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w, %s", err, string(stdout))
	}
	return nil
}

func SnapRemove(pool, image string, snaps []string) error {
	for _, snap := range snaps {
		stdout, err := Rbd("snap", "rm", pool+"/"+image+"@"+snap)
		if err != nil {
			return fmt.Errorf("failed to remove snapshot: %w, %s", err, string(stdout))
		}
	}
	return nil
}

func SnapRemoveAll(pool, image string) error {
	stdout, err := Rbd("snap", "ls", pool+"/"+image, "--format", "json")
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %w, %s", err, string(stdout))
	}

	var snaps []SnapLsEntry
	if err := json.Unmarshal(stdout, &snaps); err != nil {
		return fmt.Errorf("failed to unmarshal snapshots: %w", err)
	}

	snapNames := make([]string, 0, len(snaps))
	for _, s := range snaps {
		snapNames = append(snapNames, s.Name)
	}

	return SnapRemove(pool, image, snapNames)
}

func SnapRollback(pool, image, snap, namespace, deployName string) error {
	return RunWithStopPod(namespace, deployName, func() error {
		stdout, err := Rbd("snap", "rollback", pool+"/"+image+"@"+snap)
		if err != nil {
			return fmt.Errorf("failed to rollback snapshot: %w, %s", err, string(stdout))
		}
		return nil
	})
}

type SnapLsEntry struct {
	Name string `json:"name"`
}

func SnapExists(pool, image, snap string) (bool, error) {
	stdout, err := Rbd("snap", "ls", pool+"/"+image, "--format", "json")
	if err != nil {
		return false, fmt.Errorf("failed to list snapshots: %w, %s", err, string(stdout))
	}

	var snaps []SnapLsEntry
	if err := json.Unmarshal(stdout, &snaps); err != nil {
		return false, fmt.Errorf("failed to unmarshal snapshots: %w", err)
	}

	for _, s := range snaps {
		if s.Name == snap {
			return true, nil
		}
	}
	return false, nil
}
