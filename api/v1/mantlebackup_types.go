package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// MantleBackupSpec defines the desired state of MantleBackup
type MantleBackupSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// 'pvc' specifies backup target PVC
	// +kubebuilder:validation:Required
	PVC string `json:"pvc,omitempty"`

	// 'namespace' specifies backup target Namespace
	// +kubebuilder:validation:Required
	Namespace string `json:"namespace,omitempty"`
}

// MantleBackupStatus defines the observed state of MantleBackup
type MantleBackupStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// 'createdAt' specifies the creation date and time
	CreatedAt metav1.Time `json:"createdAt,omitempty"`

	// 'conditions' specifies current backup conditions
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// 'pvcManifest' saving backup target PVC manifests
	PVCManifest string `json:"pvcManifest,omitempty"`
	// 'pvManifest' saving backup target PV manifest
	PVManifest string `json:"pvManifest,omitempty"`

	// 'snapID' indicates SNAPID of `rbd snap ls`
	SnapID int `json:"snapID,omitempty"`
}

const (
	BackupConditionReadyToUse     = "ReadyToUse"
	BackupConditionSyncedToRemote = "SyncedToRemote"

	// Reasons for ConditionReadyToUse
	BackupReasonNone                 = "NoProblem"
	BackupReasonFailedToCreateBackup = "FailedToCreateBackup"
)

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=mb

// MantleBackup is the Schema for the mantlebackups API
type MantleBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MantleBackupSpec   `json:"spec,omitempty"`
	Status MantleBackupStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// MantleBackupList contains a list of MantleBackup
type MantleBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MantleBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MantleBackup{}, &MantleBackupList{})
}
