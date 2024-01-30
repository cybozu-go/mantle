package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// RBDPVCBackupSpec defines the desired state of RBDPVCBackup
type RBDPVCBackupSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// 'pvc' specifies backup target PVC
	// +kubebuilder:validation:Required
	PVC string `json:"pvc,omitempty"`
}

// RBDPVCBackupStatus defines the observed state of RBDPVCBackup
type RBDPVCBackupStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// 'createdAt' specifies the creation date and time
	CreatedAt metav1.Time `json:"createdAt,omitempty"`

	// 'conditions' specifies current backup conditions
	// +kubebuilder:validation:Enum=Creating;Bound;Failed;Deleting
	Conditions string `json:"conditions,omitempty"`
}

const (
	RBDPVCBackupConditionsCreating = "Creating"
	RBDPVCBackupConditionsBound    = "Bound"
	RBDPVCBackupConditionsFailed   = "Failed"
	RBDPVCBackupConditionsDeleting = "Deleting"
)

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// RBDPVCBackup is the Schema for the rbdpvcbackups API
type RBDPVCBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RBDPVCBackupSpec   `json:"spec,omitempty"`
	Status RBDPVCBackupStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// RBDPVCBackupList contains a list of RBDPVCBackup
type RBDPVCBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RBDPVCBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RBDPVCBackup{}, &RBDPVCBackupList{})
}
