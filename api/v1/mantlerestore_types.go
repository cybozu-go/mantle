package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// MantleRestoreSpec defines the desired state of MantleRestore
type MantleRestoreSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// 'backup' specifies restore target backup resource name
	// +kubebuilder:validation:Required
	Backup string `json:"backup,omitempty"`
}

// MantleRestoreStatus defines the observed state of MantleRestore
type MantleRestoreStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// 'conditions' specifies current restore conditions
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// 'pool' specifies pool name the volumes are restored to
	Pool string `json:"pool,omitempty"`
}

const (
	RestoreConditionReadyToUse = "ReadyToUse"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// MantleRestore is the Schema for the mantlerestores API
type MantleRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MantleRestoreSpec   `json:"spec,omitempty"`
	Status MantleRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MantleRestoreList contains a list of MantleRestore
type MantleRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MantleRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MantleRestore{}, &MantleRestoreList{})
}
