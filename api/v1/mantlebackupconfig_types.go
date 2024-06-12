package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// MantleBackupConfigSpec defines the desired state of MantleBackupConfig
type MantleBackupConfigSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Foo is an example field of MantleBackupConfig. Edit mantlebackupconfig_types.go to remove/update
	Foo string `json:"foo,omitempty"`
}

// MantleBackupConfigStatus defines the observed state of MantleBackupConfig
type MantleBackupConfigStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// MantleBackupConfig is the Schema for the mantlebackupconfigs API
type MantleBackupConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MantleBackupConfigSpec   `json:"spec,omitempty"`
	Status MantleBackupConfigStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// MantleBackupConfigList contains a list of MantleBackupConfig
type MantleBackupConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MantleBackupConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MantleBackupConfig{}, &MantleBackupConfigList{})
}
