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

	//+kubebuilder:validation:XValidation:message="spec.pvc is immutable",rule="self == oldSelf"
	PVC string `json:"pvc"`

	//+kubebuilder:validation:Pattern:=^\s*([0-5]?[0-9])\s+(0?[0-9]|1[0-9]|2[0-3])\s+\*\s+\*\s+\*\s*$
	Schedule string `json:"schedule"`

	// NOTE: we CANNOT use metav1.Duration for Expire due to an unresolved k8s bug.
	// See https://github.com/kubernetes/apiextensions-apiserver/issues/56 for the details.

	//+kubebuilder:validation:Format:="duration"
	//+kubebuilder:validation:XValidation:message="expire must be >= 1d",rule="self >= duration('24h')"
	//+kubebuilder:validation:XValidation:message="expire must be <= 15d",rule="self <= duration('360h')"
	//+kubebuilder:validation:XValidation:message="spec.expire is immutable",rule="self == oldSelf"
	Expire string `json:"expire"`

	//+kubebuilder:default:=false
	Suspend bool `json:"suspend,omitempty"`
}

// MantleBackupConfigStatus defines the observed state of MantleBackupConfig
type MantleBackupConfigStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=mbc

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
