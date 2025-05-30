//go:build !ignore_autogenerated

// Code generated by controller-gen. DO NOT EDIT.

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MantleBackup) DeepCopyInto(out *MantleBackup) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new MantleBackup.
func (in *MantleBackup) DeepCopy() *MantleBackup {
	if in == nil {
		return nil
	}
	out := new(MantleBackup)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *MantleBackup) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MantleBackupConfig) DeepCopyInto(out *MantleBackupConfig) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	out.Status = in.Status
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new MantleBackupConfig.
func (in *MantleBackupConfig) DeepCopy() *MantleBackupConfig {
	if in == nil {
		return nil
	}
	out := new(MantleBackupConfig)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *MantleBackupConfig) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MantleBackupConfigList) DeepCopyInto(out *MantleBackupConfigList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]MantleBackupConfig, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new MantleBackupConfigList.
func (in *MantleBackupConfigList) DeepCopy() *MantleBackupConfigList {
	if in == nil {
		return nil
	}
	out := new(MantleBackupConfigList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *MantleBackupConfigList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MantleBackupConfigSpec) DeepCopyInto(out *MantleBackupConfigSpec) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new MantleBackupConfigSpec.
func (in *MantleBackupConfigSpec) DeepCopy() *MantleBackupConfigSpec {
	if in == nil {
		return nil
	}
	out := new(MantleBackupConfigSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MantleBackupConfigStatus) DeepCopyInto(out *MantleBackupConfigStatus) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new MantleBackupConfigStatus.
func (in *MantleBackupConfigStatus) DeepCopy() *MantleBackupConfigStatus {
	if in == nil {
		return nil
	}
	out := new(MantleBackupConfigStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MantleBackupList) DeepCopyInto(out *MantleBackupList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]MantleBackup, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new MantleBackupList.
func (in *MantleBackupList) DeepCopy() *MantleBackupList {
	if in == nil {
		return nil
	}
	out := new(MantleBackupList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *MantleBackupList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MantleBackupSpec) DeepCopyInto(out *MantleBackupSpec) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new MantleBackupSpec.
func (in *MantleBackupSpec) DeepCopy() *MantleBackupSpec {
	if in == nil {
		return nil
	}
	out := new(MantleBackupSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MantleBackupStatus) DeepCopyInto(out *MantleBackupStatus) {
	*out = *in
	in.CreatedAt.DeepCopyInto(&out.CreatedAt)
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]metav1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.SnapID != nil {
		in, out := &in.SnapID, &out.SnapID
		*out = new(int)
		**out = **in
	}
	if in.SnapSize != nil {
		in, out := &in.SnapSize, &out.SnapSize
		*out = new(int64)
		**out = **in
	}
	if in.TransferPartSize != nil {
		in, out := &in.TransferPartSize, &out.TransferPartSize
		x := (*in).DeepCopy()
		*out = &x
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new MantleBackupStatus.
func (in *MantleBackupStatus) DeepCopy() *MantleBackupStatus {
	if in == nil {
		return nil
	}
	out := new(MantleBackupStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MantleRestore) DeepCopyInto(out *MantleRestore) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new MantleRestore.
func (in *MantleRestore) DeepCopy() *MantleRestore {
	if in == nil {
		return nil
	}
	out := new(MantleRestore)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *MantleRestore) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MantleRestoreList) DeepCopyInto(out *MantleRestoreList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]MantleRestore, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new MantleRestoreList.
func (in *MantleRestoreList) DeepCopy() *MantleRestoreList {
	if in == nil {
		return nil
	}
	out := new(MantleRestoreList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *MantleRestoreList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MantleRestoreSpec) DeepCopyInto(out *MantleRestoreSpec) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new MantleRestoreSpec.
func (in *MantleRestoreSpec) DeepCopy() *MantleRestoreSpec {
	if in == nil {
		return nil
	}
	out := new(MantleRestoreSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MantleRestoreStatus) DeepCopyInto(out *MantleRestoreStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]metav1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new MantleRestoreStatus.
func (in *MantleRestoreStatus) DeepCopy() *MantleRestoreStatus {
	if in == nil {
		return nil
	}
	out := new(MantleRestoreStatus)
	in.DeepCopyInto(out)
	return out
}
