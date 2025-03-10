// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.1
// 	protoc        v5.29.3
// source: pkg/controller/proto/controller.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// CreateOrUpdatePVCRequest is a request message for CreateOrUpdatePVC RPC.
type CreateOrUpdatePVCRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Pvc []byte `protobuf:"bytes,1,opt,name=pvc,proto3" json:"pvc,omitempty"`
}

func (x *CreateOrUpdatePVCRequest) Reset() {
	*x = CreateOrUpdatePVCRequest{}
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CreateOrUpdatePVCRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateOrUpdatePVCRequest) ProtoMessage() {}

func (x *CreateOrUpdatePVCRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateOrUpdatePVCRequest.ProtoReflect.Descriptor instead.
func (*CreateOrUpdatePVCRequest) Descriptor() ([]byte, []int) {
	return file_pkg_controller_proto_controller_proto_rawDescGZIP(), []int{0}
}

func (x *CreateOrUpdatePVCRequest) GetPvc() []byte {
	if x != nil {
		return x.Pvc
	}
	return nil
}

// CreateOrUpdatePVCResponse is a response message for CreateOrUpdatePVC RPC.
type CreateOrUpdatePVCResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Uid string `protobuf:"bytes,1,opt,name=uid,proto3" json:"uid,omitempty"`
}

func (x *CreateOrUpdatePVCResponse) Reset() {
	*x = CreateOrUpdatePVCResponse{}
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CreateOrUpdatePVCResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateOrUpdatePVCResponse) ProtoMessage() {}

func (x *CreateOrUpdatePVCResponse) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateOrUpdatePVCResponse.ProtoReflect.Descriptor instead.
func (*CreateOrUpdatePVCResponse) Descriptor() ([]byte, []int) {
	return file_pkg_controller_proto_controller_proto_rawDescGZIP(), []int{1}
}

func (x *CreateOrUpdatePVCResponse) GetUid() string {
	if x != nil {
		return x.Uid
	}
	return ""
}

// CreateOrUpdateMantleBackupRequest is a request message for CreateOrUpdateMantleBackup RPC.
type CreateOrUpdateMantleBackupRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MantleBackup []byte `protobuf:"bytes,1,opt,name=mantleBackup,proto3" json:"mantleBackup,omitempty"`
}

func (x *CreateOrUpdateMantleBackupRequest) Reset() {
	*x = CreateOrUpdateMantleBackupRequest{}
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CreateOrUpdateMantleBackupRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateOrUpdateMantleBackupRequest) ProtoMessage() {}

func (x *CreateOrUpdateMantleBackupRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateOrUpdateMantleBackupRequest.ProtoReflect.Descriptor instead.
func (*CreateOrUpdateMantleBackupRequest) Descriptor() ([]byte, []int) {
	return file_pkg_controller_proto_controller_proto_rawDescGZIP(), []int{2}
}

func (x *CreateOrUpdateMantleBackupRequest) GetMantleBackup() []byte {
	if x != nil {
		return x.MantleBackup
	}
	return nil
}

// CreateOrUpdateMantleBackupResponse is a response message for CreateOrUpdateMantleBackup RPC.
type CreateOrUpdateMantleBackupResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *CreateOrUpdateMantleBackupResponse) Reset() {
	*x = CreateOrUpdateMantleBackupResponse{}
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CreateOrUpdateMantleBackupResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateOrUpdateMantleBackupResponse) ProtoMessage() {}

func (x *CreateOrUpdateMantleBackupResponse) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateOrUpdateMantleBackupResponse.ProtoReflect.Descriptor instead.
func (*CreateOrUpdateMantleBackupResponse) Descriptor() ([]byte, []int) {
	return file_pkg_controller_proto_controller_proto_rawDescGZIP(), []int{3}
}

// ListMantleBackupRequest is a request message for ListMantleBackup RPC.
type ListMantleBackupRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	PvcUID    string `protobuf:"bytes,1,opt,name=pvcUID,proto3" json:"pvcUID,omitempty"`
	Namespace string `protobuf:"bytes,2,opt,name=namespace,proto3" json:"namespace,omitempty"`
}

func (x *ListMantleBackupRequest) Reset() {
	*x = ListMantleBackupRequest{}
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListMantleBackupRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListMantleBackupRequest) ProtoMessage() {}

func (x *ListMantleBackupRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListMantleBackupRequest.ProtoReflect.Descriptor instead.
func (*ListMantleBackupRequest) Descriptor() ([]byte, []int) {
	return file_pkg_controller_proto_controller_proto_rawDescGZIP(), []int{4}
}

func (x *ListMantleBackupRequest) GetPvcUID() string {
	if x != nil {
		return x.PvcUID
	}
	return ""
}

func (x *ListMantleBackupRequest) GetNamespace() string {
	if x != nil {
		return x.Namespace
	}
	return ""
}

// ListMantleBackupResponse is a response message for ListMantleBackup RPC.
type ListMantleBackupResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MantleBackupList []byte `protobuf:"bytes,1,opt,name=mantleBackupList,proto3" json:"mantleBackupList,omitempty"`
}

func (x *ListMantleBackupResponse) Reset() {
	*x = ListMantleBackupResponse{}
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListMantleBackupResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListMantleBackupResponse) ProtoMessage() {}

func (x *ListMantleBackupResponse) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListMantleBackupResponse.ProtoReflect.Descriptor instead.
func (*ListMantleBackupResponse) Descriptor() ([]byte, []int) {
	return file_pkg_controller_proto_controller_proto_rawDescGZIP(), []int{5}
}

func (x *ListMantleBackupResponse) GetMantleBackupList() []byte {
	if x != nil {
		return x.MantleBackupList
	}
	return nil
}

// SetSynchronizingRequest is a request message for SetSynchronize RPC.
type SetSynchronizingRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name      string  `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Namespace string  `protobuf:"bytes,2,opt,name=namespace,proto3" json:"namespace,omitempty"`
	DiffFrom  *string `protobuf:"bytes,3,opt,name=diffFrom,proto3,oneof" json:"diffFrom,omitempty"`
}

func (x *SetSynchronizingRequest) Reset() {
	*x = SetSynchronizingRequest{}
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SetSynchronizingRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SetSynchronizingRequest) ProtoMessage() {}

func (x *SetSynchronizingRequest) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SetSynchronizingRequest.ProtoReflect.Descriptor instead.
func (*SetSynchronizingRequest) Descriptor() ([]byte, []int) {
	return file_pkg_controller_proto_controller_proto_rawDescGZIP(), []int{6}
}

func (x *SetSynchronizingRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *SetSynchronizingRequest) GetNamespace() string {
	if x != nil {
		return x.Namespace
	}
	return ""
}

func (x *SetSynchronizingRequest) GetDiffFrom() string {
	if x != nil && x.DiffFrom != nil {
		return *x.DiffFrom
	}
	return ""
}

// SetSynchronizingResponse is a response message for SetSynchronize RPC.
type SetSynchronizingResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *SetSynchronizingResponse) Reset() {
	*x = SetSynchronizingResponse{}
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SetSynchronizingResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SetSynchronizingResponse) ProtoMessage() {}

func (x *SetSynchronizingResponse) ProtoReflect() protoreflect.Message {
	mi := &file_pkg_controller_proto_controller_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SetSynchronizingResponse.ProtoReflect.Descriptor instead.
func (*SetSynchronizingResponse) Descriptor() ([]byte, []int) {
	return file_pkg_controller_proto_controller_proto_rawDescGZIP(), []int{7}
}

var File_pkg_controller_proto_controller_proto protoreflect.FileDescriptor

var file_pkg_controller_proto_controller_proto_rawDesc = []byte{
	0x0a, 0x25, 0x70, 0x6b, 0x67, 0x2f, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65,
	0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x05, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x2c,
	0x0a, 0x18, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x4f, 0x72, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65,
	0x50, 0x56, 0x43, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x70, 0x76,
	0x63, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03, 0x70, 0x76, 0x63, 0x22, 0x2d, 0x0a, 0x19,
	0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x4f, 0x72, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x50, 0x56,
	0x43, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x69, 0x64,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x75, 0x69, 0x64, 0x22, 0x47, 0x0a, 0x21, 0x43,
	0x72, 0x65, 0x61, 0x74, 0x65, 0x4f, 0x72, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x4d, 0x61, 0x6e,
	0x74, 0x6c, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x22, 0x0a, 0x0c, 0x6d, 0x61, 0x6e, 0x74, 0x6c, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x0c, 0x6d, 0x61, 0x6e, 0x74, 0x6c, 0x65, 0x42, 0x61,
	0x63, 0x6b, 0x75, 0x70, 0x22, 0x24, 0x0a, 0x22, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x4f, 0x72,
	0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x4d, 0x61, 0x6e, 0x74, 0x6c, 0x65, 0x42, 0x61, 0x63, 0x6b,
	0x75, 0x70, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x4f, 0x0a, 0x17, 0x4c, 0x69,
	0x73, 0x74, 0x4d, 0x61, 0x6e, 0x74, 0x6c, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x16, 0x0a, 0x06, 0x70, 0x76, 0x63, 0x55, 0x49, 0x44, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x70, 0x76, 0x63, 0x55, 0x49, 0x44, 0x12, 0x1c, 0x0a,
	0x09, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x09, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x22, 0x46, 0x0a, 0x18, 0x4c,
	0x69, 0x73, 0x74, 0x4d, 0x61, 0x6e, 0x74, 0x6c, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2a, 0x0a, 0x10, 0x6d, 0x61, 0x6e, 0x74, 0x6c,
	0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x4c, 0x69, 0x73, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0c, 0x52, 0x10, 0x6d, 0x61, 0x6e, 0x74, 0x6c, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x4c,
	0x69, 0x73, 0x74, 0x22, 0x79, 0x0a, 0x17, 0x53, 0x65, 0x74, 0x53, 0x79, 0x6e, 0x63, 0x68, 0x72,
	0x6f, 0x6e, 0x69, 0x7a, 0x69, 0x6e, 0x67, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x12,
	0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61,
	0x6d, 0x65, 0x12, 0x1c, 0x0a, 0x09, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65,
	0x12, 0x1f, 0x0a, 0x08, 0x64, 0x69, 0x66, 0x66, 0x46, 0x72, 0x6f, 0x6d, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x09, 0x48, 0x00, 0x52, 0x08, 0x64, 0x69, 0x66, 0x66, 0x46, 0x72, 0x6f, 0x6d, 0x88, 0x01,
	0x01, 0x42, 0x0b, 0x0a, 0x09, 0x5f, 0x64, 0x69, 0x66, 0x66, 0x46, 0x72, 0x6f, 0x6d, 0x22, 0x1a,
	0x0a, 0x18, 0x53, 0x65, 0x74, 0x53, 0x79, 0x6e, 0x63, 0x68, 0x72, 0x6f, 0x6e, 0x69, 0x7a, 0x69,
	0x6e, 0x67, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x32, 0x84, 0x03, 0x0a, 0x0d, 0x4d,
	0x61, 0x6e, 0x74, 0x6c, 0x65, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x56, 0x0a, 0x11,
	0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x4f, 0x72, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x50, 0x56,
	0x43, 0x12, 0x1f, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65,
	0x4f, 0x72, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x50, 0x56, 0x43, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x20, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x43, 0x72, 0x65, 0x61, 0x74,
	0x65, 0x4f, 0x72, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x50, 0x56, 0x43, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x12, 0x71, 0x0a, 0x1a, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x4f, 0x72,
	0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x4d, 0x61, 0x6e, 0x74, 0x6c, 0x65, 0x42, 0x61, 0x63, 0x6b,
	0x75, 0x70, 0x12, 0x28, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x43, 0x72, 0x65, 0x61, 0x74,
	0x65, 0x4f, 0x72, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x4d, 0x61, 0x6e, 0x74, 0x6c, 0x65, 0x42,
	0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x29, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x4f, 0x72, 0x55, 0x70, 0x64,
	0x61, 0x74, 0x65, 0x4d, 0x61, 0x6e, 0x74, 0x6c, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x53, 0x0a, 0x10, 0x4c, 0x69, 0x73, 0x74, 0x4d,
	0x61, 0x6e, 0x74, 0x6c, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x12, 0x1e, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x4d, 0x61, 0x6e, 0x74, 0x6c, 0x65, 0x42, 0x61,
	0x63, 0x6b, 0x75, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1f, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x4d, 0x61, 0x6e, 0x74, 0x6c, 0x65, 0x42, 0x61,
	0x63, 0x6b, 0x75, 0x70, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x53, 0x0a, 0x10,
	0x53, 0x65, 0x74, 0x53, 0x79, 0x6e, 0x63, 0x68, 0x72, 0x6f, 0x6e, 0x69, 0x7a, 0x69, 0x6e, 0x67,
	0x12, 0x1e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x53, 0x65, 0x74, 0x53, 0x79, 0x6e, 0x63,
	0x68, 0x72, 0x6f, 0x6e, 0x69, 0x7a, 0x69, 0x6e, 0x67, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x1a, 0x1f, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x53, 0x65, 0x74, 0x53, 0x79, 0x6e, 0x63,
	0x68, 0x72, 0x6f, 0x6e, 0x69, 0x7a, 0x69, 0x6e, 0x67, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x42, 0x32, 0x5a, 0x30, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x63, 0x79, 0x62, 0x6f, 0x7a, 0x75, 0x2d, 0x67, 0x6f, 0x2f, 0x6d, 0x61, 0x6e, 0x74, 0x6c, 0x65,
	0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_pkg_controller_proto_controller_proto_rawDescOnce sync.Once
	file_pkg_controller_proto_controller_proto_rawDescData = file_pkg_controller_proto_controller_proto_rawDesc
)

func file_pkg_controller_proto_controller_proto_rawDescGZIP() []byte {
	file_pkg_controller_proto_controller_proto_rawDescOnce.Do(func() {
		file_pkg_controller_proto_controller_proto_rawDescData = protoimpl.X.CompressGZIP(file_pkg_controller_proto_controller_proto_rawDescData)
	})
	return file_pkg_controller_proto_controller_proto_rawDescData
}

var file_pkg_controller_proto_controller_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_pkg_controller_proto_controller_proto_goTypes = []any{
	(*CreateOrUpdatePVCRequest)(nil),           // 0: proto.CreateOrUpdatePVCRequest
	(*CreateOrUpdatePVCResponse)(nil),          // 1: proto.CreateOrUpdatePVCResponse
	(*CreateOrUpdateMantleBackupRequest)(nil),  // 2: proto.CreateOrUpdateMantleBackupRequest
	(*CreateOrUpdateMantleBackupResponse)(nil), // 3: proto.CreateOrUpdateMantleBackupResponse
	(*ListMantleBackupRequest)(nil),            // 4: proto.ListMantleBackupRequest
	(*ListMantleBackupResponse)(nil),           // 5: proto.ListMantleBackupResponse
	(*SetSynchronizingRequest)(nil),            // 6: proto.SetSynchronizingRequest
	(*SetSynchronizingResponse)(nil),           // 7: proto.SetSynchronizingResponse
}
var file_pkg_controller_proto_controller_proto_depIdxs = []int32{
	0, // 0: proto.MantleService.CreateOrUpdatePVC:input_type -> proto.CreateOrUpdatePVCRequest
	2, // 1: proto.MantleService.CreateOrUpdateMantleBackup:input_type -> proto.CreateOrUpdateMantleBackupRequest
	4, // 2: proto.MantleService.ListMantleBackup:input_type -> proto.ListMantleBackupRequest
	6, // 3: proto.MantleService.SetSynchronizing:input_type -> proto.SetSynchronizingRequest
	1, // 4: proto.MantleService.CreateOrUpdatePVC:output_type -> proto.CreateOrUpdatePVCResponse
	3, // 5: proto.MantleService.CreateOrUpdateMantleBackup:output_type -> proto.CreateOrUpdateMantleBackupResponse
	5, // 6: proto.MantleService.ListMantleBackup:output_type -> proto.ListMantleBackupResponse
	7, // 7: proto.MantleService.SetSynchronizing:output_type -> proto.SetSynchronizingResponse
	4, // [4:8] is the sub-list for method output_type
	0, // [0:4] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_pkg_controller_proto_controller_proto_init() }
func file_pkg_controller_proto_controller_proto_init() {
	if File_pkg_controller_proto_controller_proto != nil {
		return
	}
	file_pkg_controller_proto_controller_proto_msgTypes[6].OneofWrappers = []any{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_pkg_controller_proto_controller_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_pkg_controller_proto_controller_proto_goTypes,
		DependencyIndexes: file_pkg_controller_proto_controller_proto_depIdxs,
		MessageInfos:      file_pkg_controller_proto_controller_proto_msgTypes,
	}.Build()
	File_pkg_controller_proto_controller_proto = out.File
	file_pkg_controller_proto_controller_proto_rawDesc = nil
	file_pkg_controller_proto_controller_proto_goTypes = nil
	file_pkg_controller_proto_controller_proto_depIdxs = nil
}
