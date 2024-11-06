// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.28.3
// source: pkg/controller/proto/controller.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	MantleService_CreateOrUpdatePVC_FullMethodName          = "/proto.MantleService/CreateOrUpdatePVC"
	MantleService_CreateOrUpdateMantleBackup_FullMethodName = "/proto.MantleService/CreateOrUpdateMantleBackup"
	MantleService_ListMantleBackup_FullMethodName           = "/proto.MantleService/ListMantleBackup"
	MantleService_SetSynchronizing_FullMethodName           = "/proto.MantleService/SetSynchronizing"
)

// MantleServiceClient is the client API for MantleService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type MantleServiceClient interface {
	CreateOrUpdatePVC(ctx context.Context, in *CreateOrUpdatePVCRequest, opts ...grpc.CallOption) (*CreateOrUpdatePVCResponse, error)
	CreateOrUpdateMantleBackup(ctx context.Context, in *CreateOrUpdateMantleBackupRequest, opts ...grpc.CallOption) (*CreateOrUpdateMantleBackupResponse, error)
	ListMantleBackup(ctx context.Context, in *ListMantleBackupRequest, opts ...grpc.CallOption) (*ListMantleBackupResponse, error)
	SetSynchronizing(ctx context.Context, in *SetSynchronizingRequest, opts ...grpc.CallOption) (*SetSynchronizingResponse, error)
}

type mantleServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewMantleServiceClient(cc grpc.ClientConnInterface) MantleServiceClient {
	return &mantleServiceClient{cc}
}

func (c *mantleServiceClient) CreateOrUpdatePVC(ctx context.Context, in *CreateOrUpdatePVCRequest, opts ...grpc.CallOption) (*CreateOrUpdatePVCResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CreateOrUpdatePVCResponse)
	err := c.cc.Invoke(ctx, MantleService_CreateOrUpdatePVC_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mantleServiceClient) CreateOrUpdateMantleBackup(ctx context.Context, in *CreateOrUpdateMantleBackupRequest, opts ...grpc.CallOption) (*CreateOrUpdateMantleBackupResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CreateOrUpdateMantleBackupResponse)
	err := c.cc.Invoke(ctx, MantleService_CreateOrUpdateMantleBackup_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mantleServiceClient) ListMantleBackup(ctx context.Context, in *ListMantleBackupRequest, opts ...grpc.CallOption) (*ListMantleBackupResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListMantleBackupResponse)
	err := c.cc.Invoke(ctx, MantleService_ListMantleBackup_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *mantleServiceClient) SetSynchronizing(ctx context.Context, in *SetSynchronizingRequest, opts ...grpc.CallOption) (*SetSynchronizingResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(SetSynchronizingResponse)
	err := c.cc.Invoke(ctx, MantleService_SetSynchronizing_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MantleServiceServer is the server API for MantleService service.
// All implementations must embed UnimplementedMantleServiceServer
// for forward compatibility.
type MantleServiceServer interface {
	CreateOrUpdatePVC(context.Context, *CreateOrUpdatePVCRequest) (*CreateOrUpdatePVCResponse, error)
	CreateOrUpdateMantleBackup(context.Context, *CreateOrUpdateMantleBackupRequest) (*CreateOrUpdateMantleBackupResponse, error)
	ListMantleBackup(context.Context, *ListMantleBackupRequest) (*ListMantleBackupResponse, error)
	SetSynchronizing(context.Context, *SetSynchronizingRequest) (*SetSynchronizingResponse, error)
	mustEmbedUnimplementedMantleServiceServer()
}

// UnimplementedMantleServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedMantleServiceServer struct{}

func (UnimplementedMantleServiceServer) CreateOrUpdatePVC(context.Context, *CreateOrUpdatePVCRequest) (*CreateOrUpdatePVCResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateOrUpdatePVC not implemented")
}
func (UnimplementedMantleServiceServer) CreateOrUpdateMantleBackup(context.Context, *CreateOrUpdateMantleBackupRequest) (*CreateOrUpdateMantleBackupResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateOrUpdateMantleBackup not implemented")
}
func (UnimplementedMantleServiceServer) ListMantleBackup(context.Context, *ListMantleBackupRequest) (*ListMantleBackupResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListMantleBackup not implemented")
}
func (UnimplementedMantleServiceServer) SetSynchronizing(context.Context, *SetSynchronizingRequest) (*SetSynchronizingResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SetSynchronizing not implemented")
}
func (UnimplementedMantleServiceServer) mustEmbedUnimplementedMantleServiceServer() {}
func (UnimplementedMantleServiceServer) testEmbeddedByValue()                       {}

// UnsafeMantleServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to MantleServiceServer will
// result in compilation errors.
type UnsafeMantleServiceServer interface {
	mustEmbedUnimplementedMantleServiceServer()
}

func RegisterMantleServiceServer(s grpc.ServiceRegistrar, srv MantleServiceServer) {
	// If the following call pancis, it indicates UnimplementedMantleServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&MantleService_ServiceDesc, srv)
}

func _MantleService_CreateOrUpdatePVC_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateOrUpdatePVCRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MantleServiceServer).CreateOrUpdatePVC(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MantleService_CreateOrUpdatePVC_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MantleServiceServer).CreateOrUpdatePVC(ctx, req.(*CreateOrUpdatePVCRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MantleService_CreateOrUpdateMantleBackup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateOrUpdateMantleBackupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MantleServiceServer).CreateOrUpdateMantleBackup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MantleService_CreateOrUpdateMantleBackup_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MantleServiceServer).CreateOrUpdateMantleBackup(ctx, req.(*CreateOrUpdateMantleBackupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MantleService_ListMantleBackup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListMantleBackupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MantleServiceServer).ListMantleBackup(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MantleService_ListMantleBackup_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MantleServiceServer).ListMantleBackup(ctx, req.(*ListMantleBackupRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _MantleService_SetSynchronizing_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetSynchronizingRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MantleServiceServer).SetSynchronizing(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: MantleService_SetSynchronizing_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(MantleServiceServer).SetSynchronizing(ctx, req.(*SetSynchronizingRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// MantleService_ServiceDesc is the grpc.ServiceDesc for MantleService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var MantleService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "proto.MantleService",
	HandlerType: (*MantleServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateOrUpdatePVC",
			Handler:    _MantleService_CreateOrUpdatePVC_Handler,
		},
		{
			MethodName: "CreateOrUpdateMantleBackup",
			Handler:    _MantleService_CreateOrUpdateMantleBackup_Handler,
		},
		{
			MethodName: "ListMantleBackup",
			Handler:    _MantleService_ListMantleBackup_Handler,
		},
		{
			MethodName: "SetSynchronizing",
			Handler:    _MantleService_SetSynchronizing_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/controller/proto/controller.proto",
}
