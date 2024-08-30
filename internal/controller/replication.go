package controller

import (
	"context"

	"github.com/cybozu-go/mantle/pkg/controller/proto"
	"google.golang.org/grpc"
)

const (
	RoleStandalone = "standalone"
	RolePrimary    = "primary"
	RoleSecondary  = "secondary"
)

type PrimarySettings struct {
	ServiceEndpoint string
	Conn            *grpc.ClientConn
	Client          proto.MantleServiceClient
}

type SecondaryServer struct {
	proto.UnimplementedMantleServiceServer
}

var _ proto.MantleServiceServer = &SecondaryServer{}

func (s *SecondaryServer) CreateOrUpdatePVC(
	ctx context.Context,
	req *proto.CreateOrUpdatePVCRequest,
) (*proto.CreateOrUpdatePVCResponse, error) {
	return &proto.CreateOrUpdatePVCResponse{Uid: ""}, nil
}

func (s *SecondaryServer) CreateOrUpdateMantleBackup(
	ctx context.Context,
	req *proto.CreateOrUpdateMantleBackupRequest,
) (*proto.CreateOrUpdateMantleBackupResponse, error) {
	return &proto.CreateOrUpdateMantleBackupResponse{}, nil
}
