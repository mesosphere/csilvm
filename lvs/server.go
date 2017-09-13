package lvs

import (
	"golang.org/x/net/context"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

type Server struct {
}

func NewServer() *Server {
	return new(Server)
}

func (s *Server) GetSupportedVersions(
	ctx context.Context,
	request *csi.GetSupportedVersionsRequest) (*csi.GetSupportedVersionsResponse, error) {
	response := &csi.GetSupportedVersionsResponse{
		&csi.GetSupportedVersionsResponse_Result_{
			&csi.GetSupportedVersionsResponse_Result{
				[]*csi.Version{
					&csi.Version{0, 1, 0},
				},
			},
		},
	}
	return response, nil
}

func (s *Server) GetPluginInfo(
	ctx context.Context,
	request *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{}, nil
}
