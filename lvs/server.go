package lvs

import (
	"golang.org/x/net/context"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

const PluginName = "com.mesosphere/lvs"
const PluginVersion = "0.1.0"

type Server struct {
}

func (s *Server) supportedVersions() []*csi.Version {
	return []*csi.Version{
		&csi.Version{0, 1, 0},
	}
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
				s.supportedVersions(),
			},
		},
	}
	return response, nil
}

func (s *Server) GetPluginInfo(
	ctx context.Context,
	request *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	version := request.GetVersion()
	if version == nil {
		response := &csi.GetPluginInfoResponse{
			&csi.GetPluginInfoResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, ""},
					},
				},
			},
		}
		return response, nil
	}
	for _, v := range s.supportedVersions() {
		if *v == *version {
			response := &csi.GetPluginInfoResponse{
				&csi.GetPluginInfoResponse_Result_{
					&csi.GetPluginInfoResponse_Result{PluginName, PluginVersion, nil},
				},
			}
			return response, nil
		}
	}
	response := &csi.GetPluginInfoResponse{
		&csi.GetPluginInfoResponse_Error{
			&csi.Error{
				&csi.Error_GeneralError_{
					&csi.Error_GeneralError{csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION, true, ""},
				},
			},
		},
	}
	return response, nil
}
