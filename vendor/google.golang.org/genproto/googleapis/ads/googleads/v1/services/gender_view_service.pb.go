// Code generated by protoc-gen-go. DO NOT EDIT.
// source: google/ads/googleads/v1/services/gender_view_service.proto

package services // import "google.golang.org/genproto/googleapis/ads/googleads/v1/services"

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import resources "google.golang.org/genproto/googleapis/ads/googleads/v1/resources"
import _ "google.golang.org/genproto/googleapis/api/annotations"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// Request message for [GenderViewService.GetGenderView][google.ads.googleads.v1.services.GenderViewService.GetGenderView].
type GetGenderViewRequest struct {
	// The resource name of the gender view to fetch.
	ResourceName         string   `protobuf:"bytes,1,opt,name=resource_name,json=resourceName,proto3" json:"resource_name,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *GetGenderViewRequest) Reset()         { *m = GetGenderViewRequest{} }
func (m *GetGenderViewRequest) String() string { return proto.CompactTextString(m) }
func (*GetGenderViewRequest) ProtoMessage()    {}
func (*GetGenderViewRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_gender_view_service_cdbdad6124719293, []int{0}
}
func (m *GetGenderViewRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetGenderViewRequest.Unmarshal(m, b)
}
func (m *GetGenderViewRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetGenderViewRequest.Marshal(b, m, deterministic)
}
func (dst *GetGenderViewRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetGenderViewRequest.Merge(dst, src)
}
func (m *GetGenderViewRequest) XXX_Size() int {
	return xxx_messageInfo_GetGenderViewRequest.Size(m)
}
func (m *GetGenderViewRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_GetGenderViewRequest.DiscardUnknown(m)
}

var xxx_messageInfo_GetGenderViewRequest proto.InternalMessageInfo

func (m *GetGenderViewRequest) GetResourceName() string {
	if m != nil {
		return m.ResourceName
	}
	return ""
}

func init() {
	proto.RegisterType((*GetGenderViewRequest)(nil), "google.ads.googleads.v1.services.GetGenderViewRequest")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// GenderViewServiceClient is the client API for GenderViewService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type GenderViewServiceClient interface {
	// Returns the requested gender view in full detail.
	GetGenderView(ctx context.Context, in *GetGenderViewRequest, opts ...grpc.CallOption) (*resources.GenderView, error)
}

type genderViewServiceClient struct {
	cc *grpc.ClientConn
}

func NewGenderViewServiceClient(cc *grpc.ClientConn) GenderViewServiceClient {
	return &genderViewServiceClient{cc}
}

func (c *genderViewServiceClient) GetGenderView(ctx context.Context, in *GetGenderViewRequest, opts ...grpc.CallOption) (*resources.GenderView, error) {
	out := new(resources.GenderView)
	err := c.cc.Invoke(ctx, "/google.ads.googleads.v1.services.GenderViewService/GetGenderView", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GenderViewServiceServer is the server API for GenderViewService service.
type GenderViewServiceServer interface {
	// Returns the requested gender view in full detail.
	GetGenderView(context.Context, *GetGenderViewRequest) (*resources.GenderView, error)
}

func RegisterGenderViewServiceServer(s *grpc.Server, srv GenderViewServiceServer) {
	s.RegisterService(&_GenderViewService_serviceDesc, srv)
}

func _GenderViewService_GetGenderView_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetGenderViewRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GenderViewServiceServer).GetGenderView(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/google.ads.googleads.v1.services.GenderViewService/GetGenderView",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GenderViewServiceServer).GetGenderView(ctx, req.(*GetGenderViewRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _GenderViewService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "google.ads.googleads.v1.services.GenderViewService",
	HandlerType: (*GenderViewServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetGenderView",
			Handler:    _GenderViewService_GetGenderView_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "google/ads/googleads/v1/services/gender_view_service.proto",
}

func init() {
	proto.RegisterFile("google/ads/googleads/v1/services/gender_view_service.proto", fileDescriptor_gender_view_service_cdbdad6124719293)
}

var fileDescriptor_gender_view_service_cdbdad6124719293 = []byte{
	// 361 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x84, 0x92, 0xb1, 0x4a, 0xc3, 0x50,
	0x14, 0x86, 0x49, 0x04, 0xc1, 0x60, 0x07, 0x83, 0x88, 0x14, 0x87, 0x52, 0x3b, 0x48, 0xa1, 0xf7,
	0x12, 0x8b, 0x0e, 0xb7, 0x38, 0xa4, 0x4b, 0x9c, 0xa4, 0x54, 0xc8, 0x20, 0x81, 0x72, 0x6d, 0x0e,
	0x21, 0xd0, 0xe4, 0xd6, 0x7b, 0xd2, 0x74, 0x10, 0x17, 0x5f, 0xc1, 0x37, 0x70, 0x14, 0x7c, 0x0a,
	0x37, 0x57, 0x5f, 0xc1, 0xc9, 0x77, 0x10, 0x24, 0xbd, 0xb9, 0x29, 0x55, 0x4b, 0xb7, 0x9f, 0xd3,
	0xff, 0xfb, 0xef, 0x7f, 0x4e, 0x63, 0xb1, 0x48, 0x88, 0x68, 0x02, 0x94, 0x87, 0x48, 0x95, 0x2c,
	0x54, 0xee, 0x50, 0x04, 0x99, 0xc7, 0x63, 0x40, 0x1a, 0x41, 0x1a, 0x82, 0x1c, 0xe5, 0x31, 0xcc,
	0x47, 0xe5, 0x90, 0x4c, 0xa5, 0xc8, 0x84, 0xdd, 0x50, 0x00, 0xe1, 0x21, 0x92, 0x8a, 0x25, 0xb9,
	0x43, 0x34, 0x5b, 0xef, 0xae, 0x4b, 0x97, 0x80, 0x62, 0x26, 0x7f, 0xc5, 0xab, 0xd8, 0xfa, 0x91,
	0x86, 0xa6, 0x31, 0xe5, 0x69, 0x2a, 0x32, 0x9e, 0xc5, 0x22, 0x45, 0xf5, 0x6b, 0xb3, 0x67, 0xed,
	0x7b, 0x90, 0x79, 0x0b, 0xca, 0x8f, 0x61, 0x3e, 0x84, 0xbb, 0x19, 0x60, 0x66, 0x1f, 0x5b, 0x35,
	0x1d, 0x3a, 0x4a, 0x79, 0x02, 0x87, 0x46, 0xc3, 0x38, 0xd9, 0x19, 0xee, 0xea, 0xe1, 0x15, 0x4f,
	0xe0, 0xf4, 0xcd, 0xb0, 0xf6, 0x96, 0xe8, 0xb5, 0xaa, 0x69, 0xbf, 0x1a, 0x56, 0x6d, 0x25, 0xd3,
	0x3e, 0x27, 0x9b, 0x56, 0x23, 0xff, 0x95, 0xa8, 0x77, 0xd6, 0x72, 0xd5, 0xc2, 0x64, 0x49, 0x35,
	0xcf, 0x1e, 0x3f, 0x3e, 0x9f, 0x4c, 0x6a, 0x77, 0x8a, 0x93, 0xdc, 0xaf, 0xd4, 0xbf, 0x18, 0xcf,
	0x30, 0x13, 0x09, 0x48, 0xa4, 0xed, 0xf2, 0x46, 0x05, 0x82, 0xb4, 0xfd, 0xd0, 0xff, 0x36, 0xac,
	0xd6, 0x58, 0x24, 0x1b, 0x3b, 0xf6, 0x0f, 0xfe, 0xec, 0x3a, 0x28, 0x6e, 0x38, 0x30, 0x6e, 0x2e,
	0x4b, 0x36, 0x12, 0x13, 0x9e, 0x46, 0x44, 0xc8, 0xa8, 0x78, 0x64, 0x71, 0x61, 0xfd, 0x47, 0x4d,
	0x63, 0x5c, 0xff, 0x55, 0xf4, 0xb4, 0x78, 0x36, 0xb7, 0x3c, 0xd7, 0x7d, 0x31, 0x1b, 0x9e, 0x0a,
	0x74, 0x43, 0x24, 0x4a, 0x16, 0xca, 0x77, 0x48, 0xf9, 0x30, 0xbe, 0x6b, 0x4b, 0xe0, 0x86, 0x18,
	0x54, 0x96, 0xc0, 0x77, 0x02, 0x6d, 0xf9, 0x32, 0x5b, 0x6a, 0xce, 0x98, 0x1b, 0x22, 0x63, 0x95,
	0x89, 0x31, 0xdf, 0x61, 0x4c, 0xdb, 0x6e, 0xb7, 0x17, 0x3d, 0xbb, 0x3f, 0x01, 0x00, 0x00, 0xff,
	0xff, 0x04, 0x5c, 0x26, 0x09, 0xbc, 0x02, 0x00, 0x00,
}
