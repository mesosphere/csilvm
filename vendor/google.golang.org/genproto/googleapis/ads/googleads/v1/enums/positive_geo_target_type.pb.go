// Code generated by protoc-gen-go. DO NOT EDIT.
// source: google/ads/googleads/v1/enums/positive_geo_target_type.proto

package enums // import "google.golang.org/genproto/googleapis/ads/googleads/v1/enums"

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import _ "google.golang.org/genproto/googleapis/api/annotations"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// The possible positive geo target types.
type PositiveGeoTargetTypeEnum_PositiveGeoTargetType int32

const (
	// Not specified.
	PositiveGeoTargetTypeEnum_UNSPECIFIED PositiveGeoTargetTypeEnum_PositiveGeoTargetType = 0
	// The value is unknown in this version.
	PositiveGeoTargetTypeEnum_UNKNOWN PositiveGeoTargetTypeEnum_PositiveGeoTargetType = 1
	// Specifies that an ad is triggered if the user is in,
	// or shows interest in, advertiser's targeted locations.
	PositiveGeoTargetTypeEnum_DONT_CARE PositiveGeoTargetTypeEnum_PositiveGeoTargetType = 2
	// Specifies that an ad is triggered if the user
	// searches for advertiser's targeted locations.
	PositiveGeoTargetTypeEnum_AREA_OF_INTEREST PositiveGeoTargetTypeEnum_PositiveGeoTargetType = 3
	// Specifies that an ad is triggered if the user is in
	// or regularly in advertiser's targeted locations.
	PositiveGeoTargetTypeEnum_LOCATION_OF_PRESENCE PositiveGeoTargetTypeEnum_PositiveGeoTargetType = 4
)

var PositiveGeoTargetTypeEnum_PositiveGeoTargetType_name = map[int32]string{
	0: "UNSPECIFIED",
	1: "UNKNOWN",
	2: "DONT_CARE",
	3: "AREA_OF_INTEREST",
	4: "LOCATION_OF_PRESENCE",
}
var PositiveGeoTargetTypeEnum_PositiveGeoTargetType_value = map[string]int32{
	"UNSPECIFIED":          0,
	"UNKNOWN":              1,
	"DONT_CARE":            2,
	"AREA_OF_INTEREST":     3,
	"LOCATION_OF_PRESENCE": 4,
}

func (x PositiveGeoTargetTypeEnum_PositiveGeoTargetType) String() string {
	return proto.EnumName(PositiveGeoTargetTypeEnum_PositiveGeoTargetType_name, int32(x))
}
func (PositiveGeoTargetTypeEnum_PositiveGeoTargetType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_positive_geo_target_type_8a4eb110d34593ec, []int{0, 0}
}

// Container for enum describing possible positive geo target types.
type PositiveGeoTargetTypeEnum struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PositiveGeoTargetTypeEnum) Reset()         { *m = PositiveGeoTargetTypeEnum{} }
func (m *PositiveGeoTargetTypeEnum) String() string { return proto.CompactTextString(m) }
func (*PositiveGeoTargetTypeEnum) ProtoMessage()    {}
func (*PositiveGeoTargetTypeEnum) Descriptor() ([]byte, []int) {
	return fileDescriptor_positive_geo_target_type_8a4eb110d34593ec, []int{0}
}
func (m *PositiveGeoTargetTypeEnum) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_PositiveGeoTargetTypeEnum.Unmarshal(m, b)
}
func (m *PositiveGeoTargetTypeEnum) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_PositiveGeoTargetTypeEnum.Marshal(b, m, deterministic)
}
func (dst *PositiveGeoTargetTypeEnum) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PositiveGeoTargetTypeEnum.Merge(dst, src)
}
func (m *PositiveGeoTargetTypeEnum) XXX_Size() int {
	return xxx_messageInfo_PositiveGeoTargetTypeEnum.Size(m)
}
func (m *PositiveGeoTargetTypeEnum) XXX_DiscardUnknown() {
	xxx_messageInfo_PositiveGeoTargetTypeEnum.DiscardUnknown(m)
}

var xxx_messageInfo_PositiveGeoTargetTypeEnum proto.InternalMessageInfo

func init() {
	proto.RegisterType((*PositiveGeoTargetTypeEnum)(nil), "google.ads.googleads.v1.enums.PositiveGeoTargetTypeEnum")
	proto.RegisterEnum("google.ads.googleads.v1.enums.PositiveGeoTargetTypeEnum_PositiveGeoTargetType", PositiveGeoTargetTypeEnum_PositiveGeoTargetType_name, PositiveGeoTargetTypeEnum_PositiveGeoTargetType_value)
}

func init() {
	proto.RegisterFile("google/ads/googleads/v1/enums/positive_geo_target_type.proto", fileDescriptor_positive_geo_target_type_8a4eb110d34593ec)
}

var fileDescriptor_positive_geo_target_type_8a4eb110d34593ec = []byte{
	// 348 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x7c, 0x90, 0xd1, 0x4a, 0xfb, 0x30,
	0x18, 0xc5, 0xff, 0xed, 0xfe, 0x28, 0x66, 0x88, 0xa5, 0x4c, 0xd0, 0xe1, 0x2e, 0xb6, 0x07, 0x48,
	0x29, 0xde, 0x45, 0x6f, 0xb2, 0x2e, 0x1b, 0x45, 0x49, 0x4b, 0xd7, 0x4d, 0x90, 0x42, 0xa9, 0x36,
	0x84, 0xc2, 0xd6, 0x94, 0x25, 0x1b, 0xec, 0x31, 0x7c, 0x05, 0x2f, 0x7d, 0x14, 0x1f, 0xc5, 0x1b,
	0x5f, 0x41, 0x9a, 0x6e, 0xbb, 0x9a, 0xde, 0x84, 0x43, 0xce, 0xf7, 0x3b, 0x7c, 0xdf, 0x01, 0xf7,
	0x5c, 0x08, 0xbe, 0x60, 0x4e, 0x96, 0x4b, 0xa7, 0x91, 0xb5, 0xda, 0xb8, 0x0e, 0x2b, 0xd7, 0x4b,
	0xe9, 0x54, 0x42, 0x16, 0xaa, 0xd8, 0xb0, 0x94, 0x33, 0x91, 0xaa, 0x6c, 0xc5, 0x99, 0x4a, 0xd5,
	0xb6, 0x62, 0xb0, 0x5a, 0x09, 0x25, 0xec, 0x5e, 0x83, 0xc0, 0x2c, 0x97, 0xf0, 0x40, 0xc3, 0x8d,
	0x0b, 0x35, 0xdd, 0xbd, 0xd9, 0x87, 0x57, 0x85, 0x93, 0x95, 0xa5, 0x50, 0x99, 0x2a, 0x44, 0x29,
	0x1b, 0x78, 0xf0, 0x66, 0x80, 0xeb, 0x70, 0x97, 0x3f, 0x61, 0x22, 0xd6, 0xe9, 0xf1, 0xb6, 0x62,
	0xa4, 0x5c, 0x2f, 0x07, 0x0a, 0x5c, 0x1e, 0x35, 0xed, 0x0b, 0xd0, 0x9e, 0xd1, 0x69, 0x48, 0x3c,
	0x7f, 0xec, 0x93, 0x91, 0xf5, 0xcf, 0x6e, 0x83, 0xd3, 0x19, 0x7d, 0xa0, 0xc1, 0x13, 0xb5, 0x0c,
	0xfb, 0x1c, 0x9c, 0x8d, 0x02, 0x1a, 0xa7, 0x1e, 0x8e, 0x88, 0x65, 0xda, 0x1d, 0x60, 0xe1, 0x88,
	0xe0, 0x34, 0x18, 0xa7, 0x3e, 0x8d, 0x49, 0x44, 0xa6, 0xb1, 0xd5, 0xb2, 0xaf, 0x40, 0xe7, 0x31,
	0xf0, 0x70, 0xec, 0x07, 0xb4, 0x76, 0xc2, 0x88, 0x4c, 0x09, 0xf5, 0x88, 0xf5, 0x7f, 0xf8, 0x6d,
	0x80, 0xfe, 0xab, 0x58, 0xc2, 0x3f, 0xef, 0x1a, 0x76, 0x8f, 0x6e, 0x16, 0xd6, 0x57, 0x85, 0xc6,
	0xf3, 0x70, 0x07, 0x73, 0xb1, 0xc8, 0x4a, 0x0e, 0xc5, 0x8a, 0x3b, 0x9c, 0x95, 0xfa, 0xe6, 0x7d,
	0xc5, 0x55, 0x21, 0x7f, 0x69, 0xfc, 0x4e, 0xbf, 0xef, 0x66, 0x6b, 0x82, 0xf1, 0x87, 0xd9, 0x9b,
	0x34, 0x51, 0x38, 0x97, 0xb0, 0x91, 0xb5, 0x9a, 0xbb, 0xb0, 0xae, 0x48, 0x7e, 0xee, 0xfd, 0x04,
	0xe7, 0x32, 0x39, 0xf8, 0xc9, 0xdc, 0x4d, 0xb4, 0xff, 0x65, 0xf6, 0x9b, 0x4f, 0x84, 0x70, 0x2e,
	0x11, 0x3a, 0x4c, 0x20, 0x34, 0x77, 0x11, 0xd2, 0x33, 0x2f, 0x27, 0x7a, 0xb1, 0xdb, 0x9f, 0x00,
	0x00, 0x00, 0xff, 0xff, 0x95, 0x62, 0x2c, 0xa8, 0x09, 0x02, 0x00, 0x00,
}
