// Code generated by protoc-gen-go. DO NOT EDIT.
// source: google/ads/googleads/v1/enums/campaign_experiment_type.proto

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

// Indicates if this campaign is a normal campaign,
// a draft campaign, or an experiment campaign.
type CampaignExperimentTypeEnum_CampaignExperimentType int32

const (
	// Not specified.
	CampaignExperimentTypeEnum_UNSPECIFIED CampaignExperimentTypeEnum_CampaignExperimentType = 0
	// Used for return value only. Represents value unknown in this version.
	CampaignExperimentTypeEnum_UNKNOWN CampaignExperimentTypeEnum_CampaignExperimentType = 1
	// This is a regular campaign.
	CampaignExperimentTypeEnum_BASE CampaignExperimentTypeEnum_CampaignExperimentType = 2
	// This is a draft version of a campaign.
	// It has some modifications from a base campaign,
	// but it does not serve or accrue metrics.
	CampaignExperimentTypeEnum_DRAFT CampaignExperimentTypeEnum_CampaignExperimentType = 3
	// This is an experiment version of a campaign.
	// It has some modifications from a base campaign,
	// and a percentage of traffic is being diverted
	// from the BASE campaign to this experiment campaign.
	CampaignExperimentTypeEnum_EXPERIMENT CampaignExperimentTypeEnum_CampaignExperimentType = 4
)

var CampaignExperimentTypeEnum_CampaignExperimentType_name = map[int32]string{
	0: "UNSPECIFIED",
	1: "UNKNOWN",
	2: "BASE",
	3: "DRAFT",
	4: "EXPERIMENT",
}
var CampaignExperimentTypeEnum_CampaignExperimentType_value = map[string]int32{
	"UNSPECIFIED": 0,
	"UNKNOWN":     1,
	"BASE":        2,
	"DRAFT":       3,
	"EXPERIMENT":  4,
}

func (x CampaignExperimentTypeEnum_CampaignExperimentType) String() string {
	return proto.EnumName(CampaignExperimentTypeEnum_CampaignExperimentType_name, int32(x))
}
func (CampaignExperimentTypeEnum_CampaignExperimentType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_campaign_experiment_type_1f7d6a53994657ce, []int{0, 0}
}

// Container for enum describing campaign experiment type.
type CampaignExperimentTypeEnum struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *CampaignExperimentTypeEnum) Reset()         { *m = CampaignExperimentTypeEnum{} }
func (m *CampaignExperimentTypeEnum) String() string { return proto.CompactTextString(m) }
func (*CampaignExperimentTypeEnum) ProtoMessage()    {}
func (*CampaignExperimentTypeEnum) Descriptor() ([]byte, []int) {
	return fileDescriptor_campaign_experiment_type_1f7d6a53994657ce, []int{0}
}
func (m *CampaignExperimentTypeEnum) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_CampaignExperimentTypeEnum.Unmarshal(m, b)
}
func (m *CampaignExperimentTypeEnum) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_CampaignExperimentTypeEnum.Marshal(b, m, deterministic)
}
func (dst *CampaignExperimentTypeEnum) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CampaignExperimentTypeEnum.Merge(dst, src)
}
func (m *CampaignExperimentTypeEnum) XXX_Size() int {
	return xxx_messageInfo_CampaignExperimentTypeEnum.Size(m)
}
func (m *CampaignExperimentTypeEnum) XXX_DiscardUnknown() {
	xxx_messageInfo_CampaignExperimentTypeEnum.DiscardUnknown(m)
}

var xxx_messageInfo_CampaignExperimentTypeEnum proto.InternalMessageInfo

func init() {
	proto.RegisterType((*CampaignExperimentTypeEnum)(nil), "google.ads.googleads.v1.enums.CampaignExperimentTypeEnum")
	proto.RegisterEnum("google.ads.googleads.v1.enums.CampaignExperimentTypeEnum_CampaignExperimentType", CampaignExperimentTypeEnum_CampaignExperimentType_name, CampaignExperimentTypeEnum_CampaignExperimentType_value)
}

func init() {
	proto.RegisterFile("google/ads/googleads/v1/enums/campaign_experiment_type.proto", fileDescriptor_campaign_experiment_type_1f7d6a53994657ce)
}

var fileDescriptor_campaign_experiment_type_1f7d6a53994657ce = []byte{
	// 326 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x7c, 0x90, 0x4b, 0x4e, 0xf3, 0x30,
	0x1c, 0xc4, 0xbf, 0xa6, 0xfd, 0x78, 0xb8, 0x12, 0x44, 0x59, 0xb0, 0x28, 0x74, 0xd1, 0x1e, 0xc0,
	0x51, 0xc4, 0xce, 0xb0, 0x71, 0x5a, 0xb7, 0xaa, 0x10, 0x21, 0xea, 0x0b, 0x04, 0x91, 0x2a, 0xd3,
	0x58, 0x56, 0xa4, 0xc6, 0xb6, 0xea, 0xb4, 0xa2, 0xd7, 0x61, 0xc9, 0x51, 0x38, 0x0a, 0x2b, 0x8e,
	0x80, 0x12, 0x37, 0x59, 0x15, 0x36, 0xd1, 0x28, 0xf3, 0xff, 0x8d, 0xc6, 0x03, 0x6e, 0xb9, 0x94,
	0x7c, 0xc5, 0x5c, 0x1a, 0x6b, 0xd7, 0xc8, 0x5c, 0x6d, 0x3d, 0x97, 0x89, 0x4d, 0xaa, 0xdd, 0x25,
	0x4d, 0x15, 0x4d, 0xb8, 0x58, 0xb0, 0x37, 0xc5, 0xd6, 0x49, 0xca, 0x44, 0xb6, 0xc8, 0x76, 0x8a,
	0x41, 0xb5, 0x96, 0x99, 0x74, 0xda, 0x06, 0x81, 0x34, 0xd6, 0xb0, 0xa2, 0xe1, 0xd6, 0x83, 0x05,
	0xdd, 0xba, 0x2a, 0xc3, 0x55, 0xe2, 0x52, 0x21, 0x64, 0x46, 0xb3, 0x44, 0x0a, 0x6d, 0xe0, 0xee,
	0x0e, 0xb4, 0x7a, 0xfb, 0x78, 0x52, 0xa5, 0x4f, 0x77, 0x8a, 0x11, 0xb1, 0x49, 0xbb, 0x2f, 0xe0,
	0xe2, 0xb0, 0xeb, 0x9c, 0x83, 0xe6, 0x2c, 0x98, 0x84, 0xa4, 0x37, 0x1a, 0x8c, 0x48, 0xdf, 0xfe,
	0xe7, 0x34, 0xc1, 0xf1, 0x2c, 0xb8, 0x0b, 0x1e, 0x1e, 0x03, 0xbb, 0xe6, 0x9c, 0x80, 0x86, 0x8f,
	0x27, 0xc4, 0xb6, 0x9c, 0x53, 0xf0, 0xbf, 0x3f, 0xc6, 0x83, 0xa9, 0x5d, 0x77, 0xce, 0x00, 0x20,
	0x4f, 0x21, 0x19, 0x8f, 0xee, 0x49, 0x30, 0xb5, 0x1b, 0xfe, 0x77, 0x0d, 0x74, 0x96, 0x32, 0x85,
	0x7f, 0xd6, 0xf7, 0x2f, 0x0f, 0x17, 0x08, 0xf3, 0xf6, 0x61, 0xed, 0xd9, 0xdf, 0xd3, 0x5c, 0xae,
	0xa8, 0xe0, 0x50, 0xae, 0xb9, 0xcb, 0x99, 0x28, 0xde, 0x56, 0x4e, 0xa9, 0x12, 0xfd, 0xcb, 0xb2,
	0x37, 0xc5, 0xf7, 0xdd, 0xaa, 0x0f, 0x31, 0xfe, 0xb0, 0xda, 0x43, 0x13, 0x85, 0x63, 0x0d, 0x8d,
	0xcc, 0xd5, 0xdc, 0x83, 0xf9, 0x14, 0xfa, 0xb3, 0xf4, 0x23, 0x1c, 0xeb, 0xa8, 0xf2, 0xa3, 0xb9,
	0x17, 0x15, 0xfe, 0x97, 0xd5, 0x31, 0x3f, 0x11, 0xc2, 0xb1, 0x46, 0xa8, 0xba, 0x40, 0x68, 0xee,
	0x21, 0x54, 0xdc, 0xbc, 0x1e, 0x15, 0xc5, 0xae, 0x7f, 0x02, 0x00, 0x00, 0xff, 0xff, 0x50, 0x48,
	0x21, 0xf1, 0xf1, 0x01, 0x00, 0x00,
}
