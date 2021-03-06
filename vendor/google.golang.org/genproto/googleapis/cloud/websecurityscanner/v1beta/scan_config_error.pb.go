// Code generated by protoc-gen-go. DO NOT EDIT.
// source: google/cloud/websecurityscanner/v1beta/scan_config_error.proto

package websecurityscanner // import "google.golang.org/genproto/googleapis/cloud/websecurityscanner/v1beta"

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// Output only.
// Defines an error reason code.
// Next id: 43
type ScanConfigError_Code int32

const (
	// There is no error.
	ScanConfigError_CODE_UNSPECIFIED ScanConfigError_Code = 0
	// There is no error.
	ScanConfigError_OK ScanConfigError_Code = 0
	// Indicates an internal server error.
	// Please DO NOT USE THIS ERROR CODE unless the root cause is truly unknown.
	ScanConfigError_INTERNAL_ERROR ScanConfigError_Code = 1
	// One of the seed URLs is an App Engine URL but we cannot validate the scan
	// settings due to an App Engine API backend error.
	ScanConfigError_APPENGINE_API_BACKEND_ERROR ScanConfigError_Code = 2
	// One of the seed URLs is an App Engine URL but we cannot access the
	// App Engine API to validate scan settings.
	ScanConfigError_APPENGINE_API_NOT_ACCESSIBLE ScanConfigError_Code = 3
	// One of the seed URLs is an App Engine URL but the Default Host of the
	// App Engine is not set.
	ScanConfigError_APPENGINE_DEFAULT_HOST_MISSING ScanConfigError_Code = 4
	// Google corporate accounts can not be used for scanning.
	ScanConfigError_CANNOT_USE_GOOGLE_COM_ACCOUNT ScanConfigError_Code = 6
	// The account of the scan creator can not be used for scanning.
	ScanConfigError_CANNOT_USE_OWNER_ACCOUNT ScanConfigError_Code = 7
	// This scan targets Compute Engine, but we cannot validate scan settings
	// due to a Compute Engine API backend error.
	ScanConfigError_COMPUTE_API_BACKEND_ERROR ScanConfigError_Code = 8
	// This scan targets Compute Engine, but we cannot access the Compute Engine
	// API to validate the scan settings.
	ScanConfigError_COMPUTE_API_NOT_ACCESSIBLE ScanConfigError_Code = 9
	// The Custom Login URL does not belong to the current project.
	ScanConfigError_CUSTOM_LOGIN_URL_DOES_NOT_BELONG_TO_CURRENT_PROJECT ScanConfigError_Code = 10
	// The Custom Login URL is malformed (can not be parsed).
	ScanConfigError_CUSTOM_LOGIN_URL_MALFORMED ScanConfigError_Code = 11
	// The Custom Login URL is mapped to a non-routable IP address in DNS.
	ScanConfigError_CUSTOM_LOGIN_URL_MAPPED_TO_NON_ROUTABLE_ADDRESS ScanConfigError_Code = 12
	// The Custom Login URL is mapped to an IP address which is not reserved for
	// the current project.
	ScanConfigError_CUSTOM_LOGIN_URL_MAPPED_TO_UNRESERVED_ADDRESS ScanConfigError_Code = 13
	// The Custom Login URL has a non-routable IP address.
	ScanConfigError_CUSTOM_LOGIN_URL_HAS_NON_ROUTABLE_IP_ADDRESS ScanConfigError_Code = 14
	// The Custom Login URL has an IP address which is not reserved for the
	// current project.
	ScanConfigError_CUSTOM_LOGIN_URL_HAS_UNRESERVED_IP_ADDRESS ScanConfigError_Code = 15
	// Another scan with the same name (case-sensitive) already exists.
	ScanConfigError_DUPLICATE_SCAN_NAME ScanConfigError_Code = 16
	// A field is set to an invalid value.
	ScanConfigError_INVALID_FIELD_VALUE ScanConfigError_Code = 18
	// There was an error trying to authenticate to the scan target.
	ScanConfigError_FAILED_TO_AUTHENTICATE_TO_TARGET ScanConfigError_Code = 19
	// Finding type value is not specified in the list findings request.
	ScanConfigError_FINDING_TYPE_UNSPECIFIED ScanConfigError_Code = 20
	// Scan targets Compute Engine, yet current project was not whitelisted for
	// Google Compute Engine Scanning Alpha access.
	ScanConfigError_FORBIDDEN_TO_SCAN_COMPUTE ScanConfigError_Code = 21
	// The supplied filter is malformed. For example, it can not be parsed, does
	// not have a filter type in expression, or the same filter type appears
	// more than once.
	ScanConfigError_MALFORMED_FILTER ScanConfigError_Code = 22
	// The supplied resource name is malformed (can not be parsed).
	ScanConfigError_MALFORMED_RESOURCE_NAME ScanConfigError_Code = 23
	// The current project is not in an active state.
	ScanConfigError_PROJECT_INACTIVE ScanConfigError_Code = 24
	// A required field is not set.
	ScanConfigError_REQUIRED_FIELD ScanConfigError_Code = 25
	// Project id, scanconfig id, scanrun id, or finding id are not consistent
	// with each other in resource name.
	ScanConfigError_RESOURCE_NAME_INCONSISTENT ScanConfigError_Code = 26
	// The scan being requested to start is already running.
	ScanConfigError_SCAN_ALREADY_RUNNING ScanConfigError_Code = 27
	// The scan that was requested to be stopped is not running.
	ScanConfigError_SCAN_NOT_RUNNING ScanConfigError_Code = 28
	// One of the seed URLs does not belong to the current project.
	ScanConfigError_SEED_URL_DOES_NOT_BELONG_TO_CURRENT_PROJECT ScanConfigError_Code = 29
	// One of the seed URLs is malformed (can not be parsed).
	ScanConfigError_SEED_URL_MALFORMED ScanConfigError_Code = 30
	// One of the seed URLs is mapped to a non-routable IP address in DNS.
	ScanConfigError_SEED_URL_MAPPED_TO_NON_ROUTABLE_ADDRESS ScanConfigError_Code = 31
	// One of the seed URLs is mapped to an IP address which is not reserved
	// for the current project.
	ScanConfigError_SEED_URL_MAPPED_TO_UNRESERVED_ADDRESS ScanConfigError_Code = 32
	// One of the seed URLs has on-routable IP address.
	ScanConfigError_SEED_URL_HAS_NON_ROUTABLE_IP_ADDRESS ScanConfigError_Code = 33
	// One of the seed URLs has an IP address that is not reserved
	// for the current project.
	ScanConfigError_SEED_URL_HAS_UNRESERVED_IP_ADDRESS ScanConfigError_Code = 35
	// The Cloud Security Scanner service account is not configured under the
	// project.
	ScanConfigError_SERVICE_ACCOUNT_NOT_CONFIGURED ScanConfigError_Code = 36
	// A project has reached the maximum number of scans.
	ScanConfigError_TOO_MANY_SCANS ScanConfigError_Code = 37
	// Resolving the details of the current project fails.
	ScanConfigError_UNABLE_TO_RESOLVE_PROJECT_INFO ScanConfigError_Code = 38
	// One or more blacklist patterns were in the wrong format.
	ScanConfigError_UNSUPPORTED_BLACKLIST_PATTERN_FORMAT ScanConfigError_Code = 39
	// The supplied filter is not supported.
	ScanConfigError_UNSUPPORTED_FILTER ScanConfigError_Code = 40
	// The supplied finding type is not supported. For example, we do not
	// provide findings of the given finding type.
	ScanConfigError_UNSUPPORTED_FINDING_TYPE ScanConfigError_Code = 41
	// The URL scheme of one or more of the supplied URLs is not supported.
	ScanConfigError_UNSUPPORTED_URL_SCHEME ScanConfigError_Code = 42
)

var ScanConfigError_Code_name = map[int32]string{
	0: "CODE_UNSPECIFIED",
	// Duplicate value: 0: "OK",
	1:  "INTERNAL_ERROR",
	2:  "APPENGINE_API_BACKEND_ERROR",
	3:  "APPENGINE_API_NOT_ACCESSIBLE",
	4:  "APPENGINE_DEFAULT_HOST_MISSING",
	6:  "CANNOT_USE_GOOGLE_COM_ACCOUNT",
	7:  "CANNOT_USE_OWNER_ACCOUNT",
	8:  "COMPUTE_API_BACKEND_ERROR",
	9:  "COMPUTE_API_NOT_ACCESSIBLE",
	10: "CUSTOM_LOGIN_URL_DOES_NOT_BELONG_TO_CURRENT_PROJECT",
	11: "CUSTOM_LOGIN_URL_MALFORMED",
	12: "CUSTOM_LOGIN_URL_MAPPED_TO_NON_ROUTABLE_ADDRESS",
	13: "CUSTOM_LOGIN_URL_MAPPED_TO_UNRESERVED_ADDRESS",
	14: "CUSTOM_LOGIN_URL_HAS_NON_ROUTABLE_IP_ADDRESS",
	15: "CUSTOM_LOGIN_URL_HAS_UNRESERVED_IP_ADDRESS",
	16: "DUPLICATE_SCAN_NAME",
	18: "INVALID_FIELD_VALUE",
	19: "FAILED_TO_AUTHENTICATE_TO_TARGET",
	20: "FINDING_TYPE_UNSPECIFIED",
	21: "FORBIDDEN_TO_SCAN_COMPUTE",
	22: "MALFORMED_FILTER",
	23: "MALFORMED_RESOURCE_NAME",
	24: "PROJECT_INACTIVE",
	25: "REQUIRED_FIELD",
	26: "RESOURCE_NAME_INCONSISTENT",
	27: "SCAN_ALREADY_RUNNING",
	28: "SCAN_NOT_RUNNING",
	29: "SEED_URL_DOES_NOT_BELONG_TO_CURRENT_PROJECT",
	30: "SEED_URL_MALFORMED",
	31: "SEED_URL_MAPPED_TO_NON_ROUTABLE_ADDRESS",
	32: "SEED_URL_MAPPED_TO_UNRESERVED_ADDRESS",
	33: "SEED_URL_HAS_NON_ROUTABLE_IP_ADDRESS",
	35: "SEED_URL_HAS_UNRESERVED_IP_ADDRESS",
	36: "SERVICE_ACCOUNT_NOT_CONFIGURED",
	37: "TOO_MANY_SCANS",
	38: "UNABLE_TO_RESOLVE_PROJECT_INFO",
	39: "UNSUPPORTED_BLACKLIST_PATTERN_FORMAT",
	40: "UNSUPPORTED_FILTER",
	41: "UNSUPPORTED_FINDING_TYPE",
	42: "UNSUPPORTED_URL_SCHEME",
}
var ScanConfigError_Code_value = map[string]int32{
	"CODE_UNSPECIFIED":                                    0,
	"OK":                                                  0,
	"INTERNAL_ERROR":                                      1,
	"APPENGINE_API_BACKEND_ERROR":                         2,
	"APPENGINE_API_NOT_ACCESSIBLE":                        3,
	"APPENGINE_DEFAULT_HOST_MISSING":                      4,
	"CANNOT_USE_GOOGLE_COM_ACCOUNT":                       6,
	"CANNOT_USE_OWNER_ACCOUNT":                            7,
	"COMPUTE_API_BACKEND_ERROR":                           8,
	"COMPUTE_API_NOT_ACCESSIBLE":                          9,
	"CUSTOM_LOGIN_URL_DOES_NOT_BELONG_TO_CURRENT_PROJECT": 10,
	"CUSTOM_LOGIN_URL_MALFORMED":                          11,
	"CUSTOM_LOGIN_URL_MAPPED_TO_NON_ROUTABLE_ADDRESS":     12,
	"CUSTOM_LOGIN_URL_MAPPED_TO_UNRESERVED_ADDRESS":       13,
	"CUSTOM_LOGIN_URL_HAS_NON_ROUTABLE_IP_ADDRESS":        14,
	"CUSTOM_LOGIN_URL_HAS_UNRESERVED_IP_ADDRESS":          15,
	"DUPLICATE_SCAN_NAME":                                 16,
	"INVALID_FIELD_VALUE":                                 18,
	"FAILED_TO_AUTHENTICATE_TO_TARGET":                    19,
	"FINDING_TYPE_UNSPECIFIED":                            20,
	"FORBIDDEN_TO_SCAN_COMPUTE":                           21,
	"MALFORMED_FILTER":                                    22,
	"MALFORMED_RESOURCE_NAME":                             23,
	"PROJECT_INACTIVE":                                    24,
	"REQUIRED_FIELD":                                      25,
	"RESOURCE_NAME_INCONSISTENT":                          26,
	"SCAN_ALREADY_RUNNING":                                27,
	"SCAN_NOT_RUNNING":                                    28,
	"SEED_URL_DOES_NOT_BELONG_TO_CURRENT_PROJECT":         29,
	"SEED_URL_MALFORMED":                                  30,
	"SEED_URL_MAPPED_TO_NON_ROUTABLE_ADDRESS":             31,
	"SEED_URL_MAPPED_TO_UNRESERVED_ADDRESS":               32,
	"SEED_URL_HAS_NON_ROUTABLE_IP_ADDRESS":                33,
	"SEED_URL_HAS_UNRESERVED_IP_ADDRESS":                  35,
	"SERVICE_ACCOUNT_NOT_CONFIGURED":                      36,
	"TOO_MANY_SCANS":                                      37,
	"UNABLE_TO_RESOLVE_PROJECT_INFO":                      38,
	"UNSUPPORTED_BLACKLIST_PATTERN_FORMAT":                39,
	"UNSUPPORTED_FILTER":                                  40,
	"UNSUPPORTED_FINDING_TYPE":                            41,
	"UNSUPPORTED_URL_SCHEME":                              42,
}

func (x ScanConfigError_Code) String() string {
	return proto.EnumName(ScanConfigError_Code_name, int32(x))
}
func (ScanConfigError_Code) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_scan_config_error_4b32cc55957c457c, []int{0, 0}
}

// Defines a custom error message used by CreateScanConfig and UpdateScanConfig
// APIs when scan configuration validation fails. It is also reported as part of
// a ScanRunErrorTrace message if scan validation fails due to a scan
// configuration error.
type ScanConfigError struct {
	// Output only.
	// Indicates the reason code for a configuration failure.
	Code ScanConfigError_Code `protobuf:"varint,1,opt,name=code,proto3,enum=google.cloud.websecurityscanner.v1beta.ScanConfigError_Code" json:"code,omitempty"`
	// Output only.
	// Indicates the full name of the ScanConfig field that triggers this error,
	// for example "scan_config.max_qps". This field is provided for
	// troubleshooting purposes only and its actual value can change in the
	// future.
	FieldName            string   `protobuf:"bytes,2,opt,name=field_name,json=fieldName,proto3" json:"field_name,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ScanConfigError) Reset()         { *m = ScanConfigError{} }
func (m *ScanConfigError) String() string { return proto.CompactTextString(m) }
func (*ScanConfigError) ProtoMessage()    {}
func (*ScanConfigError) Descriptor() ([]byte, []int) {
	return fileDescriptor_scan_config_error_4b32cc55957c457c, []int{0}
}
func (m *ScanConfigError) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ScanConfigError.Unmarshal(m, b)
}
func (m *ScanConfigError) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ScanConfigError.Marshal(b, m, deterministic)
}
func (dst *ScanConfigError) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ScanConfigError.Merge(dst, src)
}
func (m *ScanConfigError) XXX_Size() int {
	return xxx_messageInfo_ScanConfigError.Size(m)
}
func (m *ScanConfigError) XXX_DiscardUnknown() {
	xxx_messageInfo_ScanConfigError.DiscardUnknown(m)
}

var xxx_messageInfo_ScanConfigError proto.InternalMessageInfo

func (m *ScanConfigError) GetCode() ScanConfigError_Code {
	if m != nil {
		return m.Code
	}
	return ScanConfigError_CODE_UNSPECIFIED
}

func (m *ScanConfigError) GetFieldName() string {
	if m != nil {
		return m.FieldName
	}
	return ""
}

func init() {
	proto.RegisterType((*ScanConfigError)(nil), "google.cloud.websecurityscanner.v1beta.ScanConfigError")
	proto.RegisterEnum("google.cloud.websecurityscanner.v1beta.ScanConfigError_Code", ScanConfigError_Code_name, ScanConfigError_Code_value)
}

func init() {
	proto.RegisterFile("google/cloud/websecurityscanner/v1beta/scan_config_error.proto", fileDescriptor_scan_config_error_4b32cc55957c457c)
}

var fileDescriptor_scan_config_error_4b32cc55957c457c = []byte{
	// 865 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x55, 0x5d, 0x53, 0x1b, 0x37,
	0x14, 0x8d, 0x29, 0x43, 0x8b, 0xd2, 0x12, 0x8d, 0x42, 0xc1, 0xe1, 0x2b, 0x0e, 0x25, 0x84, 0x90,
	0xd6, 0x6e, 0xca, 0x43, 0x1f, 0xda, 0xe9, 0x8c, 0x2c, 0xdd, 0x35, 0x2a, 0x5a, 0x69, 0xab, 0x0f,
	0xb7, 0xf4, 0x45, 0x63, 0xcc, 0xc6, 0xc3, 0x0c, 0x78, 0x33, 0x86, 0xb4, 0xd3, 0x3f, 0xd3, 0x3f,
	0xd5, 0x97, 0xfe, 0x9c, 0x8e, 0xd6, 0xae, 0x3f, 0xc0, 0x43, 0x78, 0xdc, 0x7b, 0xcf, 0x39, 0xf7,
	0xea, 0xde, 0xb3, 0x12, 0xfa, 0xa9, 0x57, 0x14, 0xbd, 0xcb, 0xbc, 0xd1, 0xbd, 0x2c, 0x3e, 0x9c,
	0x37, 0xfe, 0xcc, 0xcf, 0xae, 0xf3, 0xee, 0x87, 0xc1, 0xc5, 0xcd, 0x5f, 0xd7, 0xdd, 0x4e, 0xbf,
	0x9f, 0x0f, 0x1a, 0x7f, 0xbc, 0x3d, 0xcb, 0x6f, 0x3a, 0x8d, 0xf8, 0x19, 0xba, 0x45, 0xff, 0xdd,
	0x45, 0x2f, 0xe4, 0x83, 0x41, 0x31, 0xa8, 0xbf, 0x1f, 0x14, 0x37, 0x05, 0xd9, 0x1f, 0xf2, 0xeb,
	0x25, 0xbf, 0x7e, 0x97, 0x5f, 0x1f, 0xf2, 0x77, 0xff, 0x7d, 0x8c, 0x9e, 0xd8, 0x6e, 0xa7, 0xcf,
	0x4a, 0x09, 0x88, 0x0a, 0x24, 0x43, 0x8b, 0xdd, 0xe2, 0x3c, 0xaf, 0x56, 0x6a, 0x95, 0x83, 0x95,
	0xef, 0x7e, 0xac, 0x3f, 0x4c, 0xaa, 0x7e, 0x4b, 0xa6, 0xce, 0x8a, 0xf3, 0xdc, 0x94, 0x4a, 0x64,
	0x1b, 0xa1, 0x77, 0x17, 0xf9, 0xe5, 0x79, 0xe8, 0x77, 0xae, 0xf2, 0xea, 0x42, 0xad, 0x72, 0xb0,
	0x6c, 0x96, 0xcb, 0x88, 0xea, 0x5c, 0xe5, 0xbb, 0xff, 0x20, 0xb4, 0x18, 0xd1, 0x64, 0x15, 0x61,
	0xa6, 0x39, 0x04, 0xaf, 0x6c, 0x06, 0x4c, 0x24, 0x02, 0x38, 0x7e, 0x44, 0x96, 0xd0, 0x82, 0x3e,
	0xc1, 0x8f, 0x08, 0x41, 0x2b, 0x42, 0x39, 0x30, 0x8a, 0xca, 0x00, 0xc6, 0x68, 0x83, 0x2b, 0xe4,
	0x39, 0xda, 0xa4, 0x59, 0x06, 0xaa, 0x25, 0x14, 0x04, 0x9a, 0x89, 0xd0, 0xa4, 0xec, 0x04, 0x14,
	0x1f, 0x01, 0x16, 0x48, 0x0d, 0x6d, 0xcd, 0x02, 0x94, 0x76, 0x81, 0x32, 0x06, 0xd6, 0x8a, 0xa6,
	0x04, 0xfc, 0x09, 0xd9, 0x45, 0x3b, 0x13, 0x04, 0x87, 0x84, 0x7a, 0xe9, 0xc2, 0xb1, 0xb6, 0x2e,
	0xa4, 0xc2, 0x5a, 0xa1, 0x5a, 0x78, 0x91, 0xbc, 0x40, 0xdb, 0x8c, 0xaa, 0x48, 0xf5, 0x16, 0x42,
	0x4b, 0xeb, 0x96, 0x84, 0xc0, 0x74, 0x1a, 0x95, 0xb4, 0x57, 0x0e, 0x2f, 0x91, 0x2d, 0x54, 0x9d,
	0x82, 0xe8, 0x5f, 0x15, 0x98, 0x71, 0xf6, 0x53, 0xb2, 0x8d, 0x9e, 0x31, 0x9d, 0x66, 0xde, 0xcd,
	0xeb, 0xf2, 0x33, 0xb2, 0x83, 0x36, 0xa6, 0xd3, 0xb7, 0x7a, 0x5c, 0x26, 0xdf, 0xa3, 0x23, 0xe6,
	0xad, 0xd3, 0x69, 0x90, 0xba, 0x25, 0x54, 0xf0, 0x46, 0x06, 0xae, 0xc1, 0x96, 0xc8, 0x26, 0x48,
	0xad, 0x5a, 0xc1, 0xe9, 0xc0, 0xbc, 0x31, 0xa0, 0x5c, 0xc8, 0x8c, 0xfe, 0x19, 0x98, 0xc3, 0xa8,
	0x14, 0xbe, 0x4d, 0x4c, 0xa9, 0x4c, 0xb4, 0x49, 0x81, 0xe3, 0xc7, 0xe4, 0x08, 0x35, 0xe6, 0xe4,
	0xb3, 0x0c, 0x78, 0xd4, 0x53, 0x5a, 0x05, 0xa3, 0xbd, 0xa3, 0x4d, 0x09, 0x81, 0x72, 0x6e, 0xc0,
	0x5a, 0xfc, 0x39, 0x79, 0x8b, 0xbe, 0xb9, 0x87, 0xe4, 0x95, 0x01, 0x0b, 0xa6, 0x0d, 0x7c, 0x4c,
	0xf9, 0x82, 0x7c, 0x8b, 0xbe, 0xbe, 0x43, 0x39, 0xa6, 0x76, 0xb6, 0x82, 0xc8, 0xc6, 0x8c, 0x15,
	0x52, 0x47, 0x87, 0x73, 0x19, 0x53, 0xf2, 0x53, 0xf8, 0x27, 0x64, 0x1d, 0x3d, 0xe5, 0x3e, 0x93,
	0x82, 0x51, 0x07, 0xc1, 0x32, 0xaa, 0x82, 0xa2, 0x29, 0x60, 0x1c, 0x13, 0x42, 0xb5, 0xa9, 0x14,
	0x3c, 0x24, 0x02, 0x24, 0x0f, 0x6d, 0x2a, 0x3d, 0x60, 0x42, 0xf6, 0x50, 0x2d, 0xa1, 0x42, 0x0e,
	0xbb, 0xa6, 0xde, 0x1d, 0x83, 0x72, 0x43, 0xba, 0xd3, 0xc1, 0x51, 0xd3, 0x02, 0x87, 0x9f, 0xc6,
	0xbd, 0x26, 0x42, 0x71, 0x11, 0x27, 0x7c, 0x9a, 0xcd, 0x7a, 0x73, 0x35, 0xee, 0x35, 0xd1, 0xa6,
	0x29, 0x38, 0x07, 0x15, 0x69, 0x65, 0xe1, 0xd1, 0x2a, 0xf1, 0x97, 0xd1, 0xd0, 0xe3, 0x69, 0x87,
	0x44, 0x48, 0x07, 0x06, 0xaf, 0x91, 0x4d, 0xb4, 0x3e, 0x89, 0x1a, 0xb0, 0xda, 0x1b, 0x06, 0xc3,
	0x76, 0xd7, 0x23, 0x65, 0xb4, 0xbe, 0x20, 0x14, 0x65, 0x4e, 0xb4, 0x01, 0x57, 0xa3, 0xf7, 0x0d,
	0xfc, 0xe2, 0x85, 0x81, 0xd1, 0x29, 0xf0, 0xb3, 0xb8, 0xdb, 0x19, 0x72, 0x10, 0x8a, 0x69, 0x65,
	0x85, 0x75, 0xa0, 0x1c, 0xde, 0x20, 0x55, 0xb4, 0x5a, 0xb6, 0x43, 0xa5, 0x01, 0xca, 0x4f, 0x83,
	0xf1, 0x4a, 0x45, 0x3b, 0x6f, 0xc6, 0x1a, 0xc3, 0x09, 0x69, 0x37, 0x8e, 0x6e, 0x91, 0x06, 0x7a,
	0x63, 0x01, 0xf8, 0x43, 0xcd, 0xb5, 0x4d, 0xd6, 0x10, 0x19, 0x13, 0x26, 0xa6, 0xda, 0x21, 0x6f,
	0xd0, 0xab, 0xa9, 0xf8, 0xbd, 0x66, 0x7a, 0x4e, 0x5e, 0xa3, 0x97, 0x73, 0xc0, 0x73, 0x4c, 0x54,
	0x23, 0x07, 0x68, 0x6f, 0x0c, 0xbd, 0xcf, 0x3c, 0x2f, 0xc8, 0x3e, 0xda, 0x9d, 0x41, 0xce, 0x37,
	0xcd, 0x57, 0xf1, 0xdf, 0x8f, 0x61, 0xc1, 0xe0, 0xff, 0x7f, 0xb5, 0x3c, 0x34, 0xd3, 0x2a, 0x11,
	0x2d, 0x6f, 0x80, 0xe3, 0xbd, 0x38, 0x7a, 0xa7, 0x75, 0x48, 0xa9, 0x3a, 0x2d, 0xd7, 0x6b, 0xf1,
	0xcb, 0xc8, 0xf3, 0xaa, 0x2c, 0xeb, 0x74, 0xb9, 0x41, 0xd9, 0x86, 0x30, 0x59, 0x5b, 0xa2, 0xf1,
	0x7e, 0xec, 0xd6, 0x2b, 0xeb, 0xb3, 0x4c, 0x1b, 0x07, 0x3c, 0x34, 0x25, 0x65, 0x27, 0x52, 0x58,
	0x17, 0x32, 0xea, 0xe2, 0x2d, 0x16, 0xe2, 0xbc, 0xa8, 0xc3, 0xaf, 0xe2, 0x1c, 0xa7, 0x91, 0x23,
	0x9f, 0x1c, 0x44, 0xeb, 0xcd, 0xc6, 0x27, 0x36, 0xc4, 0xaf, 0xc9, 0x06, 0x5a, 0x9b, 0xce, 0xc6,
	0xa3, 0x5a, 0x76, 0x0c, 0x29, 0xe0, 0xc3, 0x8d, 0x05, 0x5c, 0x69, 0xfe, 0x5d, 0x41, 0x87, 0xdd,
	0xe2, 0xea, 0x81, 0xd7, 0x77, 0x73, 0xf5, 0xd6, 0xfd, 0x9d, 0xc5, 0x77, 0x24, 0xab, 0xfc, 0xfe,
	0xdb, 0x88, 0xdf, 0x2b, 0x2e, 0x3b, 0xfd, 0x5e, 0xbd, 0x18, 0xf4, 0x1a, 0xbd, 0xbc, 0x5f, 0xbe,
	0x32, 0x8d, 0x61, 0xaa, 0xf3, 0xfe, 0xe2, 0xfa, 0x63, 0x0f, 0xd5, 0x0f, 0x77, 0x33, 0x67, 0x4b,
	0xa5, 0xc8, 0xd1, 0x7f, 0x01, 0x00, 0x00, 0xff, 0xff, 0x82, 0x51, 0xa7, 0xe9, 0xec, 0x06, 0x00,
	0x00,
}
