package csilvm

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

// IdentityService RPCs

func TestGetPluginInfoMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetPluginInfoRequest()
	req.Version = nil
	resp, err := client.GetPluginInfo(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The version field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestGetPluginInfoUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetPluginInfoRequest()
	req.Version = &csi.Version{0, 2, 0}
	resp, err := client.GetPluginInfo(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "The requested version is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

// ControllerService RPCs

func TestCreateVolumeRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testCreateVolumeRequest()
	req.Version = nil
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNDEFINED
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "This service is running in 'remove volume group' mode."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestCreateVolumeMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.Version = nil
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The version field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestCreateVolumeUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.Version = &csi.Version{0, 2, 0}
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "The requested version is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestCreateVolumeMissingName(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.Name = ""
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The name field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestCreateVolumeMissingVolumeCapabilities(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.VolumeCapabilities = nil
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_capabilities field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestCreateVolumeEmptyVolumeCapabilities(t *testing.T) {
	t.Skip("gRPC apparently unmarshals an empty list as nil.")
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.VolumeCapabilities = req.VolumeCapabilities[:0]
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "One or more volume_capabilities must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestCreateVolumeMissingVolumeCapabilitiesAccessType(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.VolumeCapabilities[0].AccessType = nil
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_capability.access_type field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestCreateVolumeMissingVolumeCapabilitiesAccessMode(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.VolumeCapabilities[0].AccessMode = nil
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_capability.access_mode field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestCreateVolumeVolumeCapabilitiesAccessModeUNKNOWN(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.VolumeCapabilities[0].AccessMode.Mode = csi.VolumeCapability_AccessMode_UNKNOWN
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_capability.access_mode.mode field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestDeleteVolumeRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testDeleteVolumeRequest("test-volume")
	req.Version = nil
	resp, err := client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNDEFINED
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "This service is running in 'remove volume group' mode."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestDeleteVolumeMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testDeleteVolumeRequest("test-volume")
	req.Version = nil
	resp, err := client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The version field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestDeleteVolumeUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testDeleteVolumeRequest("test-volume")
	req.Version = &csi.Version{0, 2, 0}
	resp, err := client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "The requested version is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestDeleteVolumeMissingVolumeHandle(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testDeleteVolumeRequest("test-volume")
	req.VolumeHandle = nil
	resp, err := client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume handle must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestDeleteVolumeMissingVolumeHandleId(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testDeleteVolumeRequest("test-volume")
	req.VolumeHandle.Id = ""
	resp, err := client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_handle.id field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestValidateVolumeCapabilitiesRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest(fakeVolumeHandle(), "", nil)
	req.Version = nil
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNDEFINED
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "This service is running in 'remove volume group' mode."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestValidateVolumeCapabilitiesMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest(fakeVolumeHandle(), "", nil)
	req.Version = nil
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The version field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestValidateVolumeCapabilitiesUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest(fakeVolumeHandle(), "", nil)
	req.Version = &csi.Version{0, 2, 0}
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "The requested version is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestValidateVolumeCapabilitiesMissingVolumeInfo(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest(fakeVolumeHandle(), "", nil)
	req.VolumeInfo = nil
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_info field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestValidateVolumeCapabilitiesMissingVolumeInfoHandle(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest(fakeVolumeHandle(), "", nil)
	req.VolumeInfo.Handle = nil
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume handle must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestValidateVolumeCapabilitiesMissingVolumeInfoHandleId(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest(fakeVolumeHandle(), "", nil)
	req.VolumeInfo.Handle.Id = ""
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_handle.id field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestValidateVolumeCapabilitiesMissingVolumeCapabilities(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest(fakeVolumeHandle(), "", nil)
	req.VolumeCapabilities = nil
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_capabilities field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestValidateVolumeCapabilitiesEmptyVolumeCapabilities(t *testing.T) {
	t.Skip("gRPC apparently unmarshals an empty list as nil.")
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest(fakeVolumeHandle(), "", nil)
	req.VolumeCapabilities = req.VolumeCapabilities[:0]
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "One or more volume_capabilities must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestValidateVolumeCapabilitiesMissingVolumeCapabilitiesAccessType(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest(fakeVolumeHandle(), "", nil)
	req.VolumeCapabilities[0].AccessType = nil
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_capability.access_type field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestValidateVolumeCapabilitiesNodeUnpublishVolume_MountVolume_BadFilesystem(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest(fakeVolumeHandle(), "ext4", nil)
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetValidateVolumeCapabilitiesError()
	expcode := csi.Error_ValidateVolumeCapabilitiesError_UNSUPPORTED_FS_TYPE
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	expdesc := "Requested filesystem type is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestValidateVolumeCapabilitiesMissingVolumeCapabilitiesAccessMode(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest(fakeVolumeHandle(), "", nil)
	req.VolumeCapabilities[0].AccessMode = nil
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_capability.access_mode field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestValidateVolumeCapabilitiesVolumeCapabilitiesAccessModeUNKNOWN(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest(fakeVolumeHandle(), "", nil)
	req.VolumeCapabilities[0].AccessMode.Mode = csi.VolumeCapability_AccessMode_UNKNOWN
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_capability.access_mode.mode field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestListVolumesRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testListVolumesRequest()
	req.Version = nil
	resp, err := client.ListVolumes(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNDEFINED
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "This service is running in 'remove volume group' mode."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestListVolumesMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testListVolumesRequest()
	req.Version = nil
	resp, err := client.ListVolumes(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The version field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestListVolumesUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testListVolumesRequest()
	req.Version = &csi.Version{0, 2, 0}
	resp, err := client.ListVolumes(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "The requested version is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestGetCapacityMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetCapacityRequest("xfs")
	req.Version = nil
	resp, err := client.GetCapacity(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The version field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestGetCapacityUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetCapacityRequest("xfs")
	req.Version = &csi.Version{0, 2, 0}
	resp, err := client.GetCapacity(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "The requested version is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestGetCapacityMissingVolumeCapabilitiesAccessType(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetCapacityRequest("xfs")
	req.VolumeCapabilities[0].AccessType = nil
	resp, err := client.GetCapacity(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_capability.access_type field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestGetCapacity_BadFilesystem(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetCapacityRequest("ext4")
	resp, err := client.GetCapacity(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.GetError(); err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result.GetAvailableCapacity() != 0 {
		t.Fatalf("Expected 0 bytes for unsupported filesystem but got %v.", result.GetAvailableCapacity())
	}
}

func TestGetCapacityMissingVolumeCapabilitiesAccessMode(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetCapacityRequest("xfs")
	req.VolumeCapabilities[0].AccessMode = nil
	resp, err := client.GetCapacity(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_capability.access_mode field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestGetCapacityVolumeCapabilitiesAccessModeUNKNOWN(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetCapacityRequest("xfs")
	req.VolumeCapabilities[0].AccessMode.Mode = csi.VolumeCapability_AccessMode_UNKNOWN
	resp, err := client.GetCapacity(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_capability.access_mode.mode field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestControllerGetCapabilitiesRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := &csi.ControllerGetCapabilitiesRequest{}
	req.Version = nil
	resp, err := client.ControllerGetCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNDEFINED
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "This service is running in 'remove volume group' mode."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestControllerGetCapabilitiesInfoMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.ControllerGetCapabilitiesRequest{}
	resp, err := client.ControllerGetCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The version field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestControllerGetCapabilitiesInfoUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.ControllerGetCapabilitiesRequest{Version: &csi.Version{0, 2, 0}}
	resp, err := client.ControllerGetCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "The requested version is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

// NodeService RPCs

func fakeVolumeHandle() *csi.VolumeHandle {
	return &csi.VolumeHandle{"test-volume", nil}
}

var fakeMountDir = "/run/dcos/csilvm/mnt"

func TestNodePublishVolumeRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testNodePublishVolumeRequest(fakeVolumeHandle(), fakeMountDir, "", nil)
	req.Version = nil
	resp, err := client.NodePublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNDEFINED
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "This service is running in 'remove volume group' mode."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodePublishVolumeMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest(fakeVolumeHandle(), fakeMountDir, "", nil)
	req.Version = nil
	resp, err := client.NodePublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The version field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodePublishVolumeUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest(fakeVolumeHandle(), fakeMountDir, "", nil)
	req.Version = &csi.Version{0, 2, 0}
	resp, err := client.NodePublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "The requested version is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodePublishVolumeMissingVolumeHandle(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest(fakeVolumeHandle(), fakeMountDir, "", nil)
	req.VolumeHandle = nil
	resp, err := client.NodePublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume handle must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodePublishVolumeMissingVolumeHandleId(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest(fakeVolumeHandle(), fakeMountDir, "", nil)
	req.VolumeHandle.Id = ""
	resp, err := client.NodePublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_handle.id field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodePublishVolumeNotMissingPublishVolumeInfo(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest(fakeVolumeHandle(), fakeMountDir, "", nil)
	req.PublishVolumeInfo = &csi.PublishVolumeInfo{nil}
	resp, err := client.NodePublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNDEFINED
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "The publish_volume_info field must not be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodePublishVolumeMissingTargetPath(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest(fakeVolumeHandle(), fakeMountDir, "", nil)
	req.TargetPath = ""
	resp, err := client.NodePublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The target_path field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodePublishVolumeMissingVolumeCapability(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest(fakeVolumeHandle(), fakeMountDir, "", nil)
	req.VolumeCapability = nil
	resp, err := client.NodePublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_capability field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodePublishVolumeMissingVolumeCapabilityAccessType(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest(fakeVolumeHandle(), fakeMountDir, "", nil)
	req.VolumeCapability.AccessType = nil
	resp, err := client.NodePublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_capability.access_type field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodePublishVolumeMissingVolumeCapabilityAccessMode(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest(fakeVolumeHandle(), fakeMountDir, "", nil)
	req.VolumeCapability.AccessMode = nil
	resp, err := client.NodePublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_capability.access_mode field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodePublishVolumeVolumeCapabilityAccessModeUNKNOWN(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest(fakeVolumeHandle(), fakeMountDir, "", nil)
	req.VolumeCapability.AccessMode.Mode = csi.VolumeCapability_AccessMode_UNKNOWN
	resp, err := client.NodePublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_capability.access_mode.mode field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodePublishVolumeNodeUnpublishVolume_MountVolume_BadFilesystem(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest(fakeVolumeHandle(), fakeMountDir, "ext4", nil)
	resp, err := client.NodePublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetNodePublishVolumeError()
	expcode := csi.Error_NodePublishVolumeError_UNSUPPORTED_FS_TYPE
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	expdesc := "Requested filesystem type is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

var fakeTargetPath = filepath.Join(fakeMountDir, fakeVolumeHandle().GetId())

func TestNodeUnpublishVolumeRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testNodeUnpublishVolumeRequest(fakeVolumeHandle(), fakeTargetPath)
	req.Version = nil
	resp, err := client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNDEFINED
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "This service is running in 'remove volume group' mode."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodeUnpublishVolumeMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeUnpublishVolumeRequest(fakeVolumeHandle(), fakeTargetPath)
	req.Version = nil
	resp, err := client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The version field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodeUnpublishVolumeUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeUnpublishVolumeRequest(fakeVolumeHandle(), fakeTargetPath)
	req.Version = &csi.Version{0, 2, 0}
	resp, err := client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "The requested version is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodeUnpublishVolumeMissingVolumeHandle(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeUnpublishVolumeRequest(fakeVolumeHandle(), fakeTargetPath)
	req.VolumeHandle = nil
	resp, err := client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume handle must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodeUnpublishVolumeMissingVolumeHandleId(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeUnpublishVolumeRequest(fakeVolumeHandle(), fakeTargetPath)
	req.VolumeHandle.Id = ""
	resp, err := client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The volume_handle.id field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodeUnpublishVolumeMissingTargetPath(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeUnpublishVolumeRequest(fakeVolumeHandle(), fakeTargetPath)
	req.TargetPath = ""
	resp, err := client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The target_path field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestGetNodeID_RemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testGetNodeIDRequest()
	req.Version = nil
	resp, err := client.GetNodeID(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNDEFINED
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "This service is running in 'remove volume group' mode."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestGetNodeIDMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetNodeIDRequest()
	req.Version = nil
	resp, err := client.GetNodeID(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The version field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestGetNodeIDUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetNodeIDRequest()
	req.Version = &csi.Version{0, 2, 0}
	resp, err := client.GetNodeID(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "The requested version is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestProbeNodeMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testProbeNodeRequest()
	req.Version = nil
	resp, err := client.ProbeNode(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The version field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestProbeNodeUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testProbeNodeRequest()
	req.Version = &csi.Version{0, 2, 0}
	resp, err := client.ProbeNode(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "The requested version is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodeGetCapabilitiesRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testNodeGetCapabilitiesRequest()
	req.Version = nil
	resp, err := client.NodeGetCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNDEFINED
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "This service is running in 'remove volume group' mode."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodeGetCapabilitiesRequestMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeGetCapabilitiesRequest()
	req.Version = nil
	resp, err := client.NodeGetCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	expdesc := "The version field must be specified."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestNodeGetCapabilitiesRequestUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeGetCapabilitiesRequest()
	req.Version = &csi.Version{0, 2, 0}
	resp, err := client.NodeGetCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	expdesc := "The requested version is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}
