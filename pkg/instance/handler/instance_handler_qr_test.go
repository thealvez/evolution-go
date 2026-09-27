package instance_handler

import (
	"encoding/json"
	"testing"

	instance_service "github.com/evolution-foundation/evolution-go/pkg/instance/service"
)

func TestQrSuccessResponsePreservesNativeContractAndAddsAliases(t *testing.T) {
	t.Parallel()

	const (
		image = "data:image/png;base64,c3ludGhldGljLXFy"
		code  = "synthetic-pairing-code"
	)

	response := qrSuccessResponse(&instance_service.QrcodeStruct{
		Qrcode:         image,
		Code:           code,
		PasskeyStage:   "required",
		PasskeyOpenURL: "https://example.invalid/passkey",
		PasskeyCode:    "synthetic-passkey",
	})

	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("marshal QR response: %v", err)
	}

	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatalf("unmarshal QR response: %v", err)
	}

	data, ok := body["data"].(map[string]any)
	if !ok {
		t.Fatalf("data must be an object, got %T", body["data"])
	}

	for _, key := range []string{"qrcode", "Qrcode", "base64", "qrCode", "qr_code_base64"} {
		if got := data[key]; got != image {
			t.Errorf("data.%s = %v, want synthetic image", key, got)
		}
	}
	if got := data["code"]; got != code {
		t.Errorf("data.code = %v, want %q", got, code)
	}
	if got := data["Code"]; got != code {
		t.Errorf("data.Code = %v, want legacy %q", got, code)
	}
	for _, key := range []string{"base64", "qrCode", "qr_code_base64"} {
		if got := body[key]; got != image {
			t.Errorf("%s = %v, want synthetic image", key, got)
		}
	}
	if got := body["code"]; got != code {
		t.Errorf("code = %v, want %q", got, code)
	}

	legacyObject, ok := body["qrcode"].(map[string]any)
	if !ok {
		t.Fatalf("qrcode compatibility alias must be an object, got %T", body["qrcode"])
	}
	if legacyObject["base64"] != image || legacyObject["qrcode"] != image || legacyObject["code"] != code {
		t.Errorf("qrcode compatibility object does not mirror the native values: %#v", legacyObject)
	}

	if data["passkeyStage"] != "required" || data["passkeyOpenUrl"] != "https://example.invalid/passkey" || data["passkeyCode"] != "synthetic-passkey" {
		t.Errorf("passkey fields were not preserved: %#v", data)
	}
}
