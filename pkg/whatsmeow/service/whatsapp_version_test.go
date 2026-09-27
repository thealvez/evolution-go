package whatsmeow_service

import (
	"testing"

	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"
	"google.golang.org/protobuf/proto"
)

func TestApplyWhatsAppVersionUpdatesHandshakeAndDeviceProps(t *testing.T) {
	original := store.GetWAVersion()
	var originalDeviceVersion *waCompanionReg.DeviceProps_AppVersion
	if store.DeviceProps.Version != nil {
		originalDeviceVersion = proto.Clone(store.DeviceProps.Version).(*waCompanionReg.DeviceProps_AppVersion)
	}
	t.Cleanup(func() {
		whatsappVersionMu.Lock()
		defer whatsappVersionMu.Unlock()
		store.SetWAVersion(original)
		store.DeviceProps.Version = originalDeviceVersion
	})

	want := clientVersion{Major: 2, Minor: 3000, Patch: 1048579041}
	got := applyWhatsAppVersion(want)

	if got != (store.WAVersionContainer{2, 3000, 1048579041}) {
		t.Fatalf("unexpected handshake version: got %s", got.String())
	}
	if store.GetWAVersion() != got {
		t.Fatalf("global handshake version was not updated: got %s", store.GetWAVersion().String())
	}
	if deviceVersion := store.DeviceProps.GetVersion(); deviceVersion == nil || deviceVersion.GetPrimary() != got[0] || deviceVersion.GetSecondary() != got[1] || deviceVersion.GetTertiary() != got[2] {
		t.Fatalf("device props version was not synchronized with handshake version: %+v", deviceVersion)
	}
}
