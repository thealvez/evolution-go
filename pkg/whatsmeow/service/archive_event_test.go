package whatsmeow_service

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func TestArchiveEventDataBuildsWebhookPayloadWithoutTypeAssertion(t *testing.T) {
	timestamp := time.Date(2026, time.September, 28, 12, 0, 0, 0, time.UTC)
	event := &events.Archive{
		JID:          types.NewJID("5511999999999", types.DefaultUserServer),
		Timestamp:    timestamp,
		FromFullSync: true,
	}

	data := archiveEventData(event)

	if got := data["JID"]; got != event.JID {
		t.Fatalf("expected JID %v, got %v", event.JID, got)
	}
	if got := data["Timestamp"]; got != timestamp {
		t.Fatalf("expected timestamp %v, got %v", timestamp, got)
	}
	if got := data["Action"]; got != event.Action {
		t.Fatalf("expected action %v, got %v", event.Action, got)
	}
	if got := data["FromFullSync"]; got != true {
		t.Fatalf("expected FromFullSync true, got %v", got)
	}
}
