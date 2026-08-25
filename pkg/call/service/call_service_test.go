package call_service

import (
	"errors"
	"testing"
	"time"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
)

func TestNormalizeRingDurationSeconds(t *testing.T) {
	tests := []struct {
		name    string
		seconds int
		want    time.Duration
		wantErr error
	}{
		{name: "absent", seconds: 0, want: 10 * time.Second},
		{name: "negative uses default", seconds: -3, want: 10 * time.Second},
		{name: "one second", seconds: 1, want: time.Second},
		{name: "maximum", seconds: 60, want: 60 * time.Second},
		{name: "above maximum", seconds: 61, wantErr: ErrInvalidRingDuration},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeRingDurationSeconds(tt.seconds)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("normalizeRingDurationSeconds() error = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("normalizeRingDurationSeconds() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRingCallRejectsInvalidTargetBeforeConnecting(t *testing.T) {
	service := &callService{}

	err := service.RingCall(
		&RingCallStruct{Number: "123456789012345678@g.us"},
		&instance_model.Instance{Id: "test-instance"},
	)
	if !errors.Is(err, ErrInvalidRingTarget) {
		t.Fatalf("RingCall() error = %v, want %v", err, ErrInvalidRingTarget)
	}
}
