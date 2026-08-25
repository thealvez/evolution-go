package call_handler

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	call_service "github.com/evolution-foundation/evolution-go/pkg/call/service"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	"github.com/gin-gonic/gin"
)

type fakeCallService struct {
	ringErr  error
	ringData *call_service.RingCallStruct
}

func (f *fakeCallService) RejectCall(*call_service.RejectCallStruct, *instance_model.Instance) error {
	return nil
}

func (f *fakeCallService) RingCall(data *call_service.RingCallStruct, _ *instance_model.Instance) error {
	f.ringData = data
	return f.ringErr
}

func TestRingCallHTTPContract(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name       string
		body       string
		serviceErr error
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"number":"5511999999999@s.whatsapp.net","durationSeconds":10}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid duration",
			body:       `{"number":"5511999999999@s.whatsapp.net","durationSeconds":61}`,
			serviceErr: call_service.ErrInvalidRingDuration,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid target",
			body:       `{"number":""}`,
			serviceErr: call_service.ErrInvalidRingTarget,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "protocol failure",
			body:       `{"number":"5511999999999@s.whatsapp.net"}`,
			serviceErr: errors.New("offer failed"),
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "malformed JSON",
			body:       `{"number":`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeCallService{ringErr: tt.serviceErr}
			handler := &callHandler{callService: fake}
			response := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(response)
			ctx.Request = httptest.NewRequest(http.MethodPost, "/call/ring", strings.NewReader(tt.body))
			ctx.Request.Header.Set("Content-Type", "application/json")
			ctx.Set("instance", &instance_model.Instance{Id: "test-instance"})

			handler.RingCall(ctx)

			if response.Code != tt.wantStatus {
				t.Fatalf("RingCall() status = %d, want %d; body=%s", response.Code, tt.wantStatus, response.Body.String())
			}
			if tt.wantStatus == http.StatusOK {
				if fake.ringData == nil || fake.ringData.DurationSeconds != 10 {
					t.Fatalf("RingCall() data = %#v, want durationSeconds=10", fake.ringData)
				}
				if response.Body.String() != `{"message":"success"}` {
					t.Fatalf("RingCall() body = %s, want success response", response.Body.String())
				}
			}
		})
	}
}
