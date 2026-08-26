package call_handler

import (
	"errors"
	"net/http"

	call_service "github.com/evolution-foundation/evolution-go/pkg/call/service"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	"github.com/gin-gonic/gin"
)

type CallHandler interface {
	RejectCall(ctx *gin.Context)
	RingCall(ctx *gin.Context)
}

type callHandler struct {
	callService call_service.CallService
}

// Reject call
// @Summary Reject call
// @Description Reject call
// @Tags Call
// @Accept json
// @Produce json
// @Param message body call_service.RejectCallStruct true "Call data"
// @Success 200 {object} gin.H "success"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /call/reject [post]
func (g *callHandler) RejectCall(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *call_service.RejectCallStruct
	err := ctx.ShouldBindBodyWithJSON(&data)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err = g.callService.RejectCall(data, instance)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success"})
}

// Ring call
// @Summary Ring call
// @Description Place an experimental outbound voice call, optionally play a recorded audio after answer, and hang up automatically
// @Tags Call
// @Accept json
// @Produce json
// @Param message body call_service.RingCallStruct true "Call data"
// @Success 200 {object} gin.H "queued"
// @Failure 400 {object} gin.H "Invalid input"
// @Failure 429 {object} gin.H "Call queue full for this instance"
// @Failure 500 {object} gin.H "Internal server error"
// @Router /call/ring [post]
func (g *callHandler) RingCall(ctx *gin.Context) {
	getInstance := ctx.MustGet("instance")

	instance, ok := getInstance.(*instance_model.Instance)
	if !ok {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "instance not found"})
		return
	}

	var data *call_service.RingCallStruct
	if err := ctx.ShouldBindBodyWithJSON(&data); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := g.callService.RingCall(data, instance); err != nil {
		if errors.Is(err, call_service.ErrRingCallQueueFull) {
			ctx.JSON(http.StatusTooManyRequests, gin.H{"error": err.Error()})
			return
		}
		if errors.Is(err, call_service.ErrInvalidRingDuration) ||
			errors.Is(err, call_service.ErrInvalidRingTarget) ||
			errors.Is(err, call_service.ErrInvalidRingAudioURL) ||
			errors.Is(err, call_service.ErrUnsupportedRingAudio) {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "queued"})
}

func NewCallHandler(
	callService call_service.CallService,
) CallHandler {
	return &callHandler{
		callService: callService,
	}
}
