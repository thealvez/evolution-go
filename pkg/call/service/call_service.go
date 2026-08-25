package call_service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/evolution-foundation/evolution-go/pkg/utils"
	whatsmeow_service "github.com/evolution-foundation/evolution-go/pkg/whatsmeow/service"
	"github.com/gomessguii/logger"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

type CallService interface {
	RejectCall(data *RejectCallStruct, instance *instance_model.Instance) error
	RingCall(data *RingCallStruct, instance *instance_model.Instance) error
}

const (
	defaultRingDurationSeconds = 10
	maxRingDurationSeconds     = 60
)

var (
	ErrInvalidRingDuration = errors.New("durationSeconds must be at most 60")
	ErrInvalidRingTarget   = errors.New("invalid call target")
)

type callService struct {
	clientPointer    map[string]*whatsmeow.Client
	whatsmeowService whatsmeow_service.WhatsmeowService
	loggerWrapper    *logger_wrapper.LoggerManager
}

type RejectCallStruct struct {
	CallCreator types.JID `json:"callCreator"`
	CallID      string    `json:"callId"`
}

type RingCallStruct struct {
	Number          string `json:"number"`
	DurationSeconds int    `json:"durationSeconds,omitempty"`
}

func normalizeRingDurationSeconds(seconds int) (time.Duration, error) {
	if seconds <= 0 {
		seconds = defaultRingDurationSeconds
	}
	if seconds > maxRingDurationSeconds {
		return 0, ErrInvalidRingDuration
	}
	return time.Duration(seconds) * time.Second, nil
}

func (c *callService) ensureClientConnected(instanceId string) (*whatsmeow.Client, error) {
	client := c.clientPointer[instanceId]
	c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Checking client connection status - Client exists: %v", instanceId, client != nil)

	if client == nil {
		c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] No client found, attempting to start new instance", instanceId)
		err := c.whatsmeowService.StartInstance(instanceId)
		if err != nil {
			c.loggerWrapper.GetLogger(instanceId).LogError("[%s] Failed to start instance: %v", instanceId, err)
			return nil, errors.New("no active session found")
		}

		c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Instance started, waiting 2 seconds...", instanceId)
		time.Sleep(2 * time.Second)

		client = c.clientPointer[instanceId]
		c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Checking new client - Exists: %v, Connected: %v",
			instanceId,
			client != nil,
			client != nil && client.IsConnected())

		if client == nil || !client.IsConnected() {
			c.loggerWrapper.GetLogger(instanceId).LogError("[%s] New client validation failed - Exists: %v, Connected: %v",
				instanceId,
				client != nil,
				client != nil && client.IsConnected())
			return nil, errors.New("no active session found")
		}
	} else if !client.IsConnected() {
		c.loggerWrapper.GetLogger(instanceId).LogError("[%s] Existing client is disconnected - Connected status: %v",
			instanceId,
			client.IsConnected())
		return nil, errors.New("client disconnected")
	}

	c.loggerWrapper.GetLogger(instanceId).LogInfo("[%s] Client successfully validated - Connected: %v", instanceId, client.IsConnected())
	return client, nil
}

func (c *callService) RejectCall(data *RejectCallStruct, instance *instance_model.Instance) error {
	client, err := c.ensureClientConnected(instance.Id)
	if err != nil {
		return err
	}

	err = client.RejectCall(context.Background(), data.CallCreator, data.CallID)
	if err != nil {
		logger.LogError("[%s] error reject call: %v", instance.Id, err)
		return err
	}

	return nil
}

func (c *callService) RingCall(data *RingCallStruct, instance *instance_model.Instance) error {
	if data == nil || instance == nil {
		return ErrInvalidRingTarget
	}

	duration, err := normalizeRingDurationSeconds(data.DurationSeconds)
	if err != nil {
		return err
	}

	target, ok := utils.ParseJID(data.Number)
	if !ok || (target.Server != types.DefaultUserServer && target.Server != types.HiddenUserServer) {
		return ErrInvalidRingTarget
	}
	target = utils.CanonicalJID(target)

	if _, err = c.ensureClientConnected(instance.Id); err != nil {
		return err
	}

	callClient, ok := c.whatsmeowService.GetCallClient(instance.Id)
	if !ok {
		return fmt.Errorf("call client not available for instance %s", instance.Id)
	}

	// WARNING: outbound calls to unknown contacts may trigger WhatsApp's
	// Reach-out Time-lock and can ban the connected number. Keep this endpoint
	// isolated from campaigns, workers and other automated production flows.
	call, err := callClient.Call(context.Background(), target.String())
	if err != nil {
		return fmt.Errorf("failed to send call offer: %w", err)
	}

	var hangupOnce sync.Once
	var deadlineMu sync.Mutex
	var deadlineTimer *time.Timer
	callEnded := false
	hangup := func(reason string) {
		hangupOnce.Do(func() {
			if err := call.Hangup(); err != nil {
				c.loggerWrapper.GetLogger(instance.Id).LogError("[%s] Failed to hang up outbound call (%s): %v", instance.Id, reason, err)
				return
			}
			c.loggerWrapper.GetLogger(instance.Id).LogInfo("[%s] Outbound call ended (%s)", instance.Id, reason)
		})
	}

	call.OnEnd(func(_ string) {
		deadlineMu.Lock()
		callEnded = true
		if deadlineTimer != nil {
			deadlineTimer.Stop()
		}
		deadlineMu.Unlock()
	})
	call.OnReady(func() {
		go hangup("peer answered")
	})
	deadlineMu.Lock()
	if !callEnded {
		deadlineTimer = time.AfterFunc(duration, func() {
			hangup("ring deadline reached")
		})
	}
	deadlineMu.Unlock()

	return nil
}

func NewCallService(
	clientPointer map[string]*whatsmeow.Client,
	whatsmeowService whatsmeow_service.WhatsmeowService,
	loggerWrapper *logger_wrapper.LoggerManager,
) CallService {
	return &callService{
		clientPointer:    clientPointer,
		whatsmeowService: whatsmeowService,
		loggerWrapper:    loggerWrapper,
	}
}
