package call_service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/evolution-foundation/evolution-go/pkg/utils"
	whatsmeow_service "github.com/evolution-foundation/evolution-go/pkg/whatsmeow/service"
	"github.com/gomessguii/logger"
	"github.com/purpshell/meowcaller"
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
	maxQueuedRingCalls         = 10
)

var (
	ErrInvalidRingDuration  = errors.New("durationSeconds must be at most 60")
	ErrInvalidRingTarget    = errors.New("invalid call target")
	ErrInvalidRingAudioURL  = errors.New("audioUrl must be a public http(s) URL")
	ErrUnsupportedRingAudio = errors.New("audioUrl format not supported")
	ErrRingCallQueueFull    = errors.New("call queue is full for this instance")
	nonPublicAudioPrefixes  = []netip.Prefix{
		netip.MustParsePrefix("0.0.0.0/8"),
		netip.MustParsePrefix("100.64.0.0/10"),
		netip.MustParsePrefix("192.0.0.0/24"),
		netip.MustParsePrefix("192.0.2.0/24"),
		netip.MustParsePrefix("198.18.0.0/15"),
		netip.MustParsePrefix("198.51.100.0/24"),
		netip.MustParsePrefix("203.0.113.0/24"),
		netip.MustParsePrefix("240.0.0.0/4"),
		netip.MustParsePrefix("2001:db8::/32"),
	}
)

type callService struct {
	clientPointer           map[string]*whatsmeow.Client
	whatsmeowService        whatsmeow_service.WhatsmeowService
	loggerWrapper           *logger_wrapper.LoggerManager
	ensureClientConnectedFn func(instanceId string) (*whatsmeow.Client, error)
	openRingAudioSourceFn   func(string) (meowcaller.AudioSource, func(), error)
	placeRingCallFn         func(context.Context, string, string) (ringCallSession, error)
	ringQueuesMu            sync.Mutex
	ringQueues              map[string]*ringCallQueue
}

type ringCallJob struct {
	instanceID string
	target     string
	duration   time.Duration
	audioURL   string
}

type ringCallQueue struct {
	mu      sync.Mutex
	active  bool
	pending []ringCallJob
}

type ringCallPlayer interface {
	Stop()
}

type ringCallSession interface {
	Play(meowcaller.AudioSource, func()) ringCallPlayer
	OnReady(func())
	OnEnd(func(reason string))
	Hangup() error
}

type meowCallSessionAdapter struct {
	call *meowcaller.Call
}

func (c meowCallSessionAdapter) Play(source meowcaller.AudioSource, onFinish func()) ringCallPlayer {
	player := meowcaller.NewPlayer()
	player.OnFinish(onFinish)
	c.call.Subscribe(player)
	player.Play(source)
	return player
}

func (c meowCallSessionAdapter) OnReady(fn func()) {
	c.call.OnReady(fn)
}

func (c meowCallSessionAdapter) OnEnd(fn func(reason string)) {
	c.call.OnEnd(fn)
}

func (c meowCallSessionAdapter) Hangup() error {
	return c.call.Hangup()
}

type RejectCallStruct struct {
	CallCreator types.JID `json:"callCreator"`
	CallID      string    `json:"callId"`
}

type RingCallStruct struct {
	Number          string `json:"number"`
	DurationSeconds int    `json:"durationSeconds,omitempty"`
	AudioURL        string `json:"audioUrl,omitempty"`
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

func normalizeRingAudioURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", ErrInvalidRingAudioURL
	}
	if err := validateRingAudioDestination(parsed); err != nil {
		return "", ErrInvalidRingAudioURL
	}
	ext := strings.ToLower(filepath.Ext(parsed.Path))
	switch ext {
	case ".mp3", ".wav", ".ogg", ".opus":
		return raw, nil
	default:
		return "", ErrUnsupportedRingAudio
	}
}

func openRingAudioSource(rawURL string) (meowcaller.AudioSource, func(), error) {
	client := newRingAudioHTTPClient()
	defer client.CloseIdleConnections()
	return openRingAudioSourceWithClient(rawURL, client)
}

func validateRingAudioDestination(destination *url.URL) error {
	if destination == nil || destination.Host == "" {
		return ErrInvalidRingAudioURL
	}
	scheme := strings.ToLower(destination.Scheme)
	if scheme != "http" && scheme != "https" {
		return ErrInvalidRingAudioURL
	}
	host := destination.Hostname()
	if host == "" || strings.Contains(host, "%") {
		return ErrInvalidRingAudioURL
	}
	if ip := net.ParseIP(host); ip != nil && !isPublicRingAudioIP(ip) {
		return ErrInvalidRingAudioURL
	}
	return nil
}

func isPublicRingAudioIP(ip net.IP) bool {
	address, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	address = address.Unmap()
	if !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast() {
		return false
	}
	for _, prefix := range nonPublicAudioPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

type lookupRingAudioIPs func(context.Context, string) ([]net.IPAddr, error)

func newRingAudioHTTPClient() *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// A proxy would resolve the destination outside this process and bypass the
	// connection-time IP check below.
	transport.Proxy = nil
	transport.DialContext = publicRingAudioDialContext(dialer, net.DefaultResolver.LookupIPAddr)

	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			return validateRingAudioDestination(request.URL)
		},
	}
}

func publicRingAudioDialContext(dialer *net.Dialer, lookup lookupRingAudioIPs) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}

		var resolved []net.IPAddr
		if ip := net.ParseIP(host); ip != nil {
			resolved = []net.IPAddr{{IP: ip}}
		} else {
			resolved, err = lookup(ctx, host)
			if err != nil {
				return nil, err
			}
		}
		if len(resolved) == 0 {
			return nil, fmt.Errorf("audioUrl host %q resolved to no addresses", host)
		}
		for _, candidate := range resolved {
			if !isPublicRingAudioIP(candidate.IP) {
				return nil, fmt.Errorf("%w: host %q resolves to non-public address", ErrInvalidRingAudioURL, host)
			}
		}

		var lastErr error
		for _, candidate := range resolved {
			connection, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(candidate.IP.String(), port))
			if dialErr == nil {
				return connection, nil
			}
			lastErr = dialErr
		}
		return nil, lastErr
	}
}

func openRingAudioSourceWithClient(rawURL string, client *http.Client) (meowcaller.AudioSource, func(), error) {
	audioURL, err := normalizeRingAudioURL(rawURL)
	if err != nil {
		return nil, nil, err
	}
	if audioURL == "" {
		return nil, nil, nil
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, audioURL, nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		if len(body) == 0 {
			body = []byte(resp.Status)
		}
		return nil, nil, fmt.Errorf("failed to download audio: %s", strings.TrimSpace(string(body)))
	}

	ext := strings.ToLower(filepath.Ext(req.URL.Path))
	if ext == "" {
		return nil, nil, ErrUnsupportedRingAudio
	}

	tmp, err := os.CreateTemp("", "evolution-call-audio-*"+ext)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		_ = os.Remove(tmp.Name())
	}
	keepTempFile := false
	defer func() {
		_ = tmp.Close()
		if !keepTempFile {
			cleanup()
		}
	}()

	const maxAudioBytes = 20 << 20
	written, err := io.Copy(tmp, io.LimitReader(resp.Body, maxAudioBytes+1))
	if err != nil {
		return nil, nil, err
	}
	if written > maxAudioBytes {
		return nil, nil, fmt.Errorf("audioUrl exceeds %d bytes", maxAudioBytes)
	}
	if err := tmp.Close(); err != nil {
		return nil, nil, err
	}

	switch ext {
	case ".mp3":
		src, err := meowcaller.MP3File(tmp.Name())
		if err != nil {
			return nil, nil, err
		}
		keepTempFile = true
		return src, cleanup, nil
	case ".wav":
		src, err := meowcaller.WAVFile(tmp.Name())
		if err != nil {
			return nil, nil, err
		}
		keepTempFile = true
		return src, cleanup, nil
	case ".ogg", ".opus":
		src, err := meowcaller.OpusFile(tmp.Name())
		if err != nil {
			return nil, nil, err
		}
		keepTempFile = true
		return src, cleanup, nil
	default:
		return nil, nil, ErrUnsupportedRingAudio
	}
}

func (c *callService) ensureClientConnected(instanceId string) (*whatsmeow.Client, error) {
	if c.ensureClientConnectedFn != nil {
		return c.ensureClientConnectedFn(instanceId)
	}
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

func (c *callService) placeRingCall(ctx context.Context, instanceID, target string) (ringCallSession, error) {
	if c.placeRingCallFn != nil {
		return c.placeRingCallFn(ctx, instanceID, target)
	}

	callClient, ok := c.whatsmeowService.GetCallClient(instanceID)
	if !ok {
		return nil, fmt.Errorf("call client not available for instance %s", instanceID)
	}
	call, err := callClient.Call(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("failed to send call offer: %w", err)
	}
	if call == nil {
		return nil, errors.New("call session not available")
	}
	return meowCallSessionAdapter{call: call}, nil
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

func normalizeRingCallJob(data *RingCallStruct, instance *instance_model.Instance) (ringCallJob, error) {
	if data == nil || instance == nil {
		return ringCallJob{}, ErrInvalidRingTarget
	}

	duration, err := normalizeRingDurationSeconds(data.DurationSeconds)
	if err != nil {
		return ringCallJob{}, err
	}

	target, ok := utils.ParseJID(data.Number)
	if !ok || (target.Server != types.DefaultUserServer && target.Server != types.HiddenUserServer) {
		return ringCallJob{}, ErrInvalidRingTarget
	}
	target = utils.CanonicalJID(target)
	audioURL, err := normalizeRingAudioURL(data.AudioURL)
	if err != nil {
		return ringCallJob{}, err
	}

	return ringCallJob{
		instanceID: instance.Id,
		target:     target.String(),
		duration:   duration,
		audioURL:   audioURL,
	}, nil
}

func (c *callService) enqueueRingCall(job ringCallJob) error {
	c.ringQueuesMu.Lock()
	if c.ringQueues == nil {
		c.ringQueues = make(map[string]*ringCallQueue)
	}
	queue := c.ringQueues[job.instanceID]
	if queue == nil {
		queue = &ringCallQueue{}
		c.ringQueues[job.instanceID] = queue
	}
	c.ringQueuesMu.Unlock()

	queue.mu.Lock()
	if queue.active {
		if len(queue.pending) >= maxQueuedRingCalls {
			queue.mu.Unlock()
			return ErrRingCallQueueFull
		}
		queue.pending = append(queue.pending, job)
		queue.mu.Unlock()
		return nil
	}
	queue.active = true
	queue.mu.Unlock()

	go c.runRingCallQueue(queue, job)
	return nil
}

func (c *callService) runRingCallQueue(queue *ringCallQueue, job ringCallJob) {
	for {
		if err := c.executeRingCall(job); err != nil && c.loggerWrapper != nil {
			c.loggerWrapper.GetLogger(job.instanceID).LogError(
				"[%s] Queued outbound call failed: %v",
				job.instanceID,
				err,
			)
		}

		queue.mu.Lock()
		if len(queue.pending) == 0 {
			queue.active = false
			queue.mu.Unlock()
			return
		}
		job = queue.pending[0]
		queue.pending[0] = ringCallJob{}
		queue.pending = queue.pending[1:]
		queue.mu.Unlock()
	}
}

func (c *callService) executeRingCall(job ringCallJob) error {

	var audioSource meowcaller.AudioSource
	var audioCleanup func()
	if job.audioURL != "" {
		openAudioSource := openRingAudioSource
		if c.openRingAudioSourceFn != nil {
			openAudioSource = c.openRingAudioSourceFn
		}
		var err error
		audioSource, audioCleanup, err = openAudioSource(job.audioURL)
		if err != nil {
			return err
		}
	}
	cleanupAudio := func() {
		if audioSource != nil {
			_ = audioSource.Close()
		}
		if audioCleanup != nil {
			audioCleanup()
		}
	}

	if _, err := c.ensureClientConnected(job.instanceID); err != nil {
		cleanupAudio()
		return err
	}

	// WARNING: outbound calls to unknown contacts may trigger WhatsApp's
	// Reach-out Time-lock and can ban the connected number. Keep this endpoint
	// isolated from campaigns, workers and other automated production flows.
	call, err := c.placeRingCall(context.Background(), job.instanceID, job.target)
	if err != nil {
		cleanupAudio()
		return err
	}

	var hangupOnce sync.Once
	var cleanupOnce sync.Once
	var deadlineMu sync.Mutex
	var deadlineTimer *time.Timer
	deadlineStopped := false
	var playerMu sync.Mutex
	var activePlayer ringCallPlayer
	callStopped := false
	var completeOnce sync.Once
	done := make(chan struct{})
	complete := func() {
		completeOnce.Do(func() {
			close(done)
		})
	}
	stopDeadline := func() {
		deadlineMu.Lock()
		deadlineStopped = true
		if deadlineTimer != nil {
			deadlineTimer.Stop()
			deadlineTimer = nil
		}
		deadlineMu.Unlock()
	}
	stopPlayer := func() {
		playerMu.Lock()
		callStopped = true
		if activePlayer != nil {
			activePlayer.Stop()
			activePlayer = nil
		}
		playerMu.Unlock()
	}
	hangup := func(reason string) {
		hangupOnce.Do(func() {
			stopDeadline()
			stopPlayer()
			cleanupOnce.Do(cleanupAudio)
			if err := call.Hangup(); err != nil {
				if c.loggerWrapper != nil {
					c.loggerWrapper.GetLogger(job.instanceID).LogError("[%s] Failed to hang up outbound call (%s): %v", job.instanceID, reason, err)
				}
				complete()
				return
			}
			if c.loggerWrapper != nil {
				c.loggerWrapper.GetLogger(job.instanceID).LogInfo("[%s] Outbound call ended (%s)", job.instanceID, reason)
			}
			complete()
		})
	}

	call.OnEnd(func(_ string) {
		stopDeadline()
		stopPlayer()
		cleanupOnce.Do(cleanupAudio)
		complete()
	})
	call.OnReady(func() {
		if audioSource != nil {
			stopDeadline()
			playerMu.Lock()
			if callStopped {
				playerMu.Unlock()
				return
			}
			player := call.Play(audioSource, func() {
				hangup("audio playback finished")
			})
			if player == nil {
				playerMu.Unlock()
				go hangup("audio player unavailable")
				return
			}
			activePlayer = player
			playerMu.Unlock()
			return
		}
		go hangup("peer answered")
	})
	deadlineMu.Lock()
	if !deadlineStopped {
		deadlineTimer = time.AfterFunc(job.duration, func() {
			hangup("ring deadline reached")
		})
	}
	deadlineMu.Unlock()

	<-done
	return nil
}

func (c *callService) RingCall(data *RingCallStruct, instance *instance_model.Instance) error {
	job, err := normalizeRingCallJob(data, instance)
	if err != nil {
		return err
	}
	return c.enqueueRingCall(job)
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
