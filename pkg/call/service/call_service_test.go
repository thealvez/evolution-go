package call_service

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/evolution-foundation/evolution-go/pkg/config"
	instance_model "github.com/evolution-foundation/evolution-go/pkg/instance/model"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type fakeRingAudioSource struct {
	mu     sync.Mutex
	closed bool
}

func (s *fakeRingAudioSource) ReadFrame() ([]float32, error) {
	return nil, io.EOF
}

func (s *fakeRingAudioSource) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func (s *fakeRingAudioSource) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

type fakeCallPlayer struct {
	mu       sync.Mutex
	onFinish func()
	source   meowcaller.AudioSource
}

func (p *fakeCallPlayer) Stop() {
	p.mu.Lock()
	source := p.source
	p.source = nil
	p.mu.Unlock()
	if source != nil {
		_ = source.Close()
	}
}

func (p *fakeCallPlayer) finish() {
	p.mu.Lock()
	source := p.source
	p.source = nil
	fn := p.onFinish
	p.mu.Unlock()
	if source != nil {
		_ = source.Close()
	}
	if fn != nil {
		fn()
	}
}

type fakeCallSession struct {
	mu              sync.Mutex
	onReady         func()
	onEnd           func(string)
	player          *fakeCallPlayer
	readyOnRegister bool
	hangups         int
	hungUp          chan struct{}
}

func newFakeCallSession(readyOnRegister bool) *fakeCallSession {
	return &fakeCallSession{
		readyOnRegister: readyOnRegister,
		hungUp:          make(chan struct{}, 1),
	}
}

func (s *fakeCallSession) Play(source meowcaller.AudioSource, onFinish func()) ringCallPlayer {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.player = &fakeCallPlayer{source: source, onFinish: onFinish}
	return s.player
}

func (s *fakeCallSession) OnReady(fn func()) {
	s.mu.Lock()
	s.onReady = fn
	readyOnRegister := s.readyOnRegister
	s.mu.Unlock()
	if readyOnRegister {
		fn()
	}
}

func (s *fakeCallSession) OnEnd(fn func(reason string)) {
	s.mu.Lock()
	s.onEnd = fn
	s.mu.Unlock()
}

func (s *fakeCallSession) Hangup() error {
	s.mu.Lock()
	s.hangups++
	onEnd := s.onEnd
	s.mu.Unlock()
	if onEnd != nil {
		onEnd("hangup")
	}
	select {
	case s.hungUp <- struct{}{}:
	default:
	}
	return nil
}

func (s *fakeCallSession) currentPlayer() *fakeCallPlayer {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.player
}

func (s *fakeCallSession) hangupCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hangups
}

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

func TestNormalizeRingAudioURL(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr error
	}{
		{name: "absent"},
		{name: "mp3", raw: " https://cdn.example.com/audio/greeting.mp3?version=2 ", want: "https://cdn.example.com/audio/greeting.mp3?version=2"},
		{name: "wav", raw: "http://cdn.example.com/audio/greeting.WAV", want: "http://cdn.example.com/audio/greeting.WAV"},
		{name: "relative URL", raw: "/audio/greeting.mp3", wantErr: ErrInvalidRingAudioURL},
		{name: "unsupported scheme", raw: "file:///tmp/greeting.mp3", wantErr: ErrInvalidRingAudioURL},
		{name: "loopback IPv4", raw: "http://127.0.0.1/greeting.mp3", wantErr: ErrInvalidRingAudioURL},
		{name: "loopback IPv6", raw: "http://[::1]/greeting.mp3", wantErr: ErrInvalidRingAudioURL},
		{name: "private network", raw: "https://10.0.0.8/greeting.mp3", wantErr: ErrInvalidRingAudioURL},
		{name: "metadata address", raw: "http://169.254.169.254/greeting.mp3", wantErr: ErrInvalidRingAudioURL},
		{name: "public IP", raw: "https://8.8.8.8/greeting.mp3", want: "https://8.8.8.8/greeting.mp3"},
		{name: "unsupported format", raw: "https://cdn.example.com/audio/greeting.aac", wantErr: ErrUnsupportedRingAudio},
		{name: "missing format", raw: "https://cdn.example.com/audio/greeting", wantErr: ErrUnsupportedRingAudio},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeRingAudioURL(tt.raw)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("normalizeRingAudioURL() error = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("normalizeRingAudioURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPublicRingAudioDialRejectsMixedDNSResults(t *testing.T) {
	dial := publicRingAudioDialContext(&net.Dialer{}, func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{
			{IP: net.ParseIP("8.8.8.8")},
			{IP: net.ParseIP("10.0.0.8")},
		}, nil
	})

	connection, err := dial(context.Background(), "tcp", "cdn.example.com:443")
	if connection != nil {
		_ = connection.Close()
		t.Fatal("publicRingAudioDialContext() unexpectedly opened a connection")
	}
	if !errors.Is(err, ErrInvalidRingAudioURL) {
		t.Fatalf("publicRingAudioDialContext() error = %v, want %v", err, ErrInvalidRingAudioURL)
	}
}

func TestRingAudioHTTPClientRejectsPrivateRedirect(t *testing.T) {
	redirectURL, err := url.Parse("http://169.254.169.254/greeting.mp3")
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}

	err = newRingAudioHTTPClient().CheckRedirect(&http.Request{URL: redirectURL}, nil)
	if !errors.Is(err, ErrInvalidRingAudioURL) {
		t.Fatalf("CheckRedirect() error = %v, want %v", err, ErrInvalidRingAudioURL)
	}
}

func TestOpenRingAudioSourceDownloadsWAV(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/greeting.wav" {
			t.Fatalf("audio request path = %q, want /greeting.wav", request.URL.Path)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"audio/wav"}},
			Body:       io.NopCloser(bytes.NewReader(testWAV())),
			Request:    request,
		}, nil
	})}

	source, cleanup, err := openRingAudioSourceWithClient("https://cdn.example.com/greeting.wav", client)
	if err != nil {
		t.Fatalf("openRingAudioSource() error = %v", err)
	}
	if source == nil || cleanup == nil {
		t.Fatal("openRingAudioSource() did not return both source and cleanup")
	}
	if _, err := source.ReadFrame(); err != nil {
		t.Fatalf("downloaded WAV ReadFrame() error = %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatalf("downloaded WAV Close() error = %v", err)
	}
	cleanup()
}

func TestRingCallPlaysAudioAndHangsUpAfterPlayback(t *testing.T) {
	session := newFakeCallSession(true)
	audioSource := &fakeRingAudioSource{}
	cleanupCalls := 0
	placedTarget := ""
	service := &callService{
		loggerWrapper: logger_wrapper.NewLoggerManager(&config.Config{
			LogDirectory: t.TempDir(),
		}),
		ensureClientConnectedFn: func(string) (*whatsmeow.Client, error) {
			return nil, nil
		},
		openRingAudioSourceFn: func(rawURL string) (meowcaller.AudioSource, func(), error) {
			if rawURL != "https://cdn.example.com/greeting.wav" {
				t.Fatalf("audio URL = %q, want configured URL", rawURL)
			}
			return audioSource, func() { cleanupCalls++ }, nil
		},
		placeRingCallFn: func(_ context.Context, instanceID, target string) (ringCallSession, error) {
			if instanceID != "test-instance" {
				t.Fatalf("call instance = %q, want test-instance", instanceID)
			}
			placedTarget = target
			return session, nil
		},
	}

	err := service.RingCall(
		&RingCallStruct{
			Number:          "5511999999999@s.whatsapp.net",
			DurationSeconds: 1,
			AudioURL:        "https://cdn.example.com/greeting.wav",
		},
		&instance_model.Instance{Id: "test-instance"},
	)
	if err != nil {
		t.Fatalf("RingCall() error = %v", err)
	}
	if placedTarget != "5511999999999@s.whatsapp.net" {
		t.Fatalf("Call() target = %q, want canonical target", placedTarget)
	}
	player := session.currentPlayer()
	if player == nil {
		t.Fatal("RingCall() did not start audio playback after ready")
	}

	// OnReady fired while its callback was being registered. The ring deadline
	// must not be installed afterward and interrupt active playback.
	select {
	case <-session.hungUp:
		t.Fatal("RingCall() hung up before audio playback finished")
	case <-time.After(1100 * time.Millisecond):
	}

	player.finish()
	select {
	case <-session.hungUp:
	case <-time.After(time.Second):
		t.Fatal("RingCall() did not hang up after audio playback finished")
	}
	if got := session.hangupCount(); got != 1 {
		t.Fatalf("Hangup() calls = %d, want 1", got)
	}
	if cleanupCalls != 1 {
		t.Fatalf("audio cleanup calls = %d, want 1", cleanupCalls)
	}
	if !audioSource.isClosed() {
		t.Fatal("audio source was not closed")
	}
}

func testWAV() []byte {
	const sampleBytes = 2
	wav := make([]byte, 44+sampleBytes)
	copy(wav[0:4], "RIFF")
	binary.LittleEndian.PutUint32(wav[4:8], uint32(len(wav)-8))
	copy(wav[8:12], "WAVE")
	copy(wav[12:16], "fmt ")
	binary.LittleEndian.PutUint32(wav[16:20], 16)
	binary.LittleEndian.PutUint16(wav[20:22], 1)
	binary.LittleEndian.PutUint16(wav[22:24], 1)
	binary.LittleEndian.PutUint32(wav[24:28], 16000)
	binary.LittleEndian.PutUint32(wav[28:32], 32000)
	binary.LittleEndian.PutUint16(wav[32:34], 2)
	binary.LittleEndian.PutUint16(wav[34:36], 16)
	copy(wav[36:40], "data")
	binary.LittleEndian.PutUint32(wav[40:44], sampleBytes)
	return wav
}
