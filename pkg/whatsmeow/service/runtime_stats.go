package whatsmeow_service

import (
	"runtime"
	"time"

	"github.com/gomessguii/logger"
)

// RuntimeStatsInterval is how often LogRuntimeStats reports (Story 72.7, AC6).
const RuntimeStatsInterval = 5 * time.Minute

// formatRuntimeStats renders one runtime stats line. The line is grep-friendly
// ("[runtime-stats]") because it is the metric of the post-deploy check:
// active StartClient loops must stay <= active instances and goroutines must
// not grow with reconnects.
func formatRuntimeStats(registry *ClientRegistry) (string, []interface{}) {
	clients, sessions, loops := registry.Stats()
	return "[runtime-stats] goroutines=%d startclient_loops=%d sessions=%d clients=%d",
		[]interface{}{runtime.NumGoroutine(), loops, sessions, clients}
}

// LogRuntimeStats logs runtime.NumGoroutine() and the number of active
// StartClient loops to stdout every interval until stop is closed (a nil stop
// runs for the life of the process).
func LogRuntimeStats(registry *ClientRegistry, interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	format, args := formatRuntimeStats(registry)
	logger.LogInfo(format, args...)
	for {
		select {
		case <-ticker.C:
			format, args := formatRuntimeStats(registry)
			logger.LogInfo(format, args...)
		case <-stop:
			return
		}
	}
}
