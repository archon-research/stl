package env

import (
	"log/slog"
	"strings"
)

// ParseLogLevel reads the LOG_LEVEL environment variable and returns the
// corresponding slog.Level. Supported values: "debug", "info", "warn", "error".
// Falls back to the provided default if the variable is empty or unrecognised.
func ParseLogLevel(fallback slog.Level) slog.Level {
	raw := Get("LOG_LEVEL", "")
	switch strings.ToLower(raw) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return fallback
	}
}
