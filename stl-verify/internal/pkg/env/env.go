package env

import (
	"log/slog"
	"os"
)

// Get returns the value of the environment variable or the default if not set.
func Get(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Require returns the value of the environment variable or exits if not set.
func Require(key string, logger *slog.Logger) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	logger.Error("required env var missing", "key", key)
	os.Exit(1)
	return "" // unreachable
}
