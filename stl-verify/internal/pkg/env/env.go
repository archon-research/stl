package env

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Get returns the value of the environment variable or the default if not set.
func Get(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Require returns the value of the environment variable or an error if not set.
func Require(key string) (string, error) {
	if value := os.Getenv(key); value != "" {
		return value, nil
	}
	return "", fmt.Errorf("required env var %s is not set", key)
}

// GetInt returns the integer value of the environment variable or the default if
// unset. A set-but-unparseable value is returned as an error so misconfiguration
// is loud rather than silently falling back to the default.
func GetInt(key string, defaultValue int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return defaultValue, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parsing %s %q as int: %w", key, raw, err)
	}
	return v, nil
}

// GetDuration returns the duration value of the environment variable or the
// default if unset. The value is parsed via time.ParseDuration so callers can
// configure it as e.g. "5s", "250ms", "2m". A set-but-unparseable value is
// returned as an error.
func GetDuration(key string, defaultValue time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return defaultValue, nil
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parsing %s %q as duration: %w", key, raw, err)
	}
	return v, nil
}
