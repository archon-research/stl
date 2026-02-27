package env

import (
	"fmt"
	"os"
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
