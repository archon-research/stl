// Package env provides utilities for working with environment variables.
package env

import "os"

// Get returns the value of the environment variable or the default if not set.
func Get(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
