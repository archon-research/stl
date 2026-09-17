package main

import (
	"context"
	"errors"
)

// exitCode treats a cancelled context as a clean stop: a drain counted as a failure would consume a
// restart budget, and two node drains in a long run would leave one real attempt.
func exitCode(err error) int {
	if err == nil || errors.Is(err, context.Canceled) {
		return 0
	}
	return 1
}
