package main

import (
	"context"
	"errors"
)

// A SIGTERM arrives as a cancelled context and is how a drain or a deliberate stop reaches this
// process, so it exits clean: counted as a failure it would consume a restart budget, and two node
// drains in a long run would leave one real attempt.
func exitCode(err error) int {
	if err == nil || errors.Is(err, context.Canceled) {
		return 0
	}
	return 1
}
