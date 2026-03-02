package leaktest

import "runtime"

// LeakGoroutine starts a goroutine permanently blocked on an unreachable
// channel. Once the channel reference goes out of scope the goroutine can
// never be unblocked, making it a textbook goroutine leak detectable by
// GOEXPERIMENT=goroutineleakprofile.
func LeakGoroutine() {
	ch := make(chan struct{})
	go func() { <-ch }()

	// Yield so the goroutine is scheduled and blocks on the channel
	// receive before this function returns and ch becomes unreachable.
	runtime.Gosched()
}
