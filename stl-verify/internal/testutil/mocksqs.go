package testutil

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// MockSQSServer tracks calls made to a fake SQS HTTP endpoint.
type MockSQSServer struct {
	mu                sync.Mutex
	messages          []string // queued message bodies (JSON strings)
	ReceiveCallCount  int
	DeleteCallCount   int
	FirstCallReceived chan struct{}
}

// AddMessage enqueues a raw JSON message body to be delivered on the next
// ReceiveMessage call. Messages are delivered one per call, FIFO.
func (s *MockSQSServer) AddMessage(body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, body)
}

// Receives returns the current ReceiveMessage call count.
func (s *MockSQSServer) Receives() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ReceiveCallCount
}

// Deletes returns the current DeleteMessage call count.
func (s *MockSQSServer) Deletes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.DeleteCallCount
}

// StartMockSQS starts an httptest.Server that speaks the AWS SQS JSON 1.0
// protocol. Enqueue messages with AddMessage before or during the test;
// once the queue is drained, ReceiveMessage returns empty responses.
func StartMockSQS(t *testing.T) (*httptest.Server, *MockSQSServer) {
	t.Helper()

	state := &MockSQSServer{
		FirstCallReceived: make(chan struct{}, 1),
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		target := r.Header.Get("X-Amz-Target")

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")

		switch {
		case strings.Contains(target, "ReceiveMessage"):
			state.mu.Lock()
			state.ReceiveCallCount++
			var body string
			if len(state.messages) > 0 {
				body = state.messages[0]
				state.messages = state.messages[1:]
			}
			state.mu.Unlock()

			select {
			case state.FirstCallReceived <- struct{}{}:
			default:
			}

			if body != "" {
				marshaled, _ := json.Marshal(body)
				fmt.Fprintf(w, `{"Messages":[{"MessageId":"msg-1","ReceiptHandle":"handle-1","Body":%s}]}`, string(marshaled))
			} else {
				fmt.Fprint(w, `{"Messages":[]}`)
			}

		case strings.Contains(target, "DeleteMessage"):
			state.mu.Lock()
			state.DeleteCallCount++
			state.mu.Unlock()
			fmt.Fprint(w, `{}`)

		default:
			fmt.Fprint(w, `{}`)
		}
	}))

	return server, state
}
