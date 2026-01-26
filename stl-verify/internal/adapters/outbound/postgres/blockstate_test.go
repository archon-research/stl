package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestIsSerializationFailure tests the isSerializationFailure helper function.
func TestIsSerializationFailure(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name: "serialization failure SQLSTATE 40001",
			err: &pgconn.PgError{
				Code:    "40001",
				Message: "could not serialize access due to concurrent update",
			},
			expected: true,
		},
		{
			name: "wrapped serialization failure",
			err: fmt.Errorf("failed to save block: %w", &pgconn.PgError{
				Code:    "40001",
				Message: "could not serialize access due to concurrent update",
			}),
			expected: true,
		},
		{
			name: "unique violation SQLSTATE 23505",
			err: &pgconn.PgError{
				Code:    "23505",
				Message: "duplicate key value violates unique constraint",
			},
			expected: false,
		},
		{
			name: "foreign key violation SQLSTATE 23503",
			err: &pgconn.PgError{
				Code:    "23503",
				Message: "foreign key violation",
			},
			expected: false,
		},
		{
			name: "deadlock detected SQLSTATE 40P01",
			err: &pgconn.PgError{
				Code:    "40P01",
				Message: "deadlock detected",
			},
			expected: false,
		},
		{
			name:     "generic error without pgconn.PgError",
			err:      errors.New("connection refused"),
			expected: false,
		},
		{
			name:     "wrapped generic error",
			err:      fmt.Errorf("database error: %w", errors.New("connection refused")),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isSerializationFailure(tt.err)
			if result != tt.expected {
				t.Errorf("isSerializationFailure(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}
