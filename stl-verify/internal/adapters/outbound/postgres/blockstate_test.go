package postgres

import (
	"errors"
	"testing"
)

// TestIsUniqueViolation tests the isUniqueViolation helper function.
func TestIsUniqueViolation(t *testing.T) {
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
			name:     "unique constraint error with 23505",
			err:      errors.New("ERROR: duplicate key value violates unique constraint \"unique_block_number_version\" (SQLSTATE 23505)"),
			expected: true,
		},
		{
			name:     "unique constraint error with text",
			err:      errors.New("failed to save block: unique constraint violation on hash"),
			expected: true,
		},
		{
			name:     "other database error",
			err:      errors.New("connection refused"),
			expected: false,
		},
		{
			name:     "wrapped error with 23505",
			err:      errors.New("failed to insert: SQLSTATE 23505"),
			expected: true,
		},
		{
			name:     "foreign key violation 23503",
			err:      errors.New("SQLSTATE 23503"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isUniqueViolation(tt.err)
			if result != tt.expected {
				t.Errorf("isUniqueViolation(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}
