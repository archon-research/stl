package entity

import (
	"strings"
	"testing"
)

func TestNewUserProtocolMetadata(t *testing.T) {
	tests := []struct {
		name        string
		id          int64
		userID      int64
		protocolID  int64
		wantErr     bool
		errContains string
	}{
		{
			name:       "valid user protocol metadata",
			id:         1,
			userID:     10,
			protocolID: 5,
			wantErr:    false,
		},
		{
			name:        "zero id",
			id:          0,
			userID:      10,
			protocolID:  5,
			wantErr:     true,
			errContains: "id must be positive",
		},
		{
			name:        "negative id",
			id:          -1,
			userID:      10,
			protocolID:  5,
			wantErr:     true,
			errContains: "id must be positive",
		},
		{
			name:        "zero userID",
			id:          1,
			userID:      0,
			protocolID:  5,
			wantErr:     true,
			errContains: "userID must be positive",
		},
		{
			name:        "negative userID",
			id:          1,
			userID:      -1,
			protocolID:  5,
			wantErr:     true,
			errContains: "userID must be positive",
		},
		{
			name:        "zero protocolID",
			id:          1,
			userID:      10,
			protocolID:  0,
			wantErr:     true,
			errContains: "protocolID must be positive",
		},
		{
			name:        "negative protocolID",
			id:          1,
			userID:      10,
			protocolID:  -1,
			wantErr:     true,
			errContains: "protocolID must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upm, err := NewUserProtocolMetadata(tt.id, tt.userID, tt.protocolID)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewUserProtocolMetadata() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("NewUserProtocolMetadata() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("NewUserProtocolMetadata() unexpected error = %v", err)
				return
			}
			if upm == nil {
				t.Errorf("NewUserProtocolMetadata() returned nil")
				return
			}
			if upm.ID != tt.id {
				t.Errorf("NewUserProtocolMetadata() ID = %v, want %v", upm.ID, tt.id)
			}
			if upm.UserID != tt.userID {
				t.Errorf("NewUserProtocolMetadata() UserID = %v, want %v", upm.UserID, tt.userID)
			}
			if upm.ProtocolID != tt.protocolID {
				t.Errorf("NewUserProtocolMetadata() ProtocolID = %v, want %v", upm.ProtocolID, tt.protocolID)
			}
			if upm.Metadata == nil {
				t.Errorf("NewUserProtocolMetadata() Metadata is nil")
			}
		})
	}
}

func TestUserProtocolMetadata_SetMetadata(t *testing.T) {
	upm, err := NewUserProtocolMetadata(1, 10, 5)
	if err != nil {
		t.Fatalf("NewUserProtocolMetadata() unexpected error = %v", err)
	}

	upm.SetMetadata("key1", "value1")
	upm.SetMetadata("key2", 42)
	upm.SetMetadata("key3", true)

	if val, ok := upm.Metadata["key1"]; !ok || val != "value1" {
		t.Errorf("SetMetadata() failed to set key1")
	}
	if val, ok := upm.Metadata["key2"]; !ok || val != 42 {
		t.Errorf("SetMetadata() failed to set key2")
	}
	if val, ok := upm.Metadata["key3"]; !ok || val != true {
		t.Errorf("SetMetadata() failed to set key3")
	}
}

func TestUserProtocolMetadata_GetMetadata(t *testing.T) {
	upm, err := NewUserProtocolMetadata(1, 10, 5)
	if err != nil {
		t.Fatalf("NewUserProtocolMetadata() unexpected error = %v", err)
	}

	upm.SetMetadata("existingKey", "existingValue")

	val, ok := upm.GetMetadata("existingKey")
	if !ok {
		t.Errorf("GetMetadata() failed to get existing key")
	}
	if val != "existingValue" {
		t.Errorf("GetMetadata() returned wrong value: got %v, want %v", val, "existingValue")
	}

	val, ok = upm.GetMetadata("nonExistentKey")
	if ok {
		t.Errorf("GetMetadata() should return false for non-existent key")
	}
	if val != nil {
		t.Errorf("GetMetadata() should return nil for non-existent key")
	}
}

func TestUserProtocolMetadata_SetMetadata_NilMap(t *testing.T) {
	// Test that SetMetadata initializes nil map
	upm := &UserProtocolMetadata{
		ID:         1,
		UserID:     10,
		ProtocolID: 5,
		Metadata:   nil,
	}

	upm.SetMetadata("key", "value")

	if upm.Metadata == nil {
		t.Errorf("SetMetadata() should initialize nil Metadata map")
	}
	if val, ok := upm.Metadata["key"]; !ok || val != "value" {
		t.Errorf("SetMetadata() failed to set value in initialized map")
	}
}

func TestUserProtocolMetadata_GetMetadata_NilMap(t *testing.T) {
	// Test that GetMetadata handles nil map
	upm := &UserProtocolMetadata{
		ID:         1,
		UserID:     10,
		ProtocolID: 5,
		Metadata:   nil,
	}

	val, ok := upm.GetMetadata("key")
	if ok {
		t.Errorf("GetMetadata() should return false for nil Metadata map")
	}
	if val != nil {
		t.Errorf("GetMetadata() should return nil for nil Metadata map")
	}
}
