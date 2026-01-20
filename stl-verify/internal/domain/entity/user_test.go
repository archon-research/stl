package entity

import (
	"strings"
	"testing"
)

func TestNewUser(t *testing.T) {
	validAddr := make([]byte, 20)
	for i := range validAddr {
		validAddr[i] = byte(i)
	}

	tests := []struct {
		name           string
		id             int64
		chainID        int
		address        []byte
		firstSeenBlock int64
		wantErr        bool
		errContains    string
	}{
		{
			name:           "valid user",
			id:             1,
			chainID:        1,
			address:        validAddr,
			firstSeenBlock: 1000,
			wantErr:        false,
		},
		{
			name:           "zero id",
			id:             0,
			chainID:        1,
			address:        validAddr,
			firstSeenBlock: 1000,
			wantErr:        true,
			errContains:    "id must be positive",
		},
		{
			name:           "negative id",
			id:             -1,
			chainID:        1,
			address:        validAddr,
			firstSeenBlock: 1000,
			wantErr:        true,
			errContains:    "id must be positive",
		},
		{
			name:           "zero chainID",
			id:             1,
			chainID:        0,
			address:        validAddr,
			firstSeenBlock: 1000,
			wantErr:        true,
			errContains:    "chainID must be positive",
		},
		{
			name:           "invalid address length - too short",
			id:             1,
			chainID:        1,
			address:        make([]byte, 19),
			firstSeenBlock: 1000,
			wantErr:        true,
			errContains:    "invalid address length",
		},
		{
			name:           "invalid address length - too long",
			id:             1,
			chainID:        1,
			address:        make([]byte, 21),
			firstSeenBlock: 1000,
			wantErr:        true,
			errContains:    "invalid address length",
		},
		{
			name:           "zero firstSeenBlock",
			id:             1,
			chainID:        1,
			address:        validAddr,
			firstSeenBlock: 0,
			wantErr:        true,
			errContains:    "firstSeenBlock must be positive",
		},
		{
			name:           "negative firstSeenBlock",
			id:             1,
			chainID:        1,
			address:        validAddr,
			firstSeenBlock: -1,
			wantErr:        true,
			errContains:    "firstSeenBlock must be positive",
		},
		{
			name:           "valid user on polygon",
			id:             2,
			chainID:        137,
			address:        validAddr,
			firstSeenBlock: 5000,
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user, err := NewUser(tt.id, tt.chainID, tt.address, tt.firstSeenBlock)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewUser() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("NewUser() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("NewUser() unexpected error = %v", err)
				return
			}
			if user == nil {
				t.Errorf("NewUser() returned nil")
				return
			}
			if user.ID != tt.id {
				t.Errorf("NewUser() ID = %v, want %v", user.ID, tt.id)
			}
			if user.ChainID != tt.chainID {
				t.Errorf("NewUser() ChainID = %v, want %v", user.ChainID, tt.chainID)
			}
			if user.FirstSeenBlock != tt.firstSeenBlock {
				t.Errorf("NewUser() FirstSeenBlock = %v, want %v", user.FirstSeenBlock, tt.firstSeenBlock)
			}
			if user.Metadata == nil {
				t.Errorf("NewUser() Metadata is nil")
			}
		})
	}
}

func TestUser_AddressHex(t *testing.T) {
	addr := make([]byte, 20)
	for i := range addr {
		addr[i] = byte(i)
	}

	user := &User{Address: addr}
	hex := user.AddressHex()
	if hex == "" {
		t.Errorf("AddressHex() returned empty string")
	}
	if hex[:2] != "0x" {
		t.Errorf("AddressHex() should start with 0x, got %v", hex)
	}
}
