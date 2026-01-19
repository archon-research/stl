package entity

import "testing"

func TestNewProtocol(t *testing.T) {
	validAddr := make([]byte, 20)
	for i := range validAddr {
		validAddr[i] = byte(i)
	}

	tests := []struct {
		name           string
		id             int64
		chainID        int
		address        []byte
		protocolName   string
		protocolType   string
		createdAtBlock int64
		wantErr        bool
		errContains    string
	}{
		{
			name:           "valid protocol",
			id:             1,
			chainID:        1,
			address:        validAddr,
			protocolName:   "SparkLend",
			protocolType:   "lending",
			createdAtBlock: 1000,
			wantErr:        false,
		},
		{
			name:           "zero id",
			id:             0,
			chainID:        1,
			address:        validAddr,
			protocolName:   "SparkLend",
			protocolType:   "lending",
			createdAtBlock: 1000,
			wantErr:        true,
			errContains:    "id must be positive",
		},
		{
			name:           "negative id",
			id:             -1,
			chainID:        1,
			address:        validAddr,
			protocolName:   "SparkLend",
			protocolType:   "lending",
			createdAtBlock: 1000,
			wantErr:        true,
			errContains:    "id must be positive",
		},
		{
			name:           "zero chainID",
			id:             1,
			chainID:        0,
			address:        validAddr,
			protocolName:   "SparkLend",
			protocolType:   "lending",
			createdAtBlock: 1000,
			wantErr:        true,
			errContains:    "chainID must be positive",
		},
		{
			name:           "invalid address length - too short",
			id:             1,
			chainID:        1,
			address:        make([]byte, 19),
			protocolName:   "SparkLend",
			protocolType:   "lending",
			createdAtBlock: 1000,
			wantErr:        true,
			errContains:    "invalid address length",
		},
		{
			name:           "invalid address length - too long",
			id:             1,
			chainID:        1,
			address:        make([]byte, 21),
			protocolName:   "SparkLend",
			protocolType:   "lending",
			createdAtBlock: 1000,
			wantErr:        true,
			errContains:    "invalid address length",
		},
		{
			name:           "empty name",
			id:             1,
			chainID:        1,
			address:        validAddr,
			protocolName:   "",
			protocolType:   "lending",
			createdAtBlock: 1000,
			wantErr:        true,
			errContains:    "name must not be empty",
		},
		{
			name:           "empty protocolType",
			id:             1,
			chainID:        1,
			address:        validAddr,
			protocolName:   "SparkLend",
			protocolType:   "",
			createdAtBlock: 1000,
			wantErr:        true,
			errContains:    "protocolType must not be empty",
		},
		{
			name:           "zero createdAtBlock",
			id:             1,
			chainID:        1,
			address:        validAddr,
			protocolName:   "SparkLend",
			protocolType:   "lending",
			createdAtBlock: 0,
			wantErr:        true,
			errContains:    "createdAtBlock must be positive",
		},
		{
			name:           "valid RWA protocol",
			id:             2,
			chainID:        1,
			address:        validAddr,
			protocolName:   "MakerDAO",
			protocolType:   "rwa",
			createdAtBlock: 5000,
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protocol, err := NewProtocol(tt.id, tt.chainID, tt.address, tt.protocolName, tt.protocolType, tt.createdAtBlock)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewProtocol() expected error, got nil")
					return
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("NewProtocol() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("NewProtocol() unexpected error = %v", err)
				return
			}
			if protocol == nil {
				t.Errorf("NewProtocol() returned nil")
				return
			}
			if protocol.ID != tt.id {
				t.Errorf("NewProtocol() ID = %v, want %v", protocol.ID, tt.id)
			}
			if protocol.ChainID != tt.chainID {
				t.Errorf("NewProtocol() ChainID = %v, want %v", protocol.ChainID, tt.chainID)
			}
			if protocol.Name != tt.protocolName {
				t.Errorf("NewProtocol() Name = %v, want %v", protocol.Name, tt.protocolName)
			}
			if protocol.ProtocolType != tt.protocolType {
				t.Errorf("NewProtocol() ProtocolType = %v, want %v", protocol.ProtocolType, tt.protocolType)
			}
			if protocol.CreatedAtBlock != tt.createdAtBlock {
				t.Errorf("NewProtocol() CreatedAtBlock = %v, want %v", protocol.CreatedAtBlock, tt.createdAtBlock)
			}
			if protocol.Metadata == nil {
				t.Errorf("NewProtocol() Metadata is nil")
			}
		})
	}
}

func TestProtocol_AddressHex(t *testing.T) {
	addr := make([]byte, 20)
	for i := range addr {
		addr[i] = byte(i)
	}

	protocol := &Protocol{Address: addr}
	hex := protocol.AddressHex()
	if hex == "" {
		t.Errorf("AddressHex() returned empty string")
	}
	if hex[:2] != "0x" {
		t.Errorf("AddressHex() should start with 0x, got %v", hex)
	}
}
