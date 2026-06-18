package maple

import (
	"bytes"
	"strings"
	"testing"
)

func validPoolArgs() (chainID, protocolID int64, address []byte, name string, assetTokenID int64, isSyrup bool) {
	return 1, 7, bytes.Repeat([]byte{0xaa}, 20), "Syrup USDC", 42, true
}

func TestNewPool(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(p *Pool)
		wantErr string
	}{
		{name: "valid pool"},
		{
			name:    "zero chain ID",
			mutate:  func(p *Pool) { p.ChainID = 0 },
			wantErr: "chainID must be positive",
		},
		{
			name:    "negative chain ID",
			mutate:  func(p *Pool) { p.ChainID = -1 },
			wantErr: "chainID must be positive",
		},
		{
			name:    "zero protocol ID",
			mutate:  func(p *Pool) { p.ProtocolID = 0 },
			wantErr: "protocolID must be positive",
		},
		{
			name:    "short address",
			mutate:  func(p *Pool) { p.Address = []byte{0x01} },
			wantErr: "address must be 20 bytes",
		},
		{
			name:    "nil address",
			mutate:  func(p *Pool) { p.Address = nil },
			wantErr: "address must be 20 bytes",
		},
		{
			name:    "zero asset token ID",
			mutate:  func(p *Pool) { p.AssetTokenID = 0 },
			wantErr: "assetTokenID must be positive",
		},
		{
			name:    "negative asset token ID",
			mutate:  func(p *Pool) { p.AssetTokenID = -1 },
			wantErr: "assetTokenID must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chainID, protocolID, address, name, assetTokenID, isSyrup := validPoolArgs()
			p := &Pool{
				ChainID: chainID, ProtocolID: protocolID, Address: address, Name: name,
				AssetTokenID: assetTokenID, IsSyrup: isSyrup,
			}
			if tt.mutate != nil {
				tt.mutate(p)
			}
			err := p.Validate()
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestNewPool_Constructor(t *testing.T) {
	chainID, protocolID, address, name, assetTokenID, isSyrup := validPoolArgs()

	got, err := NewPool(chainID, protocolID, address, name, assetTokenID, isSyrup)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != name || got.AssetTokenID != assetTokenID || !got.IsSyrup {
		t.Errorf("fields not set: %+v", got)
	}

	if _, err := NewPool(0, protocolID, address, name, assetTokenID, isSyrup); err == nil {
		t.Fatal("expected constructor to propagate validation error")
	} else if !strings.Contains(err.Error(), "NewPool") {
		t.Errorf("error %q should be wrapped with constructor name", err.Error())
	}
}
