package entity

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestNewProtocolEvent(t *testing.T) {
	validEventData := json.RawMessage(`{"user":"0x1234","amount":"1000"}`)
	validTxHash := []byte{0x01, 0x02, 0x03}
	validContractAddr := []byte{0xaa, 0xbb, 0xcc}

	tests := []struct {
		name            string
		chainID         int
		protocolID      int64
		blockNumber     int64
		blockVersion    int
		txHash          []byte
		logIndex        int
		contractAddress []byte
		eventName       string
		eventData       json.RawMessage
		wantErr         bool
		errContains     string
	}{
		{
			name:            "valid event",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         false,
		},
		{
			name:            "zero chainID",
			chainID:         0,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "chainID must be positive",
		},
		{
			name:            "zero protocolID",
			chainID:         1,
			protocolID:      0,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "protocolID must be positive",
		},
		{
			name:            "zero blockNumber",
			chainID:         1,
			protocolID:      1,
			blockNumber:     0,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "blockNumber must be positive",
		},
		{
			name:            "negative blockVersion",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    -1,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "blockVersion must be non-negative",
		},
		{
			name:            "empty txHash",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          nil,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "txHash must not be empty",
		},
		{
			name:            "negative logIndex",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        -1,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "logIndex must be non-negative",
		},
		{
			name:            "empty contractAddress",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: nil,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "contractAddress must not be empty",
		},
		{
			name:            "empty eventName",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "eventName must not be empty",
		},
		{
			name:            "nil eventData",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       nil,
			wantErr:         true,
			errContains:     "eventData must not be empty",
		},
		{
			name:            "invalid JSON eventData",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       json.RawMessage(`not valid json`),
			wantErr:         true,
			errContains:     "eventData must be valid JSON",
		},
		{
			name:            "logIndex zero is valid",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Supply",
			eventData:       validEventData,
			wantErr:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := NewProtocolEvent(tt.chainID, tt.protocolID, tt.blockNumber, tt.blockVersion, tt.txHash, tt.logIndex, tt.contractAddress, tt.eventName, tt.eventData)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewProtocolEvent() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("NewProtocolEvent() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("NewProtocolEvent() unexpected error = %v", err)
				return
			}
			if event == nil {
				t.Errorf("NewProtocolEvent() returned nil")
				return
			}
			if event.ChainID != tt.chainID {
				t.Errorf("ChainID = %v, want %v", event.ChainID, tt.chainID)
			}
			if event.ProtocolID != tt.protocolID {
				t.Errorf("ProtocolID = %v, want %v", event.ProtocolID, tt.protocolID)
			}
			if event.BlockNumber != tt.blockNumber {
				t.Errorf("BlockNumber = %v, want %v", event.BlockNumber, tt.blockNumber)
			}
			if event.BlockVersion != tt.blockVersion {
				t.Errorf("BlockVersion = %v, want %v", event.BlockVersion, tt.blockVersion)
			}
			if !bytes.Equal(event.TxHash, tt.txHash) {
				t.Errorf("TxHash = %v, want %v", event.TxHash, tt.txHash)
			}
			if event.LogIndex != tt.logIndex {
				t.Errorf("LogIndex = %v, want %v", event.LogIndex, tt.logIndex)
			}
			if !bytes.Equal(event.ContractAddress, tt.contractAddress) {
				t.Errorf("ContractAddress = %v, want %v", event.ContractAddress, tt.contractAddress)
			}
			if event.EventName != tt.eventName {
				t.Errorf("EventName = %v, want %v", event.EventName, tt.eventName)
			}
			if string(event.EventData) != string(tt.eventData) {
				t.Errorf("EventData = %v, want %v", string(event.EventData), string(tt.eventData))
			}
		})
	}
}
