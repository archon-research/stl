package multicall

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestCall_Structure(t *testing.T) {
	call := Call{
		Target:       common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
		AllowFailure: true,
		CallData:     []byte{0x31, 0x3c, 0xe5, 0x67},
	}

	expectedTarget := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	if call.Target != expectedTarget {
		t.Errorf("Target = %v, want %v", call.Target, expectedTarget)
	}
	if !call.AllowFailure {
		t.Errorf("AllowFailure = false, want true")
	}
	if !bytes.Equal(call.CallData, []byte{0x31, 0x3c, 0xe5, 0x67}) {
		t.Errorf("CallData = %v, want [0x31 0x3c 0xe5 0x67]", call.CallData)
	}
}

func TestResult_Structure(t *testing.T) {
	result := Result{
		Success:    true,
		ReturnData: []byte{0x00, 0x12},
	}

	if !result.Success {
		t.Errorf("Success = false, want true")
	}
	if !bytes.Equal(result.ReturnData, []byte{0x00, 0x12}) {
		t.Errorf("ReturnData = %v, want [0x00 0x12]", result.ReturnData)
	}
}

func TestCall_AllowFailureFalse(t *testing.T) {
	call := Call{
		Target:       common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
		AllowFailure: false,
		CallData:     []byte{},
	}

	if call.AllowFailure {
		t.Errorf("AllowFailure = true, want false")
	}
}

func TestResult_FailureCase(t *testing.T) {
	result := Result{
		Success:    false,
		ReturnData: []byte{},
	}

	if result.Success {
		t.Errorf("Success = true, want false")
	}
	if len(result.ReturnData) != 0 {
		t.Errorf("ReturnData length = %d, want 0", len(result.ReturnData))
	}
}
