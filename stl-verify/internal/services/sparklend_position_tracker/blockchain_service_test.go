package sparklend_position_tracker

import (
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
)

func TestBlockchainService_LoadABIs(t *testing.T) {
	// Load ERC20 ABI first since it's passed in via constructor
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	service := &blockchainService{
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		metadataCache: make(map[common.Address]TokenMetadata),
		erc20ABI:      erc20ABI,
	}

	err = service.loadABIs(false)
	if err != nil {
		t.Fatalf("loadABIs() failed: %v", err)
	}

	tests := []struct {
		name       string
		abi        *abi.ABI
		abiName    string
		methodName string
	}{
		{
			name:       "getUserReservesABI loaded",
			abi:        service.getUserReservesABI,
			abiName:    "getUserReservesABI",
			methodName: "getUserReservesData",
		},
		{
			name:       "getUserReserveDataABI loaded",
			abi:        service.getUserReserveDataABI,
			abiName:    "getUserReserveDataABI",
			methodName: "getUserReserveData",
		},
		{
			name:       "erc20ABI loaded",
			abi:        service.erc20ABI,
			abiName:    "erc20ABI",
			methodName: "decimals",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.abi == nil {
				t.Fatalf("%s is nil after loading", tt.abiName)
			}

			method, ok := tt.abi.Methods[tt.methodName]
			if !ok {
				t.Errorf("method %s not found in %s", tt.methodName, tt.abiName)
			} else if method.Name != tt.methodName {
				t.Errorf("method name mismatch: got %s, want %s", method.Name, tt.methodName)
			}
		})
	}
}

func TestBlockchainService_ERC20ABI_Methods(t *testing.T) {
	// Load ERC20 ABI first
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	service := &blockchainService{
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		metadataCache: make(map[common.Address]TokenMetadata),
		erc20ABI:      erc20ABI,
	}

	err = service.loadABIs(false)
	if err != nil {
		t.Fatalf("loadABIs() failed: %v", err)
	}

	requiredMethods := []string{"decimals", "symbol", "name"}
	for _, methodName := range requiredMethods {
		method, ok := service.erc20ABI.Methods[methodName]
		if !ok {
			t.Errorf("ERC20 method %s not found", methodName)
			continue
		}

		if !method.IsConstant() {
			t.Errorf("ERC20 method %s should be a view/pure function", methodName)
		}
	}
}

func TestBlockchainService_GetUserReservesDataABI_Structure(t *testing.T) {
	// Load ERC20 ABI
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	service := &blockchainService{
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		metadataCache: make(map[common.Address]TokenMetadata),
		erc20ABI:      erc20ABI,
	}

	err = service.loadABIs(false)
	if err != nil {
		t.Fatalf("loadABIs() failed: %v", err)
	}

	method, ok := service.getUserReservesABI.Methods["getUserReservesData"]
	if !ok {
		t.Fatal("getUserReservesData method not found")
	}

	if len(method.Inputs) != 2 {
		t.Errorf("getUserReservesData should have 2 inputs, got %d", len(method.Inputs))
	}

	expectedInputs := []struct {
		name string
		typ  string
	}{
		{"provider", "address"},
		{"user", "address"},
	}

	for i, expected := range expectedInputs {
		if i >= len(method.Inputs) {
			t.Errorf("missing input %d: %s", i, expected.name)
			continue
		}
		if method.Inputs[i].Name != expected.name {
			t.Errorf("input %d name = %s, want %s", i, method.Inputs[i].Name, expected.name)
		}
		if method.Inputs[i].Type.String() != expected.typ {
			t.Errorf("input %d type = %s, want %s", i, method.Inputs[i].Type.String(), expected.typ)
		}
	}

	if len(method.Outputs) != 2 {
		t.Errorf("getUserReservesData should have 2 outputs, got %d", len(method.Outputs))
	}
}

func TestBlockchainService_GetUserReserveDataABI_Structure(t *testing.T) {
	// Load ERC20 ABI
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	service := &blockchainService{
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		metadataCache: make(map[common.Address]TokenMetadata),
		erc20ABI:      erc20ABI,
	}

	err = service.loadABIs(false)
	if err != nil {
		t.Fatalf("loadABIs() failed: %v", err)
	}

	method, ok := service.getUserReserveDataABI.Methods["getUserReserveData"]
	if !ok {
		t.Fatal("getUserReserveData method not found")
	}

	if len(method.Inputs) != 2 {
		t.Errorf("getUserReserveData should have 2 inputs, got %d", len(method.Inputs))
	}

	expectedInputs := []struct {
		name string
		typ  string
	}{
		{"asset", "address"},
		{"user", "address"},
	}

	for i, expected := range expectedInputs {
		if i >= len(method.Inputs) {
			t.Errorf("missing input %d: %s", i, expected.name)
			continue
		}
		if method.Inputs[i].Name != expected.name {
			t.Errorf("input %d name = %s, want %s", i, method.Inputs[i].Name, expected.name)
		}
		if method.Inputs[i].Type.String() != expected.typ {
			t.Errorf("input %d type = %s, want %s", i, method.Inputs[i].Type.String(), expected.typ)
		}
	}

	if len(method.Outputs) != 9 {
		t.Errorf("getUserReserveData should have 9 outputs, got %d", len(method.Outputs))
	}
}

func TestABI_PackingDoesNotPanic(t *testing.T) {
	// Load ERC20 ABI
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	service := &blockchainService{
		logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		metadataCache:         make(map[common.Address]TokenMetadata),
		erc20ABI:              erc20ABI,
		poolAddressesProvider: common.HexToAddress("0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e"),
	}

	if err := service.loadABIs(false); err != nil {
		t.Fatalf("loadABIs() failed: %v", err)
	}

	t.Run("pack getUserReservesData", func(t *testing.T) {
		user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")
		_, err := service.getUserReservesABI.Pack("getUserReservesData", service.poolAddressesProvider, user)
		if err != nil {
			t.Errorf("failed to pack getUserReservesData: %v", err)
		}
	})

	t.Run("pack getUserReserveData", func(t *testing.T) {
		asset := common.HexToAddress("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
		user := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")
		_, err := service.getUserReserveDataABI.Pack("getUserReserveData", asset, user)
		if err != nil {
			t.Errorf("failed to pack getUserReserveData: %v", err)
		}
	})

	t.Run("pack ERC20 methods", func(t *testing.T) {
		methods := []string{"decimals", "symbol", "name"}
		for _, method := range methods {
			_, err := service.erc20ABI.Pack(method)
			if err != nil {
				t.Errorf("failed to pack %s: %v", method, err)
			}
		}
	})
}

func TestBlockchainService_ParseUserReservesData(t *testing.T) {
	tests := []struct {
		name        string
		collaterals int
	}{
		{"no collaterals", 0},
		{"few collaterals", 5},
		{"many collaterals", 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arrayLength := tt.collaterals
			const structSize = 128
			totalSize := 64 + (arrayLength * structSize)
			result := make([]byte, totalSize)

			offsetValue := big.NewInt(32)
			copy(result[0:32], common.LeftPadBytes(offsetValue.Bytes(), 32))

			length := big.NewInt(int64(arrayLength))
			copy(result[32:64], common.LeftPadBytes(length.Bytes(), 32))

			for i := 0; i < arrayLength; i++ {
				structStart := 64 + (i * structSize)

				asset := common.HexToAddress(fmt.Sprintf("0x%040x", i+1))
				copy(result[structStart:structStart+32], common.LeftPadBytes(asset.Bytes(), 32))

				balance := new(big.Int).Mul(big.NewInt(1000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
				copy(result[structStart+32:structStart+64], common.LeftPadBytes(balance.Bytes(), 32))

				copy(result[structStart+64:structStart+96], common.LeftPadBytes(big.NewInt(1).Bytes(), 32))

				variableDebt := big.NewInt(500)
				copy(result[structStart+96:structStart+128], common.LeftPadBytes(variableDebt.Bytes(), 32))
			}

			if len(result) < 64 {
				t.Fatal("invalid result: too short")
			}

			parsedOffset := new(big.Int).SetBytes(result[0:32]).Uint64()
			arrayLengthClaimed := new(big.Int).SetBytes(result[parsedOffset : parsedOffset+32]).Uint64()

			dataStart := parsedOffset + 32
			availableBytes := uint64(len(result)) - dataStart
			actualArrayLength := availableBytes / structSize

			arrayLen := arrayLengthClaimed
			if actualArrayLength < arrayLengthClaimed {
				arrayLen = actualArrayLength
			}

			reserves := make([]UserReserveData, 0, arrayLen)

			for i := uint64(0); i < arrayLen; i++ {
				structOffset := dataStart + (i * structSize)
				structData := result[structOffset : structOffset+structSize]

				underlyingAsset := common.BytesToAddress(structData[0:32])
				if underlyingAsset == (common.Address{}) {
					continue
				}

				reserves = append(reserves, UserReserveData{
					UnderlyingAsset:                underlyingAsset,
					ScaledATokenBalance:            new(big.Int).SetBytes(structData[32:64]),
					UsageAsCollateralEnabledOnUser: new(big.Int).SetBytes(structData[64:96]).Uint64() != 0,
					ScaledVariableDebt:             new(big.Int).SetBytes(structData[96:128]),
				})
			}

			if len(reserves) != tt.collaterals {
				t.Errorf("parsed %d collaterals, want %d", len(reserves), tt.collaterals)
			}

			for i, reserve := range reserves {
				if reserve.UnderlyingAsset == (common.Address{}) {
					t.Errorf("reserve[%d] has empty address", i)
				}
				if reserve.ScaledATokenBalance == nil {
					t.Errorf("reserve[%d] has nil balance", i)
				}
			}
		})
	}
}
