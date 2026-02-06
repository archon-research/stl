package sparklend_position_tracker

import (
	"encoding/json"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestPositionEventData_ToJSON(t *testing.T) {
	tests := []struct {
		name       string
		event      *PositionEventData
		wantKeys   []string
		wantValues map[string]interface{}
		wantErr    bool
	}{
		{
			name: "borrow event",
			event: &PositionEventData{
				EventType: EventBorrow,
				User:      common.HexToAddress("0x1234"),
				Reserve:   common.HexToAddress("0x5678"),
				Amount:    big.NewInt(1000000),
			},
			wantKeys: []string{"eventType", "user", "reserve", "amount"},
			wantValues: map[string]interface{}{
				"eventType": "Borrow",
				"user":      common.HexToAddress("0x1234").Hex(),
				"reserve":   common.HexToAddress("0x5678").Hex(),
				"amount":    "1000000",
			},
		},
		{
			name: "liquidation event",
			event: &PositionEventData{
				EventType:                  EventLiquidationCall,
				User:                       common.HexToAddress("0x1234"),
				Liquidator:                 common.HexToAddress("0xabcd"),
				CollateralAsset:            common.HexToAddress("0x5678"),
				DebtAsset:                  common.HexToAddress("0x9abc"),
				DebtToCover:                big.NewInt(500),
				LiquidatedCollateralAmount: big.NewInt(600),
			},
			wantKeys: []string{"eventType", "user", "liquidator", "collateralAsset", "debtAsset", "debtToCover", "liquidatedCollateralAmount"},
			wantValues: map[string]interface{}{
				"eventType":                  "LiquidationCall",
				"debtToCover":                "500",
				"liquidatedCollateralAmount": "600",
			},
		},
		{
			name: "collateral enabled event",
			event: &PositionEventData{
				EventType:         EventReserveUsedAsCollateralEnabled,
				User:              common.HexToAddress("0x1234"),
				Reserve:           common.HexToAddress("0x5678"),
				CollateralEnabled: true,
			},
			wantKeys: []string{"eventType", "user", "reserve", "collateralEnabled"},
			wantValues: map[string]interface{}{
				"eventType":         "ReserveUsedAsCollateralEnabled",
				"collateralEnabled": true,
			},
		},
		{
			name: "collateral disabled event",
			event: &PositionEventData{
				EventType:         EventReserveUsedAsCollateralDisabled,
				User:              common.HexToAddress("0x1234"),
				Reserve:           common.HexToAddress("0x5678"),
				CollateralEnabled: false,
			},
			wantKeys: []string{"eventType", "user", "reserve", "collateralEnabled"},
			wantValues: map[string]interface{}{
				"eventType":         "ReserveUsedAsCollateralDisabled",
				"collateralEnabled": false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := tt.event.ToJSON()
			if (err != nil) != tt.wantErr {
				t.Fatalf("ToJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			var result map[string]interface{}
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatalf("failed to unmarshal result: %v", err)
			}

			for _, key := range tt.wantKeys {
				if _, ok := result[key]; !ok {
					t.Errorf("expected key %q in JSON output", key)
				}
			}

			for key, wantVal := range tt.wantValues {
				gotVal, ok := result[key]
				if !ok {
					t.Errorf("expected key %q in JSON output", key)
					continue
				}
				// JSON unmarshals booleans as bool, numbers as float64, strings as string
				switch want := wantVal.(type) {
				case bool:
					got, ok := gotVal.(bool)
					if !ok || got != want {
						t.Errorf("key %q = %v (%T), want %v", key, gotVal, gotVal, want)
					}
				default:
					if fmt.Sprintf("%v", gotVal) != fmt.Sprintf("%v", want) {
						t.Errorf("key %q = %v, want %v", key, gotVal, want)
					}
				}
			}
		})
	}
}
