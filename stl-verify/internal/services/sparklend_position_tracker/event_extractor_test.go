package sparklend_position_tracker

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestPositionEventData_ToJSON(t *testing.T) {
	tests := []struct {
		name     string
		event    *PositionEventData
		wantKeys []string
		wantErr  bool
	}{
		{
			name: "borrow event",
			event: &PositionEventData{
				EventType: EventBorrow,
				User:      common.HexToAddress("0x1234"),
				Reserve:   common.HexToAddress("0x5678"),
				Amount:    big.NewInt(1000000),
			},
			wantKeys: []string{"user", "reserve", "amount"},
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
			wantKeys: []string{"user", "liquidator", "collateralAsset", "debtAsset", "debtToCover", "liquidatedCollateralAmount"},
		},
		{
			name: "collateral enabled event",
			event: &PositionEventData{
				EventType:         EventReserveUsedAsCollateralEnabled,
				User:              common.HexToAddress("0x1234"),
				Reserve:           common.HexToAddress("0x5678"),
				CollateralEnabled: true,
			},
			wantKeys: []string{"user", "reserve", "collateralEnabled"},
		},
		{
			name: "collateral disabled event",
			event: &PositionEventData{
				EventType:         EventReserveUsedAsCollateralDisabled,
				User:              common.HexToAddress("0x1234"),
				Reserve:           common.HexToAddress("0x5678"),
				CollateralEnabled: false,
			},
			wantKeys: []string{"user", "reserve", "collateralEnabled"},
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
		})
	}
}
