package maple

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

type expectedActiveLoan struct {
		loanID             common.Address
		borrower           common.Address
		state              string
		principalOwed      *big.Int
		acmRatio           *big.Int
		collateralAsset    string
		collateralAmount   *big.Int
		collateralValue    *big.Int
		collateralDecimals int
		collateralState    string
		collateralCustody  string
		liquidationLevel   *big.Int
		poolAddress        common.Address
		poolName           string
		poolAssetSymbol    string
		poolAssetDecimals  int
	}

func assertBigInt(t *testing.T, label string, got, want *big.Int) {
	t.Helper()
	if want == nil {
		return
	}
	if got == nil {
		t.Errorf("%s = <nil>, want %s", label, want)
		return
	}
	if got.Cmp(want) != 0 {
		t.Errorf("%s = %s, want %s", label, got, want)
	}
}

func assertLoan(t *testing.T, idx int, want expectedActiveLoan, got outbound.MapleActiveLoan) {
	t.Helper()
	if got.LoanID != want.loanID {
		t.Errorf("loans[%d].LoanID = %s, want %s", idx, got.LoanID.Hex(), want.loanID.Hex())
	}
	if got.Borrower != want.borrower {
		t.Errorf("loans[%d].Borrower = %s, want %s", idx, got.Borrower.Hex(), want.borrower.Hex())
	}
	if got.State != want.state {
		t.Errorf("loans[%d].State = %q, want %q", idx, got.State, want.state)
	}

	assertBigInt(t, fmt.Sprintf("loans[%d].PrincipalOwed", idx), got.PrincipalOwed, want.principalOwed)
	assertBigInt(t, fmt.Sprintf("loans[%d].AcmRatio", idx), got.AcmRatio, want.acmRatio)

	if got.Collateral.Asset != want.collateralAsset {
		t.Errorf("loans[%d].Collateral.Asset = %q, want %q", idx, got.Collateral.Asset, want.collateralAsset)
	}
	assertBigInt(t, fmt.Sprintf("loans[%d].Collateral.AssetAmount", idx), got.Collateral.AssetAmount, want.collateralAmount)
	assertBigInt(t, fmt.Sprintf("loans[%d].Collateral.AssetValueUSD", idx), got.Collateral.AssetValueUSD, want.collateralValue)
	if got.Collateral.Decimals != want.collateralDecimals {
		t.Errorf("loans[%d].Collateral.Decimals = %d, want %d", idx, got.Collateral.Decimals, want.collateralDecimals)
	}
	if got.Collateral.State != want.collateralState {
		t.Errorf("loans[%d].Collateral.State = %q, want %q", idx, got.Collateral.State, want.collateralState)
	}
	if got.Collateral.Custodian != want.collateralCustody {
		t.Errorf("loans[%d].Collateral.Custodian = %q, want %q", idx, got.Collateral.Custodian, want.collateralCustody)
	}
	assertBigInt(t, fmt.Sprintf("loans[%d].Collateral.LiquidationLevel", idx), got.Collateral.LiquidationLevel, want.liquidationLevel)

	if got.PoolAddress != want.poolAddress {
		t.Errorf("loans[%d].PoolAddress = %s, want %s", idx, got.PoolAddress.Hex(), want.poolAddress.Hex())
	}
	if got.PoolName != want.poolName {
		t.Errorf("loans[%d].PoolName = %q, want %q", idx, got.PoolName, want.poolName)
	}
	if got.PoolAssetSymbol != want.poolAssetSymbol {
		t.Errorf("loans[%d].PoolAssetSymbol = %q, want %q", idx, got.PoolAssetSymbol, want.poolAssetSymbol)
	}
	if got.PoolAssetDecimals != want.poolAssetDecimals {
		t.Errorf("loans[%d].PoolAssetDecimals = %d, want %d", idx, got.PoolAssetDecimals, want.poolAssetDecimals)
	}
}

func TestGetAllActiveLoansAtBlock(t *testing.T) {
	tests := []struct {
		name             string
		blockNumber      uint64
		serverResponse   any
		serverStatus     int
		wantLoans        []expectedActiveLoan
		wantErr          bool
		errContains      string
		checkBlockNumber bool
	}{
		{
			name:        "success: multiple loans with pool info",
			blockNumber: 21500000,
			serverResponse: graphqlResponse{
				Data: json.RawMessage(`{
					"openTermLoans": [
						{
							"id": "0x0430665a6e0ce5141b1cc42607d79632125cbbac",
							"borrower": {"id": "0xc24b928b8f28ec560200bd46bfb84c1b7ae8f4a5"},
							"state": "Active",
							"principalOwed": "2000000000000",
							"acmRatio": "1698389",
							"collateral": {
								"asset": "BTC",
								"assetAmount": "5000000000",
								"assetValueUsd": "6793556500000",
								"decimals": 8,
								"state": "Deposited",
								"custodian": "FORDEFI",
								"liquidationLevel": "1176471"
							},
							"fundingPool": {
								"id": "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b",
								"name": "Syrup USDC",
								"asset": {"symbol": "USDC", "decimals": 6}
							}
						},
						{
							"id": "0x048073ae155702e75078c010e47308695eb6be9e",
							"borrower": {"id": "0x8fee157cb621ac7484f8b218edd1c14ce8722628"},
							"state": "Active",
							"principalOwed": "50000000000000",
							"acmRatio": "1595848",
							"collateral": {
								"asset": "BTC",
								"assetAmount": "117453084230",
								"assetValueUsd": "6793556500000",
								"decimals": 8,
								"state": "Deposited",
								"custodian": "FORDEFI",
								"liquidationLevel": "1250000"
							},
							"fundingPool": {
								"id": "0x196cbfc42ebb05e3b1ae1e7b45adcd5c0596d9f2",
								"name": "Syrup USDT",
								"asset": {"symbol": "USDT", "decimals": 6}
							}
						}
					]
				}`),
			},
			serverStatus: http.StatusOK,
			wantLoans: []expectedActiveLoan{
				{
					loanID:             common.HexToAddress("0x0430665a6e0ce5141b1cc42607d79632125cbbac"),
					borrower:           common.HexToAddress("0xc24b928b8f28ec560200bd46bfb84c1b7ae8f4a5"),
					state:              "Active",
					principalOwed:      big.NewInt(2_000_000_000_000),
					acmRatio:           big.NewInt(1_698_389),
					collateralAsset:    "BTC",
					collateralAmount:   big.NewInt(5_000_000_000),
					collateralValue:    big.NewInt(6_793_556_500_000),
					collateralDecimals: 8,
					collateralState:    "Deposited",
					collateralCustody:  "FORDEFI",
					liquidationLevel:   big.NewInt(1_176_471),
					poolAddress:        common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
					poolName:           "Syrup USDC",
					poolAssetSymbol:    "USDC",
					poolAssetDecimals:  6,
				},
				{
					loanID:             common.HexToAddress("0x048073ae155702e75078c010e47308695eb6be9e"),
					borrower:           common.HexToAddress("0x8fee157cb621ac7484f8b218edd1c14ce8722628"),
					state:              "Active",
					principalOwed:      big.NewInt(50_000_000_000_000),
					acmRatio:           big.NewInt(1_595_848),
					collateralAsset:    "BTC",
					collateralAmount:   big.NewInt(117_453_084_230),
					collateralValue:    big.NewInt(6_793_556_500_000),
					collateralDecimals: 8,
					collateralState:    "Deposited",
					collateralCustody:  "FORDEFI",
					liquidationLevel:   big.NewInt(1_250_000),
					poolAddress:        common.HexToAddress("0x196cbfc42ebb05e3b1ae1e7b45adcd5c0596d9f2"),
					poolName:           "Syrup USDT",
					poolAssetSymbol:    "USDT",
					poolAssetDecimals:  6,
				},
			},
			checkBlockNumber: true,
		},
		{
			name:        "success: empty loans list",
			blockNumber: 21500000,
			serverResponse: graphqlResponse{
				Data: json.RawMessage(`{
					"openTermLoans": []
				}`),
			},
			serverStatus: http.StatusOK,
			wantLoans:    []expectedActiveLoan{},
		},
		{
			name:           "HTTP error: 502",
			blockNumber:    21500000,
			serverResponse: `bad gateway`,
			serverStatus:   http.StatusBadGateway,
			wantErr:        true,
			errContains:    "unexpected status 502",
		},
		{
			name:        "GraphQL error in response",
			blockNumber: 21500000,
			serverResponse: map[string]any{
				"errors": []map[string]string{
					{"message": "rate limit exceeded"},
					{"message": "secondary error"},
				},
			},
			serverStatus: http.StatusOK,
			wantErr:      true,
			errContains:  "rate limit exceeded; secondary error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedBlockNumber uint64

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req graphqlRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
					if block, ok := req.Variables["block"].(map[string]any); ok {
						if num, ok := block["number"].(float64); ok {
							capturedBlockNumber = uint64(num)
						}
					}
				}

				w.WriteHeader(tc.serverStatus)
				_ = json.NewEncoder(w).Encode(tc.serverResponse)
			}))
			defer server.Close()

			client, err := NewClient(Config{
				Endpoint: server.URL,
				Logger:   testutil.DiscardLogger(),
			})
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			loans, err := client.GetAllActiveLoansAtBlock(ctx, tc.blockNumber)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tc.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(loans) != len(tc.wantLoans) {
				t.Fatalf("loans = %d, want %d", len(loans), len(tc.wantLoans))
			}

			if tc.checkBlockNumber {
				if capturedBlockNumber != tc.blockNumber {
					t.Errorf("captured block number = %d, want %d", capturedBlockNumber, tc.blockNumber)
				}
			}

			for i, want := range tc.wantLoans {
				assertLoan(t, i, want, loans[i])
			}
		})
	}
}

func TestGetAllActiveLoansAtBlock_Pagination(t *testing.T) {
	callCount := 0
	totalLoans := 2500

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req graphqlRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decoding request: %v", err)
		}

		skip := int(req.Variables["skip"].(float64))
		first := int(req.Variables["first"].(float64))
		callCount++

		remaining := totalLoans - skip
		count := min(remaining, first)

		loans := make([]map[string]any, count)
		for i := range count {
			loans[i] = map[string]any{
				"id":            fmt.Sprintf("0x%040x", skip+i),
				"borrower":      map[string]any{"id": "0xc24b928b8f28ec560200bd46bfb84c1b7ae8f4a5"},
				"state":         "Active",
				"principalOwed": "1000000000",
				"acmRatio":      "1000000",
				"collateral":    map[string]any{"asset": "BTC", "assetAmount": "1000000000", "assetValueUsd": "1000000000", "decimals": 8, "state": "Deposited", "custodian": "FORDEFI"},
				"fundingPool":   map[string]any{"id": "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b", "name": "Test Pool", "asset": map[string]any{"symbol": "USDC", "decimals": 6}},
			}
		}

		resp := map[string]any{"data": map[string]any{"openTermLoans": loans}}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client, err := NewClient(Config{
		Endpoint: server.URL,
		Logger:   testutil.DiscardLogger(),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	loans, err := client.GetAllActiveLoansAtBlock(ctx, 21500000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(loans) != totalLoans {
		t.Errorf("got %d loans, want %d", len(loans), totalLoans)
	}

	if callCount != 3 {
		t.Errorf("expected 3 API calls (1000 + 1000 + 500), got %d", callCount)
	}
}

// graphqlResponse is a helper for encoding test responses.
type graphqlResponse struct {
	Data json.RawMessage `json:"data"`
}
