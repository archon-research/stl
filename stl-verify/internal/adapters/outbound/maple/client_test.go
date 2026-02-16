package maple

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// TestGetPoolCollateral
// ---------------------------------------------------------------------------

func TestGetPoolCollateral(t *testing.T) {
	type expectedCollateral struct {
		asset    string
		valueUSD *big.Int
		decimals int
	}

	tests := []struct {
		name            string
		poolAddress     common.Address
		serverResponse  any
		serverStatus    int
		wantTVL         *big.Int
		wantCollaterals []expectedCollateral
		wantErr         bool
		errContains     string
		checkPoolID     bool
	}{
		{
			name:        "success: pool with collateral",
			poolAddress: common.HexToAddress("0x80AC24AA929EAF5013F6436CDA2A7BA190F5CC0B"), // mixed case
			serverResponse: graphqlResponse{
				Data: json.RawMessage(`{
					"poolV2": {
						"tvl": "1000000000000000",
						"poolMeta": {
							"poolCollaterals": [
								{"asset": "BTC", "assetValueUsd": "674000000000000", "assetDecimals": 8},
								{"asset": "XRP", "assetValueUsd": "317000000000000", "assetDecimals": 6}
							]
						}
					}
				}`),
			},
			serverStatus: http.StatusOK,
			wantTVL:      big.NewInt(1_000_000_000_000_000),
			wantCollaterals: []expectedCollateral{
				{asset: "BTC", valueUSD: big.NewInt(674_000_000_000_000), decimals: 8},
				{asset: "XRP", valueUSD: big.NewInt(317_000_000_000_000), decimals: 6},
			},
			checkPoolID: true,
		},
		{
			name:        "zero-value collateral entries are filtered",
			poolAddress: common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
			serverResponse: graphqlResponse{
				Data: json.RawMessage(`{
					"poolV2": {
						"tvl": "1000000000000000",
						"poolMeta": {
							"poolCollaterals": [
								{"asset": "BTC", "assetValueUsd": "674000000000000", "assetDecimals": 8},
								{"asset": "ZERO_ASSET", "assetValueUsd": "0", "assetDecimals": 18},
								{"asset": "XRP", "assetValueUsd": "317000000000000", "assetDecimals": 6}
							]
						}
					}
				}`),
			},
			serverStatus: http.StatusOK,
			wantTVL:      big.NewInt(1_000_000_000_000_000),
			wantCollaterals: []expectedCollateral{
				{asset: "BTC", valueUSD: big.NewInt(674_000_000_000_000), decimals: 8},
				{asset: "XRP", valueUSD: big.NewInt(317_000_000_000_000), decimals: 6},
			},
		},
		{
			name:        "pool not found",
			poolAddress: common.HexToAddress("0x0000000000000000000000000000000000000001"),
			serverResponse: graphqlResponse{
				Data: json.RawMessage(`{"poolV2": null}`),
			},
			serverStatus: http.StatusOK,
			wantErr:      true,
			errContains:  "not found",
		},
		{
			name:           "HTTP error: 502",
			poolAddress:    common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
			serverResponse: `bad gateway`,
			serverStatus:   http.StatusBadGateway,
			wantErr:        true,
			errContains:    "unexpected status 502",
		},
		{
			name:        "GraphQL error in response",
			poolAddress: common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
			serverResponse: map[string]any{
				"errors": []map[string]string{
					{"message": "rate limit exceeded"},
				},
			},
			serverStatus: http.StatusOK,
			wantErr:      true,
			errContains:  "graphql error",
		},
		{
			name:        "empty collateral list: returns zero collaterals",
			poolAddress: common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
			serverResponse: graphqlResponse{
				Data: json.RawMessage(`{
					"poolV2": {
						"tvl": "1000000000000000",
						"poolMeta": {"poolCollaterals": []}
					}
				}`),
			},
			serverStatus:    http.StatusOK,
			wantTVL:         big.NewInt(1_000_000_000_000_000),
			wantCollaterals: []expectedCollateral{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedPoolID string

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req graphqlRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
					if pid, ok := req.Variables["poolId"].(string); ok {
						capturedPoolID = pid
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

			data, err := client.GetPoolCollateral(ctx, tc.poolAddress)
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
			if data == nil {
				t.Fatal("data is nil")
			}
			if tc.wantTVL != nil {
				if data.TVL.Cmp(tc.wantTVL) != 0 {
					t.Errorf("TVL = %s, want %s", data.TVL, tc.wantTVL)
				}
			}
			if len(data.Collaterals) != len(tc.wantCollaterals) {
				t.Fatalf("collaterals = %d, want %d", len(data.Collaterals), len(tc.wantCollaterals))
			}

			if tc.checkPoolID {
				wantPoolID := strings.ToLower(tc.poolAddress.Hex())
				if capturedPoolID != wantPoolID {
					t.Errorf("captured pool ID = %q, want %q (lowercased)", capturedPoolID, wantPoolID)
				}
			}

			for i, want := range tc.wantCollaterals {
				got := data.Collaterals[i]
				if got.Asset != want.asset {
					t.Errorf("collateral[%d].Asset = %q, want %q", i, got.Asset, want.asset)
				}
				if want.valueUSD != nil && got.AssetValueUSD.Cmp(want.valueUSD) != 0 {
					t.Errorf("collateral[%d].AssetValueUSD = %s, want %s", i, got.AssetValueUSD, want.valueUSD)
				}
				if got.AssetDecimals != want.decimals {
					t.Errorf("collateral[%d].AssetDecimals = %d, want %d", i, got.AssetDecimals, want.decimals)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestGetBorrowerCollateralAtBlock
// ---------------------------------------------------------------------------

func TestGetBorrowerCollateralAtBlock(t *testing.T) {
	type expectedLoan struct {
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
	}

	tests := []struct {
		name             string
		poolAddress      common.Address
		blockNumber      uint64
		serverResponse   any
		serverStatus     int
		wantLoans        []expectedLoan
		wantErr          bool
		errContains      string
		checkPoolID      bool
		checkBlockNumber bool
	}{
		{
			name:        "success: multiple loans with collateral",
			poolAddress: common.HexToAddress("0x80AC24AA929EAF5013F6436CDA2A7BA190F5CC0B"),
			blockNumber: 21500000,
			serverResponse: graphqlResponse{
				Data: json.RawMessage(`{
					"poolV2": {
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
								}
							}
						]
					}
				}`),
			},
			serverStatus: http.StatusOK,
			wantLoans: []expectedLoan{
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
				},
			},
			checkPoolID:      true,
			checkBlockNumber: true,
		},
		{
			name:        "success: empty loans list",
			poolAddress: common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
			blockNumber: 21500000,
			serverResponse: graphqlResponse{
				Data: json.RawMessage(`{
					"poolV2": {
						"openTermLoans": []
					}
				}`),
			},
			serverStatus: http.StatusOK,
			wantLoans:    []expectedLoan{},
		},
		{
			name:        "pool not found",
			poolAddress: common.HexToAddress("0x0000000000000000000000000000000000000001"),
			blockNumber: 21500000,
			serverResponse: graphqlResponse{
				Data: json.RawMessage(`{"poolV2": null}`),
			},
			serverStatus: http.StatusOK,
			wantErr:      true,
			errContains:  "not found",
		},
		{
			name:           "HTTP error: 502",
			poolAddress:    common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
			blockNumber:    21500000,
			serverResponse: `bad gateway`,
			serverStatus:   http.StatusBadGateway,
			wantErr:        true,
			errContains:    "unexpected status 502",
		},
		{
			name:        "GraphQL error in response",
			poolAddress: common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
			blockNumber: 21500000,
			serverResponse: map[string]any{
				"errors": []map[string]string{
					{"message": "rate limit exceeded"},
				},
			},
			serverStatus: http.StatusOK,
			wantErr:      true,
			errContains:  "graphql error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedPoolID string
			var capturedBlockNumber uint64

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req graphqlRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
					if pid, ok := req.Variables["poolId"].(string); ok {
						capturedPoolID = pid
					}
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

			loans, err := client.GetBorrowerCollateralAtBlock(ctx, tc.poolAddress, tc.blockNumber)
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

			if tc.checkPoolID {
				wantPoolID := strings.ToLower(tc.poolAddress.Hex())
				if capturedPoolID != wantPoolID {
					t.Errorf("captured pool ID = %q, want %q (lowercased)", capturedPoolID, wantPoolID)
				}
			}

			if tc.checkBlockNumber {
				if capturedBlockNumber != tc.blockNumber {
					t.Errorf("captured block number = %d, want %d", capturedBlockNumber, tc.blockNumber)
				}
			}

			for i, want := range tc.wantLoans {
				loan := loans[i]
				if loan.LoanID != want.loanID {
					t.Errorf("loans[%d].LoanID = %s, want %s", i, loan.LoanID.Hex(), want.loanID.Hex())
				}
				if loan.Borrower != want.borrower {
					t.Errorf("loans[%d].Borrower = %s, want %s", i, loan.Borrower.Hex(), want.borrower.Hex())
				}
				if loan.State != want.state {
					t.Errorf("loans[%d].State = %q, want %q", i, loan.State, want.state)
				}
				if want.principalOwed != nil && loan.PrincipalOwed.Cmp(want.principalOwed) != 0 {
					t.Errorf("loans[%d].PrincipalOwed = %s, want %s", i, loan.PrincipalOwed, want.principalOwed)
				}
				if want.acmRatio != nil && loan.AcmRatio.Cmp(want.acmRatio) != 0 {
					t.Errorf("loans[%d].AcmRatio = %s, want %s", i, loan.AcmRatio, want.acmRatio)
				}
				if loan.Collateral.Asset != want.collateralAsset {
					t.Errorf("loans[%d].Collateral.Asset = %q, want %q", i, loan.Collateral.Asset, want.collateralAsset)
				}
				if want.collateralAmount != nil && loan.Collateral.AssetAmount.Cmp(want.collateralAmount) != 0 {
					t.Errorf("loans[%d].Collateral.AssetAmount = %s, want %s", i, loan.Collateral.AssetAmount, want.collateralAmount)
				}
				if want.collateralValue != nil && loan.Collateral.AssetValueUSD.Cmp(want.collateralValue) != 0 {
					t.Errorf("loans[%d].Collateral.AssetValueUSD = %s, want %s", i, loan.Collateral.AssetValueUSD, want.collateralValue)
				}
				if loan.Collateral.Decimals != want.collateralDecimals {
					t.Errorf("loans[%d].Collateral.Decimals = %d, want %d", i, loan.Collateral.Decimals, want.collateralDecimals)
				}
				if loan.Collateral.State != want.collateralState {
					t.Errorf("loans[%d].Collateral.State = %q, want %q", i, loan.Collateral.State, want.collateralState)
				}
				if loan.Collateral.Custodian != want.collateralCustody {
					t.Errorf("loans[%d].Collateral.Custodian = %q, want %q", i, loan.Collateral.Custodian, want.collateralCustody)
				}
				if want.liquidationLevel != nil && loan.Collateral.LiquidationLevel.Cmp(want.liquidationLevel) != 0 {
					t.Errorf("loans[%d].Collateral.LiquidationLevel = %s, want %s", i, loan.Collateral.LiquidationLevel, want.liquidationLevel)
				}
			}
		})
	}
}

func TestExecute_contextCanceled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Block until client gives up.
		<-r.Context().Done()
	}))
	defer server.Close()

	client, err := NewClient(Config{
		Endpoint: server.URL,
		Logger:   testutil.DiscardLogger(),
		Timeout:  10 * time.Second, // Long timeout, but we cancel ctx.
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err = client.ListPools(ctx)
	if err == nil {
		t.Fatal("expected error for canceled context")
	}
}

func TestExecute_invalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not valid json {{{"))
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

	_, err = client.ListPools(ctx)
	if err == nil {
		t.Fatal("expected error for invalid JSON response")
	}
	if !strings.Contains(err.Error(), "decoding response") {
		t.Errorf("error %q should contain 'decoding response'", err.Error())
	}
}

// graphqlResponse is a helper for encoding test responses.
type graphqlResponse struct {
	Data json.RawMessage `json:"data"`
}
