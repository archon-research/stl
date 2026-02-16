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
	tests := []struct {
		name             string
		poolAddress      common.Address
		serverResponse   any
		serverStatus     int
		wantCollaterals  int
		wantErr          bool
		errContains      string
		checkPoolID      bool
		checkCollaterals bool
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
			serverStatus:     http.StatusOK,
			wantCollaterals:  2,
			checkPoolID:      true,
			checkCollaterals: true,
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
			serverStatus:    http.StatusOK,
			wantCollaterals: 2, // ZERO_ASSET filtered out
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
			name:        "invalid TVL: non-numeric",
			poolAddress: common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
			serverResponse: graphqlResponse{
				Data: json.RawMessage(`{
					"poolV2": {
						"tvl": "not_a_number",
						"poolMeta": {"poolCollaterals": []}
					}
				}`),
			},
			serverStatus: http.StatusOK,
			wantErr:      true,
			errContains:  "parsing TVL",
		},
		{
			name:        "invalid collateral value: non-numeric",
			poolAddress: common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
			serverResponse: graphqlResponse{
				Data: json.RawMessage(`{
					"poolV2": {
						"tvl": "1000000000000000",
						"poolMeta": {
							"poolCollaterals": [
								{"asset": "BTC", "assetValueUsd": "bad_value", "assetDecimals": 8}
							]
						}
					}
				}`),
			},
			serverStatus: http.StatusOK,
			wantErr:      true,
			errContains:  "parsing asset value",
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
			wantCollaterals: 0,
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
			if len(data.Collaterals) != tc.wantCollaterals {
				t.Fatalf("collaterals = %d, want %d", len(data.Collaterals), tc.wantCollaterals)
			}

			if tc.checkPoolID {
				wantPoolID := strings.ToLower(tc.poolAddress.Hex())
				if capturedPoolID != wantPoolID {
					t.Errorf("captured pool ID = %q, want %q (lowercased)", capturedPoolID, wantPoolID)
				}
			}

			if tc.checkCollaterals && len(data.Collaterals) >= 2 {
				// Verify TVL.
				expectedTVL := big.NewInt(1_000_000_000_000_000)
				if data.TVL.Cmp(expectedTVL) != 0 {
					t.Errorf("TVL = %s, want %s", data.TVL, expectedTVL)
				}

				// Verify first collateral.
				btc := data.Collaterals[0]
				if btc.Asset != "BTC" {
					t.Errorf("collateral[0].Asset = %q, want BTC", btc.Asset)
				}
				expectedBTC := big.NewInt(674_000_000_000_000)
				if btc.AssetValueUSD.Cmp(expectedBTC) != 0 {
					t.Errorf("BTC value = %s, want %s", btc.AssetValueUSD, expectedBTC)
				}
				if btc.AssetDecimals != 8 {
					t.Errorf("BTC decimals = %d, want 8", btc.AssetDecimals)
				}

				// Verify second collateral.
				xrp := data.Collaterals[1]
				if xrp.Asset != "XRP" {
					t.Errorf("collateral[1].Asset = %q, want XRP", xrp.Asset)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestGetBorrowerCollateralAtBlock
// ---------------------------------------------------------------------------

func TestGetBorrowerCollateralAtBlock(t *testing.T) {
	tests := []struct {
		name             string
		poolAddress      common.Address
		blockNumber      uint64
		serverResponse   any
		serverStatus     int
		wantLoans        int
		wantErr          bool
		errContains      string
		checkPoolID      bool
		checkBlockNumber bool
		checkLoans       bool
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
			serverStatus:     http.StatusOK,
			wantLoans:        2,
			checkPoolID:      true,
			checkBlockNumber: true,
			checkLoans:       true,
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
			wantLoans:    0,
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
			name:        "invalid principal owed: non-numeric",
			poolAddress: common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
			blockNumber: 21500000,
			serverResponse: graphqlResponse{
				Data: json.RawMessage(`{
					"poolV2": {
						"openTermLoans": [
							{
								"id": "0x0430665a6e0ce5141b1cc42607d79632125cbbac",
								"borrower": {"id": "0xc24b928b8f28ec560200bd46bfb84c1b7ae8f4a5"},
								"state": "Active",
								"principalOwed": "not_a_number",
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
							}
						]
					}
				}`),
			},
			serverStatus: http.StatusOK,
			wantErr:      true,
			errContains:  "parsing principal owed",
		},
		{
			name:        "invalid acm ratio: non-numeric",
			poolAddress: common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
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
								"acmRatio": "invalid",
								"collateral": {
									"asset": "BTC",
									"assetAmount": "5000000000",
									"assetValueUsd": "6793556500000",
									"decimals": 8,
									"state": "Deposited",
									"custodian": "FORDEFI",
									"liquidationLevel": "1176471"
								}
							}
						]
					}
				}`),
			},
			serverStatus: http.StatusOK,
			wantErr:      true,
			errContains:  "parsing acm ratio",
		},
		{
			name:        "invalid asset amount: non-numeric",
			poolAddress: common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"),
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
									"assetAmount": "bad_value",
									"assetValueUsd": "6793556500000",
									"decimals": 8,
									"state": "Deposited",
									"custodian": "FORDEFI",
									"liquidationLevel": "1176471"
								}
							}
						]
					}
				}`),
			},
			serverStatus: http.StatusOK,
			wantErr:      true,
			errContains:  "parsing asset amount",
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
			if len(loans) != tc.wantLoans {
				t.Fatalf("loans = %d, want %d", len(loans), tc.wantLoans)
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

			if tc.checkLoans && len(loans) >= 1 {
				loan := loans[0]
				wantLoanID := common.HexToAddress("0x0430665a6e0ce5141b1cc42607d79632125cbbac")
				if loan.LoanID != wantLoanID {
					t.Errorf("LoanID = %s, want %s", loan.LoanID.Hex(), wantLoanID.Hex())
				}
				wantBorrower := common.HexToAddress("0xc24b928b8f28ec560200bd46bfb84c1b7ae8f4a5")
				if loan.Borrower != wantBorrower {
					t.Errorf("Borrower = %s, want %s", loan.Borrower.Hex(), wantBorrower.Hex())
				}
				if loan.State != "Active" {
					t.Errorf("State = %q, want Active", loan.State)
				}
				expectedPrincipal := big.NewInt(2_000_000_000_000)
				if loan.PrincipalOwed.Cmp(expectedPrincipal) != 0 {
					t.Errorf("PrincipalOwed = %s, want %s", loan.PrincipalOwed, expectedPrincipal)
				}
				expectedAcmRatio := big.NewInt(1_698_389)
				if loan.AcmRatio.Cmp(expectedAcmRatio) != 0 {
					t.Errorf("AcmRatio = %s, want %s", loan.AcmRatio, expectedAcmRatio)
				}
				if loan.Collateral.Asset != "BTC" {
					t.Errorf("Collateral.Asset = %q, want BTC", loan.Collateral.Asset)
				}
				expectedAssetAmount := big.NewInt(5_000_000_000)
				if loan.Collateral.AssetAmount.Cmp(expectedAssetAmount) != 0 {
					t.Errorf("AssetAmount = %s, want %s", loan.Collateral.AssetAmount, expectedAssetAmount)
				}
				if loan.Collateral.Decimals != 8 {
					t.Errorf("Decimals = %d, want 8", loan.Collateral.Decimals)
				}
				if loan.Collateral.State != "Deposited" {
					t.Errorf("Collateral.State = %q, want Deposited", loan.Collateral.State)
				}
				if loan.Collateral.Custodian != "FORDEFI" {
					t.Errorf("Custodian = %q, want FORDEFI", loan.Collateral.Custodian)
				}
				expectedLiquidation := big.NewInt(1_176_471)
				if loan.Collateral.LiquidationLevel.Cmp(expectedLiquidation) != 0 {
					t.Errorf("LiquidationLevel = %s, want %s", loan.Collateral.LiquidationLevel, expectedLiquidation)
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
