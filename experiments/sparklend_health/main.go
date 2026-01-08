// Package main queries SparkLend getUserAccountData for all users using multicall and batched RPC.
package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

const (
	SparkLendPool      = "0xC13e21B648A5Ee794902342038FF3aDAB66BE987"
	Multicall3         = "0xcA11bde05977b3631167028862bE2a173976CA11"
	CallsPerMulticall  = 100
	MulticallsPerBatch = 10
)

// Multicall3 ABI for aggregate3
const multicall3ABI = `[{"inputs":[{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"bool","name":"allowFailure","type":"bool"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct Multicall3.Call3[]","name":"calls","type":"tuple[]"}],"name":"aggregate3","outputs":[{"components":[{"internalType":"bool","name":"success","type":"bool"},{"internalType":"bytes","name":"returnData","type":"bytes"}],"internalType":"struct Multicall3.Result[]","name":"returnData","type":"tuple[]"}],"stateMutability":"payable","type":"function"}]`

// SparkLend Pool ABI for getUserAccountData
const poolABI = `[{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getUserAccountData","outputs":[{"internalType":"uint256","name":"totalCollateralBase","type":"uint256"},{"internalType":"uint256","name":"totalDebtBase","type":"uint256"},{"internalType":"uint256","name":"availableBorrowsBase","type":"uint256"},{"internalType":"uint256","name":"currentLiquidationThreshold","type":"uint256"},{"internalType":"uint256","name":"ltv","type":"uint256"},{"internalType":"uint256","name":"healthFactor","type":"uint256"}],"stateMutability":"view","type":"function"}]`

type UserAccountData struct {
	User                        string
	TotalCollateralBase         *big.Int
	TotalDebtBase               *big.Int
	AvailableBorrowsBase        *big.Int
	CurrentLiquidationThreshold *big.Int
	LTV                         *big.Int
	HealthFactor                *big.Int
}

type RPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

type RPCResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      int       `json:"id"`
	Result  string    `json:"result,omitempty"`
	Error   *RPCError `json:"error,omitempty"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Call3 represents a Multicall3 call
type Call3 struct {
	Target       common.Address
	AllowFailure bool
	CallData     []byte
}

// Result represents a Multicall3 result
type Result struct {
	Success    bool
	ReturnData []byte
}

var (
	multicallParsedABI *abi.ABI
	sparkPoolParsedABI *abi.ABI
)

func init() {
	var err error
	multicallParsedABI, err = parseABI(multicall3ABI)
	if err != nil {
		panic(fmt.Sprintf("failed to parse multicall ABI: %v", err))
	}
	sparkPoolParsedABI, err = parseABI(poolABI)
	if err != nil {
		panic(fmt.Sprintf("failed to parse pool ABI: %v", err))
	}
}

func parseABI(jsonABI string) (*abi.ABI, error) {
	parsed, err := abi.JSON(strings.NewReader(jsonABI))
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	loadEnvFile(".env")

	alchemyURL := os.Getenv("ALCHEMY_URL")
	if alchemyURL == "" {
		alchemyKey := os.Getenv("ALCHEMY_API_KEY")
		if alchemyKey == "" {
			logger.Error("ALCHEMY_URL or ALCHEMY_API_KEY required")
			os.Exit(1)
		}
		alchemyURL = fmt.Sprintf("https://eth-mainnet.g.alchemy.com/v2/%s", alchemyKey)
	}

	csvPath := os.Getenv("CSV_PATH")
	if csvPath == "" {
		csvPath = "/Users/knaekbroed/Downloads/sparklend_users.csv"
	}

	blockNumber := os.Getenv("BLOCK_NUMBER")
	if blockNumber == "" {
		blockNumber = "latest"
	}

	users, err := readUsersFromCSV(csvPath)
	if err != nil {
		logger.Error("failed to read CSV", "error", err)
		os.Exit(1)
	}
	logger.Info("loaded users from CSV", "count", len(users))

	if blockNumber == "latest" {
		bn, err := getBlockNumber(alchemyURL)
		if err != nil {
			logger.Error("failed to get block number", "error", err)
			os.Exit(1)
		}
		blockNumber = fmt.Sprintf("0x%x", bn)
		logger.Info("using block", "number", bn, "hex", blockNumber)
	}

	start := time.Now()
	results, err := queryAllUsers(alchemyURL, users, blockNumber, logger)
	if err != nil {
		logger.Error("failed to query users", "error", err)
		os.Exit(1)
	}
	elapsed := time.Since(start)

	logger.Info("query complete",
		"users", len(users),
		"results", len(results),
		"duration", elapsed,
		"usersPerSecond", float64(len(users))/elapsed.Seconds(),
	)

	printSampleResults(results, 10)

	// Write results to CSV
	outputPath := filepath.Join(filepath.Dir(csvPath), "sparklend_health_results.csv")
	if err := writeResultsToCSV(outputPath, results); err != nil {
		logger.Error("failed to write CSV", "error", err)
	} else {
		logger.Info("wrote results to CSV", "path", outputPath, "rows", len(results))
	}

	usersWithDebt := 0
	usersWithCollateral := 0
	for _, r := range results {
		if r.TotalDebtBase != nil && r.TotalDebtBase.Sign() > 0 {
			usersWithDebt++
		}
		if r.TotalCollateralBase != nil && r.TotalCollateralBase.Sign() > 0 {
			usersWithCollateral++
		}
	}
	logger.Info("statistics", "usersWithCollateral", usersWithCollateral, "usersWithDebt", usersWithDebt)
}

func readUsersFromCSV(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	var users []string

	if _, err = reader.Read(); err != nil {
		return nil, err
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(record) > 0 {
			addr := strings.TrimSpace(record[0])
			addr = strings.Trim(addr, "\"")
			if strings.HasPrefix(addr, "0x") {
				users = append(users, addr)
			}
		}
	}
	return users, nil
}

func getBlockNumber(rpcURL string) (int64, error) {
	req := RPCRequest{JSONRPC: "2.0", Method: "eth_blockNumber", Params: []interface{}{}, ID: 1}
	body, _ := json.Marshal(req)
	resp, err := http.Post(rpcURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var rpcResp RPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return 0, err
	}
	if rpcResp.Error != nil {
		return 0, fmt.Errorf("RPC error: %s", rpcResp.Error.Message)
	}

	result := strings.TrimPrefix(rpcResp.Result, "0x")
	bn := new(big.Int)
	bn.SetString(result, 16)
	return bn.Int64(), nil
}

func queryAllUsers(rpcURL string, users []string, blockNumber string, logger *slog.Logger) ([]UserAccountData, error) {
	var allResults []UserAccountData
	userChunks := chunkSlice(users, CallsPerMulticall)
	logger.Info("split users into multicall chunks", "chunks", len(userChunks))

	multicallBatches := chunkSlice(userChunks, MulticallsPerBatch)
	logger.Info("split multicalls into RPC batches", "batches", len(multicallBatches))

	poolAddr := common.HexToAddress(SparkLendPool)

	for batchIdx, batch := range multicallBatches {
		var rpcRequests []RPCRequest
		for i, userChunk := range batch {
			calldata, err := buildMulticallData(userChunk, poolAddr)
			if err != nil {
				return nil, fmt.Errorf("failed to build multicall: %w", err)
			}
			rpcRequests = append(rpcRequests, RPCRequest{
				JSONRPC: "2.0",
				Method:  "eth_call",
				Params: []interface{}{
					map[string]string{"to": Multicall3, "data": "0x" + hex.EncodeToString(calldata)},
					blockNumber,
				},
				ID: batchIdx*MulticallsPerBatch + i + 1,
			})
		}

		responses, err := sendBatchRequest(rpcURL, rpcRequests)
		if err != nil {
			return nil, fmt.Errorf("batch %d failed: %w", batchIdx, err)
		}

		for i, resp := range responses {
			if resp.Error != nil {
				logger.Warn("multicall failed", "batch", batchIdx, "multicall", i, "error", resp.Error.Message)
				continue
			}
			userChunk := batch[i]
			results, err := decodeMulticallResponse(resp.Result, userChunk)
			if err != nil {
				logger.Warn("failed to decode multicall response", "error", err)
				continue
			}
			allResults = append(allResults, results...)
		}

		if batchIdx < len(multicallBatches)-1 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	return allResults, nil
}

func buildMulticallData(users []string, poolAddr common.Address) ([]byte, error) {
	// Build getUserAccountData calldata for each user
	var calls []Call3
	for _, user := range users {
		userAddr := common.HexToAddress(user)
		callData, err := sparkPoolParsedABI.Pack("getUserAccountData", userAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to pack getUserAccountData: %w", err)
		}
		calls = append(calls, Call3{
			Target:       poolAddr,
			AllowFailure: true,
			CallData:     callData,
		})
	}

	// Pack aggregate3 call
	return multicallParsedABI.Pack("aggregate3", calls)
}

func sendBatchRequest(rpcURL string, requests []RPCRequest) ([]RPCResponse, error) {
	body, err := json.Marshal(requests)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(rpcURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var responses []RPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&responses); err != nil {
		return nil, err
	}
	return responses, nil
}

func decodeMulticallResponse(result string, users []string) ([]UserAccountData, error) {
	result = strings.TrimPrefix(result, "0x")
	data, err := hex.DecodeString(result)
	if err != nil {
		return nil, fmt.Errorf("hex decode: %w", err)
	}

	// Unpack aggregate3 response
	decoded, err := multicallParsedABI.Unpack("aggregate3", data)
	if err != nil {
		return nil, fmt.Errorf("unpack aggregate3: %w", err)
	}

	// The result is a slice of Result structs
	resultsRaw := decoded[0].([]struct {
		Success    bool   `json:"success"`
		ReturnData []byte `json:"returnData"`
	})

	var results []UserAccountData
	for i, r := range resultsRaw {
		if i >= len(users) {
			break
		}
		user := users[i]

		if !r.Success || len(r.ReturnData) < 192 {
			results = append(results, UserAccountData{
				User:                        user,
				TotalCollateralBase:         big.NewInt(0),
				TotalDebtBase:               big.NewInt(0),
				AvailableBorrowsBase:        big.NewInt(0),
				CurrentLiquidationThreshold: big.NewInt(0),
				LTV:                         big.NewInt(0),
				HealthFactor:                big.NewInt(0),
			})
			continue
		}

		// Decode getUserAccountData response
		unpacked, err := sparkPoolParsedABI.Unpack("getUserAccountData", r.ReturnData)
		if err != nil {
			results = append(results, UserAccountData{User: user})
			continue
		}

		results = append(results, UserAccountData{
			User:                        user,
			TotalCollateralBase:         unpacked[0].(*big.Int),
			TotalDebtBase:               unpacked[1].(*big.Int),
			AvailableBorrowsBase:        unpacked[2].(*big.Int),
			CurrentLiquidationThreshold: unpacked[3].(*big.Int),
			LTV:                         unpacked[4].(*big.Int),
			HealthFactor:                unpacked[5].(*big.Int),
		})
	}

	return results, nil
}

func chunkSlice[T any](slice []T, chunkSize int) [][]T {
	var chunks [][]T
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize
		if end > len(slice) {
			end = len(slice)
		}
		chunks = append(chunks, slice[i:end])
	}
	return chunks
}

func printSampleResults(results []UserAccountData, count int) {
	fmt.Println("\n=== Sample Results ===")
	printed := 0
	for _, r := range results {
		if r.TotalCollateralBase != nil && r.TotalDebtBase != nil &&
			(r.TotalCollateralBase.Sign() > 0 || r.TotalDebtBase.Sign() > 0) {
			collateral := new(big.Float).Quo(new(big.Float).SetInt(r.TotalCollateralBase), big.NewFloat(1e8))
			debt := new(big.Float).Quo(new(big.Float).SetInt(r.TotalDebtBase), big.NewFloat(1e8))
			healthFactor := new(big.Float).Quo(new(big.Float).SetInt(r.HealthFactor), big.NewFloat(1e18))

			fmt.Printf("User: %s\n", r.User)
			fmt.Printf("  Collateral: $%.2f\n", collateral)
			fmt.Printf("  Debt: $%.2f\n", debt)
			fmt.Printf("  Health Factor: %.4f\n", healthFactor)
			fmt.Println()
			printed++
			if printed >= count {
				break
			}
		}
	}
}

func writeResultsToCSV(path string, results []UserAccountData) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	// Write header
	header := []string{"user", "total_collateral_usd", "total_debt_usd", "available_borrows_usd", "liquidation_threshold", "ltv", "health_factor"}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write rows
	for _, r := range results {
		collateral := "0"
		debt := "0"
		availableBorrows := "0"
		liqThreshold := "0"
		ltv := "0"
		healthFactor := "0"

		if r.TotalCollateralBase != nil {
			collateral = new(big.Float).Quo(new(big.Float).SetInt(r.TotalCollateralBase), big.NewFloat(1e8)).Text('f', 2)
		}
		if r.TotalDebtBase != nil {
			debt = new(big.Float).Quo(new(big.Float).SetInt(r.TotalDebtBase), big.NewFloat(1e8)).Text('f', 2)
		}
		if r.AvailableBorrowsBase != nil {
			availableBorrows = new(big.Float).Quo(new(big.Float).SetInt(r.AvailableBorrowsBase), big.NewFloat(1e8)).Text('f', 2)
		}
		if r.CurrentLiquidationThreshold != nil {
			liqThreshold = new(big.Float).Quo(new(big.Float).SetInt(r.CurrentLiquidationThreshold), big.NewFloat(1e4)).Text('f', 4)
		}
		if r.LTV != nil {
			ltv = new(big.Float).Quo(new(big.Float).SetInt(r.LTV), big.NewFloat(1e4)).Text('f', 4)
		}
		if r.HealthFactor != nil {
			hf := new(big.Float).Quo(new(big.Float).SetInt(r.HealthFactor), big.NewFloat(1e18))
			// Cap extremely large health factors for readability
			maxHF := big.NewFloat(1e12)
			if hf.Cmp(maxHF) > 0 {
				healthFactor = "inf"
			} else {
				healthFactor = hf.Text('f', 4)
			}
		}

		row := []string{r.User, collateral, debt, availableBorrows, liqThreshold, ltv, healthFactor}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}

func loadEnvFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			value = strings.Trim(value, "\"'")
			if os.Getenv(key) == "" {
				os.Setenv(key, value)
			}
		}
	}
}
