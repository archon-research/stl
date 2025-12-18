package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
)

const (
	SparkLendPool              = "0xC13e21B648A5Ee794902342038FF3aDAB66BE987"
	Multicall3                 = "0xcA11bde05977b3631167028862bE2a173976CA11"
	GetUserAccountDataSelector = "bf92857c"
	Aggregate3Selector         = "82ad56cb" // aggregate3((address,bool,bytes)[])
	DefaultRPCEndpoint         = "http://100.87.11.17:8545"
)

type RPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

type RPCResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      int       `json:"id"`
	Result  string    `json:"result"`
	Error   *RPCError `json:"error"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// UserAccountData contains the full response from getUserAccountData()
// All values are in base units (e.g., USD with 8 decimals for amounts)
type UserAccountData struct {
	User                        string
	TotalCollateralBase         *big.Int // Total collateral in base currency (USD, 8 decimals)
	TotalDebtBase               *big.Int // Total debt in base currency (USD, 8 decimals)
	AvailableBorrowsBase        *big.Int // Available borrows in base currency (USD, 8 decimals)
	CurrentLiquidationThreshold *big.Int // Liquidation threshold in basis points (e.g., 8000 = 80%)
	Ltv                         *big.Int // Loan-to-value in basis points (e.g., 7500 = 75%)
	HealthFactor                *big.Int // Health factor with 18 decimals (1e18 = 1.0)
	Error                       string
}

// Default blocks to query
var defaultBlocks = []int64{
	16776401, 17151906, 17527411, 17902916, 18278421, 18653926, 19029431, 19404937,
	19780442, 20155947, 20531452, 20906957, 21282462, 21657968, 22033473, 22408978,
	22784483, 23159988, 23535493, 23910999,
}

func main() {
	rpcURL := flag.String("rpc", DefaultRPCEndpoint, "Ethereum archive node RPC URL")
	usersFile := flag.String("users", "", "File with user addresses (one per line)")
	block := flag.Int64("block", 0, "Block number to query (0 for latest, -1 for default list)")
	outputDir := flag.String("outdir", ".", "Output directory for CSV files")
	batchSize := flag.Int("batch", 250, "Users per multicall")
	flag.Parse()

	if *usersFile == "" {
		fmt.Fprintln(os.Stderr, "Error: -users required")
		os.Exit(1)
	}

	users, err := readUsersFromFile(*usersFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading users: %v\n", err)
		os.Exit(1)
	}

	// Determine which blocks to query
	var blocks []int64
	if *block == -1 {
		blocks = defaultBlocks
	} else if *block == 0 {
		blocks = []int64{0} // latest
	} else {
		blocks = []int64{*block}
	}

	// If batch size is 0 or greater than user count, do all in one call
	batch := *batchSize
	if batch <= 0 || batch > len(users) {
		batch = len(users)
	}

	fmt.Printf("Querying health factors for %d users across %d blocks\n", len(users), len(blocks))
	fmt.Printf("RPC: %s\n", *rpcURL)
	fmt.Printf("Batch size: %d users per multicall\n", batch)
	fmt.Printf("Output directory: %s\n\n", *outputDir)

	for blockIdx, blockNum := range blocks {
		blockParam := "latest"
		blockLabel := "latest"
		if blockNum > 0 {
			blockParam = fmt.Sprintf("0x%x", blockNum)
			blockLabel = fmt.Sprintf("%d", blockNum)
		}

		fmt.Printf("[%d/%d] Block %s\n", blockIdx+1, len(blocks), blockLabel)

		var allResults []UserAccountData
		for i := 0; i < len(users); i += batch {
			end := i + batch
			if end > len(users) {
				end = len(users)
			}
			batchUsers := users[i:end]

			fmt.Printf("  Querying users %d-%d (%d/%d)\n", i+1, end, end, len(users))

			batchResults, err := queryMulticall(*rpcURL, batchUsers, blockParam)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error querying batch: %v\n", err)
				os.Exit(1)
			}
			allResults = append(allResults, batchResults...)
		}

		outputFile := fmt.Sprintf("%s/health_factors_block_%s.csv", *outputDir, blockLabel)
		if err := writeCSV(allResults, outputFile, blockNum); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing CSV: %v\n", err)
			os.Exit(1)
		}

		// Summary
		var withDebt, noDebt, errors int
		maxUint256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
		for _, r := range allResults {
			if r.Error != "" {
				errors++
			} else if r.HealthFactor.Cmp(maxUint256) == 0 {
				noDebt++
			} else {
				withDebt++
			}
		}
		fmt.Printf("  Written %d results → %s (debt: %d, no-debt: %d, errors: %d)\n\n",
			len(allResults), outputFile, withDebt, noDebt, errors)
	}

	fmt.Println("Done!")
}

func queryMulticall(rpcURL string, users []string, blockParam string) ([]UserAccountData, error) {
	// Build multicall data
	calldata := buildMulticallData(users)

	request := RPCRequest{
		JSONRPC: "2.0",
		Method:  "eth_call",
		Params: []interface{}{
			map[string]string{
				"to":   Multicall3,
				"data": calldata,
			},
			blockParam,
		},
		ID: 1,
	}

	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(rpcURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var response RPCResponse
	if err := json.Unmarshal(respBody, &response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %v", err)
	}

	if response.Error != nil {
		return nil, fmt.Errorf("RPC error: %s", response.Error.Message)
	}

	return decodeMulticallResults(users, response.Result)
}

func decodeUint256(hexData string, offset int) (*big.Int, error) {
	start := offset * 64
	end := start + 64
	if end > len(hexData) {
		return nil, fmt.Errorf("data too short for uint256 at offset %d", offset)
	}
	bytes, err := hex.DecodeString(hexData[start:end])
	if err != nil {
		return nil, err
	}
	return new(big.Int).SetBytes(bytes), nil
}

func buildMulticallData(users []string) string {
	// aggregate3((address target, bool allowFailure, bytes callData)[])
	// Returns (bool success, bytes returnData)[]

	var data strings.Builder
	data.WriteString("0x")
	data.WriteString(Aggregate3Selector)

	// Offset to array (always 0x20 = 32)
	data.WriteString(fmt.Sprintf("%064x", 32))

	// Array length
	data.WriteString(fmt.Sprintf("%064x", len(users)))

	// Each Call3 struct offset (relative to array start)
	// Structs are dynamic, so we need offsets
	baseOffset := len(users) * 32 // space for all offset pointers
	for i := range users {
		// Each struct is: address (32) + bool (32) + bytes offset (32) + bytes length (32) + bytes data (64 padded)
		// = 192 bytes per struct
		structOffset := baseOffset + i*192
		data.WriteString(fmt.Sprintf("%064x", structOffset))
	}

	// Now write each Call3 struct
	for _, user := range users {
		// target address (SparkLend Pool)
		data.WriteString(fmt.Sprintf("%064s", strings.TrimPrefix(SparkLendPool, "0x")))

		// allowFailure = true (so we get results even if some fail)
		data.WriteString(fmt.Sprintf("%064x", 1))

		// offset to callData (always 96 = 0x60, relative to struct start)
		data.WriteString(fmt.Sprintf("%064x", 96))

		// callData length (36 bytes = 4 selector + 32 address)
		data.WriteString(fmt.Sprintf("%064x", 36))

		// callData: getUserAccountData(user)
		callData := GetUserAccountDataSelector + fmt.Sprintf("%064s", strings.TrimPrefix(strings.ToLower(user), "0x"))
		// Pad to 64 bytes (2 * 32)
		data.WriteString(callData)
		data.WriteString(strings.Repeat("0", 64-len(callData)%64))
	}

	return data.String()
}

func decodeMulticallResults(users []string, resultHex string) ([]UserAccountData, error) {
	data := strings.TrimPrefix(resultHex, "0x")
	if len(data) < 128 {
		return nil, fmt.Errorf("response too short: %d", len(data))
	}

	results := make([]UserAccountData, len(users))
	for i := range results {
		results[i].User = users[i]
	}

	// aggregate3 returns: (bool success, bytes returnData)[]
	// ABI encoding:
	// - offset to array (32 bytes) -> points to array start
	// - [at array start] array length (32 bytes)
	// - [then] N offsets (32 bytes each), relative to array start
	// - [then] N Result structs

	// Read offset to array
	arrayOffset, _ := strconv.ParseInt(data[:64], 16, 64)
	arrayStart := int(arrayOffset) * 2 // convert to hex chars

	if arrayStart+64 > len(data) {
		return nil, fmt.Errorf("array offset out of bounds")
	}

	// Read array length at arrayStart
	arrayLen, _ := strconv.ParseInt(data[arrayStart:arrayStart+64], 16, 64)
	if int(arrayLen) != len(users) {
		return nil, fmt.Errorf("unexpected array length: got %d, expected %d", arrayLen, len(users))
	}

	// Read offsets (each relative to arrayStart)
	offsetsStart := arrayStart + 64
	offsets := make([]int64, len(users))
	for i := range users {
		pos := offsetsStart + i*64
		if pos+64 > len(data) {
			results[i].Error = "missing offset"
			continue
		}
		offsets[i], _ = strconv.ParseInt(data[pos:pos+64], 16, 64)
	}

	// Read each Result struct
	for i, user := range users {
		results[i].User = user

		// Offset is relative to start of array content (after length word)
		// arrayStart + 64 (length word) + offset_bytes*2
		structPos := arrayStart + 64 + int(offsets[i])*2
		if structPos+128 > len(data) {
			results[i].Error = "struct out of bounds"
			continue
		}

		// Read success (bool, 32 bytes)
		success, _ := strconv.ParseInt(data[structPos:structPos+64], 16, 64)
		if success == 0 {
			results[i].Error = "call failed"
			continue
		}

		// Read offset to returnData (relative to struct start)
		returnDataOffset, _ := strconv.ParseInt(data[structPos+64:structPos+128], 16, 64)
		returnDataPos := structPos + int(returnDataOffset)*2

		if returnDataPos < 0 || returnDataPos+64 > len(data) {
			results[i].Error = fmt.Sprintf("returnData offset out of bounds: pos=%d, len=%d", returnDataPos, len(data))
			continue
		}

		// Read returnData length
		returnDataLen, _ := strconv.ParseInt(data[returnDataPos:returnDataPos+64], 16, 64)
		returnDataStart := returnDataPos + 64
		returnDataEnd := returnDataStart + int(returnDataLen)*2

		if returnDataStart < 0 || returnDataEnd < returnDataStart || returnDataEnd > len(data) {
			results[i].Error = fmt.Sprintf("returnData truncated: start=%d, end=%d, len=%d", returnDataStart, returnDataEnd, len(data))
			continue
		}

		returnDataHex := data[returnDataStart:returnDataEnd]

		// Decode all 6 fields from getUserAccountData
		if err := decodeUserAccountData(&results[i], returnDataHex); err != nil {
			results[i].Error = err.Error()
			continue
		}
	}

	return results, nil
}

func decodeUserAccountData(result *UserAccountData, data string) error {
	// getUserAccountData returns 6 uint256 values (192 bytes = 384 hex chars):
	// 0: totalCollateralBase
	// 1: totalDebtBase
	// 2: availableBorrowsBase
	// 3: currentLiquidationThreshold
	// 4: ltv
	// 5: healthFactor
	if len(data) < 384 {
		return fmt.Errorf("response too short: %d chars", len(data))
	}

	var err error
	result.TotalCollateralBase, err = decodeUint256(data, 0)
	if err != nil {
		return fmt.Errorf("totalCollateralBase: %w", err)
	}

	result.TotalDebtBase, err = decodeUint256(data, 1)
	if err != nil {
		return fmt.Errorf("totalDebtBase: %w", err)
	}

	result.AvailableBorrowsBase, err = decodeUint256(data, 2)
	if err != nil {
		return fmt.Errorf("availableBorrowsBase: %w", err)
	}

	result.CurrentLiquidationThreshold, err = decodeUint256(data, 3)
	if err != nil {
		return fmt.Errorf("currentLiquidationThreshold: %w", err)
	}

	result.Ltv, err = decodeUint256(data, 4)
	if err != nil {
		return fmt.Errorf("ltv: %w", err)
	}

	result.HealthFactor, err = decodeUint256(data, 5)
	if err != nil {
		return fmt.Errorf("healthFactor: %w", err)
	}

	return nil
}

func formatHealthFactor(hf *big.Int) string {
	maxUint256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	if hf.Cmp(maxUint256) == 0 {
		return "infinity"
	}

	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	whole := new(big.Int).Div(hf, divisor)
	remainder := new(big.Int).Mod(hf, divisor)
	remainderStr := fmt.Sprintf("%018d", remainder)
	return fmt.Sprintf("%s.%s", whole.String(), remainderStr[:4])
}

func readUsersFromFile(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var users []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || !strings.HasPrefix(line, "0x") {
			continue
		}
		users = append(users, line)
	}
	return users, scanner.Err()
}

func writeCSV(results []UserAccountData, filename string, block int64) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Header with all getUserAccountData fields
	fmt.Fprintln(file, "user,total_collateral_base,total_debt_base,available_borrows_base,liquidation_threshold,ltv,health_factor,health_factor_decimal,error")
	for _, r := range results {
		if r.Error != "" {
			fmt.Fprintf(file, "%s,,,,,,,,%s\n", r.User, r.Error)
			continue
		}
		fmt.Fprintf(file, "%s,%s,%s,%s,%s,%s,%s,%s,\n",
			r.User,
			r.TotalCollateralBase.String(),
			r.TotalDebtBase.String(),
			r.AvailableBorrowsBase.String(),
			r.CurrentLiquidationThreshold.String(),
			r.Ltv.String(),
			r.HealthFactor.String(),
			formatHealthFactor(r.HealthFactor))
	}
	return nil
}
