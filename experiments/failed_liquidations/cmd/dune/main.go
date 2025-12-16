package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// SparkLend Pool contract on Ethereum mainnet
	SparkLendPool = "0xC13e21B648A5Ee794902342038FF3aDAB66BE987"

	// liquidationCall function selector
	LiquidationCallSelector = "0x00a718a9"

	// Dune API endpoints
	DuneAPIBase = "https://api.dune.com/api/v1"

	// Default timeout for query execution
	DefaultTimeout = 600 * time.Second
)

// SQL query to find all failed liquidationCall traces
var DuneQuery = `
-- Find all failed liquidationCall attempts to SparkLend Pool
-- This includes both direct calls and internal calls from other contracts

SELECT 
    t.block_number,
    t.block_time,
    t.tx_hash,
    t."from" as call_from,
    t."to" as call_to,
    t.trace_address,
    CASE 
        WHEN cardinality(t.trace_address) = 0 THEN 'direct'
        ELSE 'internal'
    END as call_type,
    cardinality(t.trace_address) as call_depth,
    -- Decode liquidationCall parameters from input (skip 4-byte selector)
    substr(t.input, 17, 20) as collateral_asset,
    substr(t.input, 49, 20) as debt_asset,
    substr(t.input, 81, 20) as user,
    t.gas_used,
    t.success,
    t.error as revert_reason
FROM ethereum.traces t
WHERE 
    t."to" = 0xC13e21B648A5Ee794902342038FF3aDAB66BE987
    AND substr(t.input, 1, 4) = 0x00a718a9
    AND t.block_number >= 16932378  -- SparkLend deployment
    AND (t.success = false OR t.error IS NOT NULL)
ORDER BY t.block_number ASC
`

// DuneClient is a simple Dune Analytics API client
type DuneClient struct {
	apiKey     string
	httpClient *http.Client
}

// ExecuteResponse represents the response from query execution
type ExecuteResponse struct {
	ExecutionID string `json:"execution_id"`
	State       string `json:"state"`
}

// ResultsResponse represents the query results
type ResultsResponse struct {
	ExecutionID string `json:"execution_id"`
	State       string `json:"state"`
	Result      struct {
		Rows     []map[string]interface{} `json:"rows"`
		Metadata struct {
			ColumnNames []string `json:"column_names"`
			RowCount    int      `json:"row_count"`
		} `json:"metadata"`
	} `json:"result"`
	Error interface{} `json:"error"` // Can be string or object
}

// FailedLiquidation represents a failed liquidation record
type FailedLiquidation struct {
	BlockNumber     int64  `json:"block_number"`
	BlockTime       string `json:"block_time"`
	TxHash          string `json:"tx_hash"`
	CallFrom        string `json:"call_from"`
	CallTo          string `json:"call_to"`
	CallType        string `json:"call_type"`
	CallDepth       int    `json:"call_depth"`
	CollateralAsset string `json:"collateral_asset"`
	DebtAsset       string `json:"debt_asset"`
	User            string `json:"user"`
	GasUsed         int64  `json:"gas_used"`
	Success         bool   `json:"success"`
	RevertReason    string `json:"revert_reason"`
}

// NewDuneClient creates a new Dune API client
func NewDuneClient(apiKey string) *DuneClient {
	return &DuneClient{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// ExecuteQuery executes a SQL query and waits for results
func (c *DuneClient) ExecuteQuery(query string, timeout time.Duration) (*ResultsResponse, error) {
	// Execute the query
	executionID, err := c.executeInlineQuery(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	fmt.Printf("Query execution started (ID: %s)\n", executionID)

	// Wait for results
	return c.waitForResults(executionID, timeout)
}

func (c *DuneClient) executeInlineQuery(query string) (string, error) {
	url := fmt.Sprintf("%s/sql/execute", DuneAPIBase)

	payload := map[string]interface{}{
		"sql":         query,
		"performance": "medium",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}

	req.Header.Set("X-Dune-API-Key", c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	var result ExecuteResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	return result.ExecutionID, nil
}

func (c *DuneClient) waitForResults(executionID string, timeout time.Duration) (*ResultsResponse, error) {
	url := fmt.Sprintf("%s/execution/%s/results", DuneAPIBase, executionID)

	startTime := time.Now()
	pollInterval := 2 * time.Second

	for {
		if time.Since(startTime) > timeout {
			return nil, fmt.Errorf("query execution timed out after %v", timeout)
		}

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}

		req.Header.Set("X-Dune-API-Key", c.apiKey)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, err
		}

		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(bodyBytes))
		}

		var result ResultsResponse
		if err := json.Unmarshal(bodyBytes, &result); err != nil {
			return nil, err
		}

		switch result.State {
		case "QUERY_STATE_COMPLETED":
			return &result, nil
		case "QUERY_STATE_FAILED":
			return nil, fmt.Errorf("query failed: %v", result.Error)
		case "QUERY_STATE_PENDING", "QUERY_STATE_EXECUTING":
			fmt.Printf("  Query state: %s, waiting...\n", result.State)
			time.Sleep(pollInterval)
			// Exponential backoff up to 30 seconds
			if pollInterval < 30*time.Second {
				pollInterval = time.Duration(float64(pollInterval) * 1.5)
			}
		default:
			fmt.Printf("  Unknown state: %s, waiting...\n", result.State)
			time.Sleep(pollInterval)
		}
	}
}

// parseResults converts raw results to FailedLiquidation structs
func parseResults(results *ResultsResponse) []FailedLiquidation {
	var liquidations []FailedLiquidation

	for _, row := range results.Result.Rows {
		liq := FailedLiquidation{}

		if v, ok := row["block_number"].(float64); ok {
			liq.BlockNumber = int64(v)
		}
		if v, ok := row["block_time"].(string); ok {
			liq.BlockTime = v
		}
		if v, ok := row["tx_hash"].(string); ok {
			liq.TxHash = v
		}
		if v, ok := row["call_from"].(string); ok {
			liq.CallFrom = v
		}
		if v, ok := row["call_to"].(string); ok {
			liq.CallTo = v
		}
		if v, ok := row["call_type"].(string); ok {
			liq.CallType = v
		}
		if v, ok := row["call_depth"].(float64); ok {
			liq.CallDepth = int(v)
		}
		if v, ok := row["collateral_asset"].(string); ok {
			liq.CollateralAsset = v
		}
		if v, ok := row["debt_asset"].(string); ok {
			liq.DebtAsset = v
		}
		if v, ok := row["user"].(string); ok {
			liq.User = v
		}
		if v, ok := row["gas_used"].(float64); ok {
			liq.GasUsed = int64(v)
		}
		if v, ok := row["success"].(bool); ok {
			liq.Success = v
		}
		if v, ok := row["revert_reason"].(string); ok {
			liq.RevertReason = v
		}

		liquidations = append(liquidations, liq)
	}

	return liquidations
}

// saveToCSV saves liquidations to a CSV file
func saveToCSV(liquidations []FailedLiquidation, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"block_number",
		"block_time",
		"tx_hash",
		"call_from",
		"call_to",
		"call_type",
		"call_depth",
		"collateral_asset",
		"debt_asset",
		"user",
		"gas_used",
		"success",
		"revert_reason",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write rows
	for _, liq := range liquidations {
		row := []string{
			strconv.FormatInt(liq.BlockNumber, 10),
			liq.BlockTime,
			liq.TxHash,
			liq.CallFrom,
			liq.CallTo,
			liq.CallType,
			strconv.Itoa(liq.CallDepth),
			liq.CollateralAsset,
			liq.DebtAsset,
			liq.User,
			strconv.FormatInt(liq.GasUsed, 10),
			strconv.FormatBool(liq.Success),
			liq.RevertReason,
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}

// saveToJSON saves liquidations to a JSON file
func saveToJSON(liquidations []FailedLiquidation, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(liquidations)
}

// printSummary prints a summary of the results
func printSummary(liquidations []FailedLiquidation) {
	if len(liquidations) == 0 {
		fmt.Println("No failed liquidations found!")
		return
	}

	fmt.Println()
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("Summary")
	fmt.Println(strings.Repeat("=", 60))

	// Count by call type
	direct := 0
	internal := 0
	for _, liq := range liquidations {
		if liq.CallType == "direct" {
			direct++
		} else {
			internal++
		}
	}
	fmt.Printf("Direct calls:   %d\n", direct)
	fmt.Printf("Internal calls: %d\n", internal)

	// Block range
	minBlock := liquidations[0].BlockNumber
	maxBlock := liquidations[0].BlockNumber
	for _, liq := range liquidations {
		if liq.BlockNumber < minBlock {
			minBlock = liq.BlockNumber
		}
		if liq.BlockNumber > maxBlock {
			maxBlock = liq.BlockNumber
		}
	}
	fmt.Printf("Block range:    %d to %d\n", minBlock, maxBlock)

	// Top revert reasons
	reasons := make(map[string]int)
	for _, liq := range liquidations {
		reason := liq.RevertReason
		if reason == "" {
			reason = "unknown"
		}
		// Truncate long reasons
		if len(reason) > 50 {
			reason = reason[:50] + "..."
		}
		reasons[reason]++
	}

	// Sort by count
	type reasonCount struct {
		reason string
		count  int
	}
	var sortedReasons []reasonCount
	for reason, count := range reasons {
		sortedReasons = append(sortedReasons, reasonCount{reason, count})
	}
	sort.Slice(sortedReasons, func(i, j int) bool {
		return sortedReasons[i].count > sortedReasons[j].count
	})

	fmt.Println("\nTop revert reasons:")
	for i, rc := range sortedReasons {
		if i >= 5 {
			break
		}
		fmt.Printf("  %4dx %s\n", rc.count, rc.reason)
	}
}

func main() {
	// Command line flags
	outputFile := flag.String("output", "failed_liquidations_dune.csv", "Output CSV file")
	outputJSON := flag.Bool("json", false, "Also save results as JSON")
	apiKey := flag.String("api-key", "", "Dune API key (or set DUNE_API_KEY env var)")
	showQuery := flag.Bool("show-query", false, "Print the SQL query and exit")
	timeout := flag.Duration("timeout", DefaultTimeout, "Query execution timeout")

	flag.Parse()

	if *showQuery {
		fmt.Println("SQL Query:")
		fmt.Println(strings.Repeat("-", 60))
		fmt.Println(DuneQuery)
		return
	}

	// Get API key
	key := *apiKey
	if key == "" {
		key = os.Getenv("DUNE_API_KEY")
	}
	if key == "" {
		fmt.Println("Error: Dune API key required.")
		fmt.Println("Set DUNE_API_KEY environment variable or use -api-key flag")
		fmt.Println("\nGet your API key from: https://dune.com/settings/api")
		os.Exit(1)
	}

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("SparkLend Failed Liquidations - Dune Analytics Query")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Contract: %s\n", SparkLendPool)
	fmt.Printf("Method:   liquidationCall (%s)\n", LiquidationCallSelector)
	fmt.Printf("Output:   %s\n", *outputFile)
	fmt.Printf("Timeout:  %v\n", *timeout)
	fmt.Println(strings.Repeat("=", 60))

	// Create client and execute query
	client := NewDuneClient(key)

	fmt.Println("\nExecuting Dune query...")
	fmt.Println("(This may take a few minutes for full historical scan)")

	results, err := client.ExecuteQuery(DuneQuery, *timeout)
	if err != nil {
		fmt.Printf("\nError: %v\n", err)
		os.Exit(1)
	}

	// Parse results
	liquidations := parseResults(results)
	fmt.Printf("\nFound %d failed liquidation records\n", len(liquidations))

	// Save to CSV
	fmt.Printf("Saving to %s...\n", *outputFile)
	if err := saveToCSV(liquidations, *outputFile); err != nil {
		fmt.Printf("Error saving CSV: %v\n", err)
		os.Exit(1)
	}

	// Optionally save to JSON
	if *outputJSON {
		jsonFile := strings.TrimSuffix(*outputFile, ".csv") + ".json"
		fmt.Printf("Saving to %s...\n", jsonFile)
		if err := saveToJSON(liquidations, jsonFile); err != nil {
			fmt.Printf("Error saving JSON: %v\n", err)
			os.Exit(1)
		}
	}

	// Print summary
	printSummary(liquidations)

	fmt.Println("\nDone!")
}
