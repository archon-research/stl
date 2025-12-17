package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	SparkLendPool  = "0xC13e21B648A5Ee794902342038FF3aDAB66BE987"
	DuneAPIBase    = "https://api.dune.com/api/v1"
	DefaultTimeout = 600 * time.Second
)

// Simple query - just get unique user addresses
var DuneQuery = `
SELECT DISTINCT
    CONCAT('0x', SUBSTR(CAST(topic2 AS VARCHAR), 27)) as user_address
FROM ethereum.logs
WHERE contract_address = 0xC13e21B648A5Ee794902342038FF3aDAB66BE987
AND topic0 IN (
    0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61,
    0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0,
    0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7,
    0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5571f6ed0e5a1c2d
)
ORDER BY user_address
`

type ExecuteResponse struct {
	ExecutionID string `json:"execution_id"`
	State       string `json:"state"`
}

type ResultsResponse struct {
	ExecutionID string `json:"execution_id"`
	State       string `json:"state"`
	Result      struct {
		Rows     []map[string]interface{} `json:"rows"`
		Metadata struct {
			RowCount int `json:"row_count"`
		} `json:"metadata"`
	} `json:"result"`
	Error interface{} `json:"error"`
}

func main() {
	output := flag.String("output", "sparklend_users.txt", "Output file")
	timeout := flag.Duration("timeout", DefaultTimeout, "Query timeout")
	flag.Parse()

	apiKey := os.Getenv("DUNE_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "Error: DUNE_API_KEY required")
		os.Exit(1)
	}

	fmt.Printf("Querying SparkLend users from %s\n", SparkLendPool)

	// Execute query
	execID, err := executeQuery(apiKey, DuneQuery)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Execution ID: %s\n", execID)

	// Wait for results
	results, err := waitForResults(apiKey, execID, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Extract addresses
	var addresses []string
	for _, row := range results.Result.Rows {
		if addr, ok := row["user_address"].(string); ok {
			addresses = append(addresses, addr)
		}
	}

	fmt.Printf("Found %d unique users\n", len(addresses))

	// Write to file
	file, err := os.Create(*output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	for _, addr := range addresses {
		fmt.Fprintln(file, addr)
	}

	fmt.Printf("Written to %s\n", *output)
}

func executeQuery(apiKey, query string) (string, error) {
	payload, _ := json.Marshal(map[string]interface{}{
		"sql":         query,
		"performance": "medium",
	})

	req, _ := http.NewRequest("POST", DuneAPIBase+"/sql/execute", bytes.NewReader(payload))
	req.Header.Set("X-Dune-API-Key", apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("API error %d: %s", resp.StatusCode, body)
	}

	var result ExecuteResponse
	json.NewDecoder(resp.Body).Decode(&result)
	return result.ExecutionID, nil
}

func waitForResults(apiKey, execID string, timeout time.Duration) (*ResultsResponse, error) {
	start := time.Now()

	for {
		if time.Since(start) > timeout {
			return nil, fmt.Errorf("timeout")
		}

		req, _ := http.NewRequest("GET", fmt.Sprintf("%s/execution/%s/results", DuneAPIBase, execID), nil)
		req.Header.Set("X-Dune-API-Key", apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var result ResultsResponse
		json.Unmarshal(body, &result)

		switch result.State {
		case "QUERY_STATE_COMPLETED":
			return &result, nil
		case "QUERY_STATE_FAILED":
			return nil, fmt.Errorf("query failed: %v", result.Error)
		default:
			fmt.Printf("State: %s (%.0fs)\n", result.State, time.Since(start).Seconds())
			time.Sleep(2 * time.Second)
		}
	}
}
