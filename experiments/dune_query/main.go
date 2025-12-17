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
	"time"
)

const (
	SparkLendPool  = "0xC13e21B648A5Ee794902342038FF3aDAB66BE987"
	DuneAPIBase    = "https://api.dune.com/api/v1"
	DefaultTimeout = 600 * time.Second
)

var DuneQueryTemplate = `
SELECT
    block_number,
    block_time,
    tx_hash,
    CONCAT('0x', SUBSTR(CAST(topic1 AS VARCHAR), 27)) as reserve,
    CONCAT('0x', SUBSTR(CAST(topic2 AS VARCHAR), 27)) as user,
    bytearray_to_uint256(bytearray_substring(data, 1, 32)) as amount
FROM ethereum.logs
WHERE contract_address = 0xC13e21B648A5Ee794902342038FF3aDAB66BE987
AND topic0 = 0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61
AND topic2 = 0x%s
ORDER BY block_number ASC
`

type ExecuteResponse struct {
	ExecutionID string `json:"execution_id"`
}

type ResultsResponse struct {
	State  string `json:"state"`
	Result struct {
		Rows     []map[string]interface{} `json:"rows"`
		Metadata struct {
			RowCount int `json:"row_count"`
		} `json:"metadata"`
	} `json:"result"`
	Error interface{} `json:"error"`
}

type SupplyEvent struct {
	BlockNumber int64
	BlockTime   string
	TxHash      string
	Reserve     string
	User        string
	Amount      string
}

func main() {
	user := flag.String("user", "0x000000093e55f433fb57a32aa5d5fe717b3f7ab1", "User address")
	output := flag.String("output", "supply_events.csv", "Output file")
	timeout := flag.Duration("timeout", DefaultTimeout, "Timeout")
	flag.Parse()

	apiKey := os.Getenv("DUNE_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "Error: DUNE_API_KEY required")
		os.Exit(1)
	}

	userAddr := *user
	if len(userAddr) > 2 && userAddr[:2] == "0x" {
		userAddr = userAddr[2:]
	}
	paddedAddr := fmt.Sprintf("%064s", userAddr)
	query := fmt.Sprintf(DuneQueryTemplate, paddedAddr)

	fmt.Printf("Querying supply events for: %s\n", *user)

	execID, err := executeQuery(apiKey, query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Execution ID: %s\n", execID)

	results, err := waitForResults(apiKey, execID, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Found %d supply events\n", results.Result.Metadata.RowCount)

	events := parseEvents(results)
	if err := writeCSV(events, *output); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Written to %s\n", *output)
}

func parseEvents(results *ResultsResponse) []SupplyEvent {
	var events []SupplyEvent
	for _, row := range results.Result.Rows {
		e := SupplyEvent{}
		if v, ok := row["block_number"].(float64); ok {
			e.BlockNumber = int64(v)
		}
		if v, ok := row["block_time"].(string); ok {
			e.BlockTime = v
		}
		if v, ok := row["tx_hash"].(string); ok {
			e.TxHash = v
		}
		if v, ok := row["reserve"].(string); ok {
			e.Reserve = v
		}
		if v, ok := row["user"].(string); ok {
			e.User = v
		}
		if v, ok := row["amount"].(string); ok {
			e.Amount = v
		} else if v, ok := row["amount"].(float64); ok {
			e.Amount = fmt.Sprintf("%.0f", v)
		}
		events = append(events, e)
	}
	return events
}

func writeCSV(events []SupplyEvent, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	w := csv.NewWriter(file)
	defer w.Flush()

	w.Write([]string{"block_number", "block_time", "tx_hash", "reserve", "user", "amount"})
	for _, e := range events {
		w.Write([]string{
			fmt.Sprintf("%d", e.BlockNumber),
			e.BlockTime,
			e.TxHash,
			e.Reserve,
			e.User,
			e.Amount,
		})
	}
	return nil
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
			return nil, fmt.Errorf("failed: %v", result.Error)
		default:
			fmt.Printf("State: %s (%.0fs)\n", result.State, time.Since(start).Seconds())
			time.Sleep(2 * time.Second)
		}
	}
}
