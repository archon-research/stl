.PHONY: build run clean

# Build the binary
build:
	go build -o get_all_users main.go

# Run the query (requires DUNE_API_KEY)
run:
	go run main.go

# Run with JSON output
run-json:
	go run main.go -json sparklend_users.json

# Clean build artifacts
clean:
	rm -f get_all_users sparklend_users.csv sparklend_users.json
