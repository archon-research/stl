.PHONY: build build-arm64 clean

# Default target - build for current platform
build:
	go build -o failed-tx-scanner .

# Build static Linux ARM64 binary for deployment
build-arm64:
	CGO_ENABLED=1 CC=aarch64-linux-gnu-gcc GOOS=linux GOARCH=arm64 \
		go build -ldflags='-extldflags "-static"' -o failed-tx-scanner-linux-arm64 .

clean:
	rm -f failed-tx-scanner failed-tx-scanner-linux-arm64
