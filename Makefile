# Repo-root Makefile: repo-wide tooling only.
#
# The Go service's canonical workflow Makefile is stl-verify/Makefile. This root
# Makefile stays deliberately thin: it wires in cross-cutting tooling that spans
# the whole repo — Skillfile-managed AI skills (see skills.mk), and the alert
# rule unit tests, whose subject (alerts/) sits outside every language subtree.

.DEFAULT_GOAL := help

include skills.mk

# promtool is pinned by release tag *and* archive checksum. It cannot come from
# `go install`: prometheus/prometheus carries replace directives in its go.mod,
# which module-aware install refuses, so the release archive is the only route.
PROMTOOL_VERSION := 3.14.0
PROMTOOL_SHA256_darwin-amd64 := a14307b9726e66cadb81be9a544732623af26dabeb7702c987aa9c3c062ada34
PROMTOOL_SHA256_darwin-arm64 := a9623f7f4fe65b1b171b423c1a72bbf23dfdf41a171dcb33e7dd302af80dc01c
PROMTOOL_SHA256_linux-amd64 := f665c6da19eb7ba399c915d30c7d9793c9b417bf8a749b504bc470678631478d
PROMTOOL_SHA256_linux-arm64 := 077f3781ab7245dc04c9a3c9b78ba120fc8e41aa0dc97489b0af67247e50ba83
PROMTOOL_PLATFORM := $(shell uname -s | tr '[:upper:]' '[:lower:]')-$(shell uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')
PROMTOOL ?= $(shell go env GOPATH)/bin/promtool

.PHONY: help promtool test-alerts
help: ## Show available targets
	@grep -hE "^[a-zA-Z0-9_-]+:.*##" $(MAKEFILE_LIST) | sed -E "s/:[^#]*## / -- /" | sort

promtool: ## Install the pinned promtool
	@if "$(PROMTOOL)" --version 2>/dev/null | grep -q "version $(PROMTOOL_VERSION) "; then \
		echo "==> promtool $(PROMTOOL_VERSION) already installed"; exit 0; \
	fi; \
	sha="$(PROMTOOL_SHA256_$(PROMTOOL_PLATFORM))"; \
	if [ -z "$$sha" ]; then \
		echo "ERROR: no pinned promtool checksum for platform '$(PROMTOOL_PLATFORM)'"; exit 1; \
	fi; \
	echo "==> Installing promtool $(PROMTOOL_VERSION) ($(PROMTOOL_PLATFORM))..."; \
	dir="prometheus-$(PROMTOOL_VERSION).$(PROMTOOL_PLATFORM)"; \
	tmp="$$(mktemp -d)"; \
	trap 'rm -rf "$$tmp"' EXIT; \
	curl -fsSL -o "$$tmp/archive.tar.gz" \
		"https://github.com/prometheus/prometheus/releases/download/v$(PROMTOOL_VERSION)/$$dir.tar.gz"; \
	if command -v sha256sum >/dev/null 2>&1; then \
		echo "$$sha  $$tmp/archive.tar.gz" | sha256sum -c - >/dev/null; \
	else \
		echo "$$sha  $$tmp/archive.tar.gz" | shasum -a 256 -c - >/dev/null; \
	fi; \
	tar -xzf "$$tmp/archive.tar.gz" -C "$$tmp" "$$dir/promtool"; \
	mkdir -p "$$(dirname "$(PROMTOOL)")"; \
	install -m 0755 "$$tmp/$$dir/promtool" "$(PROMTOOL)"

test-alerts: promtool ## Evaluate the shipped alert rules against synthetic series
	@echo "==> Checking alert rule syntax..."
	$(PROMTOOL) check rules alerts/*.yaml
	@echo "==> Unit-testing alert rules..."
	$(PROMTOOL) test rules alerts-tests/*.yaml
