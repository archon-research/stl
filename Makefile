# Repo-root Makefile: repo-wide tooling only.
#
# The Go service's canonical workflow Makefile is stl-verify/Makefile. This root
# Makefile stays deliberately thin: it wires in cross-cutting tooling that spans
# the whole repo. Today that is just Skillfile-managed AI skills (see skills.mk).

.DEFAULT_GOAL := help

include skills.mk

.PHONY: help
help: ## Show available targets
	@grep -hE "^[a-zA-Z0-9_-]+:.*##" $(MAKEFILE_LIST) | sed -E "s/:[^#]*## / -- /" | sort
