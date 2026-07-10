.PHONY: \
	skillfile-install-cli \
	skillfile-update-cli \
	skills-install \
	skills-list \
	skills-update \
	skills-install-dry-run \
	skills-validate

SKILLFILE_BIN := ./bin/skillfile
SKILLFILE_RELEASE ?= latest
SKILLFILE_FORCE_INSTALL ?= 0
SKILLFILE_MANIFEST := ./Skillfile

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

ifeq ($(UNAME_S),Darwin)
SKILLFILE_OS := macos
else ifeq ($(UNAME_S),Linux)
SKILLFILE_OS := linux
else
SKILLFILE_OS := unsupported
endif

ifeq ($(UNAME_M),arm64)
SKILLFILE_ARCH := aarch64
else ifeq ($(UNAME_M),aarch64)
SKILLFILE_ARCH := aarch64
else ifeq ($(UNAME_M),x86_64)
SKILLFILE_ARCH := x86_64
else ifeq ($(UNAME_M),amd64)
SKILLFILE_ARCH := x86_64
else
SKILLFILE_ARCH := unsupported
endif

skillfile-install-cli: ## Install skillfile CLI to ./bin from GitHub Releases
	@set -e; \
	if [ "$(SKILLFILE_OS)" = "unsupported" ] || [ "$(SKILLFILE_ARCH)" = "unsupported" ]; then \
		echo "Unsupported platform: $(UNAME_S)/$(UNAME_M)"; \
		exit 1; \
	fi; \
	if [ "$(SKILLFILE_FORCE_INSTALL)" != "1" ] && [ -x "$(SKILLFILE_BIN)" ]; then \
		echo "Using existing $(SKILLFILE_BIN)"; \
		exit 0; \
	fi; \
	mkdir -p ./bin; \
	ASSET="skillfile-$(SKILLFILE_ARCH)-$(SKILLFILE_OS)"; \
	if [ "$(SKILLFILE_RELEASE)" = "latest" ]; then \
		URL="https://github.com/eljulians/skillfile/releases/latest/download/$$ASSET"; \
	else \
		URL="https://github.com/eljulians/skillfile/releases/download/$(SKILLFILE_RELEASE)/$$ASSET"; \
	fi; \
	echo "Downloading $$URL"; \
	curl -fsSL "$$URL" -o "$(SKILLFILE_BIN)"; \
	chmod +x "$(SKILLFILE_BIN)"; \
	echo "Installed $(SKILLFILE_BIN)"

skillfile-update-cli: ## Update skillfile CLI (alias for install, defaults to latest)
	@$(MAKE) SKILLFILE_FORCE_INSTALL=1 skillfile-install-cli

skills-install: skillfile-install-cli ## Fetch, lock, and deploy skills to configured platforms
	@$(SKILLFILE_BIN) install

skills-list: skillfile-install-cli ## Show skill status from Skillfile/Skillfile.lock
	@$(SKILLFILE_BIN) status

skills-update: skillfile-install-cli ## Update upstream refs and redeploy to configured platforms
	@$(SKILLFILE_BIN) install --update

skills-install-dry-run: skillfile-install-cli ## Preview install/update changes without writing files
	@$(SKILLFILE_BIN) install --dry-run

skills-validate: skillfile-install-cli ## Validate and normalize Skillfile manifest
	@$(SKILLFILE_BIN) validate
	@$(SKILLFILE_BIN) format --dry-run
