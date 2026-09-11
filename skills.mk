.PHONY: \
	skillfile-install-cli \
	skillfile-update-cli \
	skills-install \
	skills-list \
	skills-update \
	skills-install-dry-run \
	skills-deploy-local \
	skills-validate

SKILLFILE_BIN := ./bin/skillfile
SKILLFILE_RELEASE ?= latest
SKILLFILE_FORCE_INSTALL ?= 0
SKILLFILE_MANIFEST := ./Skillfile
LOCAL_SKILL_SOURCES := $(shell awk '$$1 == "local" && $$2 == "skill" { print $$3 }' $(SKILLFILE_MANIFEST))
LOCAL_SKILLS := $(notdir $(LOCAL_SKILL_SOURCES))
SKILL_DEPLOY_TARGETS := .claude/skills .codex/skills .github/skills

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
	@$(MAKE) --no-print-directory skills-deploy-local

skills-list: skillfile-install-cli ## Show skill status from Skillfile/Skillfile.lock
	@$(SKILLFILE_BIN) status

skills-update: skillfile-install-cli ## Update upstream refs and redeploy to configured platforms
	@$(SKILLFILE_BIN) install --update
	@$(MAKE) --no-print-directory skills-deploy-local

skills-install-dry-run: skillfile-install-cli ## Preview install/update changes without writing files
	@$(SKILLFILE_BIN) install --dry-run

skills-deploy-local: ## Sync canonical local skills to every generated deployment target
	@set -e; \
	for source in $(LOCAL_SKILL_SOURCES); do \
		if [ ! -d "$$source" ]; then \
			echo "Local skill source is missing: $$source"; \
			exit 1; \
		fi; \
	done; \
	for target in $(SKILL_DEPLOY_TARGETS); do \
		managed="$$target/.skillfile-local-skills"; \
		if [ -f "$$managed" ]; then \
			while IFS= read -r skill; do \
				[ -z "$$skill" ] && continue; \
				case " $(LOCAL_SKILLS) " in *" $$skill "*) continue ;; esac; \
				case "$$skill" in */*|.|..) echo "Invalid managed skill name: $$skill"; exit 1 ;; esac; \
				rm -rf "$$target/$$skill"; \
			done < "$$managed"; \
		fi; \
		for source in $(LOCAL_SKILL_SOURCES); do \
			skill="$${source##*/}"; \
			mkdir -p "$$target/$$skill"; \
			rsync -a --delete "$$source/" "$$target/$$skill/"; \
		done; \
		printf '%s\n' $(LOCAL_SKILLS) > "$$managed"; \
	done

skills-validate: skillfile-install-cli ## Validate Skillfile and generated local skill copies
	@$(SKILLFILE_BIN) validate
	@set -e; \
	for source in $(LOCAL_SKILL_SOURCES); do \
		for target in $(SKILL_DEPLOY_TARGETS); do \
			deployed="$$target/$$(basename "$$source")"; \
			if [ ! -d "$$deployed" ] || ! diff -qr "$$source" "$$deployed" >/dev/null; then \
				echo "Skill deployment is stale: $$deployed (run make skills-install)"; \
				exit 1; \
			fi; \
		done; \
	done
# Orphan check disabled: remote-sourced skills (e.g. the github-hosted gh-stack)
# have no local source, so they would be flagged here. The stale-deployment loop
# above still guards local skills.
#	for deployed in .claude/skills/*; do \
#		[ -d "$$deployed" ] || continue; \
#		source="skills/$$(basename "$$deployed")"; \
#		if [ ! -d "$$source" ]; then \
#			echo "Orphaned Claude skill deployment: $$deployed"; \
#			exit 1; \
#		fi; \
#	done
	@$(SKILLFILE_BIN) format --dry-run
