UIKIT_REPO ?= https://github.com/archon-research/uikit.git
UIKIT_PLUGIN_SHA ?= 98ea9e55edae3397c43421582126dc9742a8fe7b
PLUGIN_SUBDIR := packages/agent-marketplace/copilot-plugin
COPILOT_MARKETPLACE_DIR := .copilot-marketplace
COPILOT_PLUGIN_DIR := $(COPILOT_MARKETPLACE_DIR)/uikit-agent-marketplace
CLONE_DIR := $(COPILOT_MARKETPLACE_DIR)/.cache/uikit-plugin-src

.PHONY: copilot-plugin-sync-from-sha
copilot-plugin-sync-from-sha:
	@echo "Syncing Copilot plugin from $(UIKIT_REPO) (sha=$(UIKIT_PLUGIN_SHA))"
	@rm -rf "$(CLONE_DIR)"
	@mkdir -p "$(CLONE_DIR)"
	@git clone --depth 1 --filter=blob:none --sparse "$(UIKIT_REPO)" "$(CLONE_DIR)"
	@cd "$(CLONE_DIR)" && git sparse-checkout set --no-cone "$(PLUGIN_SUBDIR)"
	@cd "$(CLONE_DIR)" && git fetch --depth 1 origin "$(UIKIT_PLUGIN_SHA)" && git checkout --detach FETCH_HEAD || { \
		echo "Failed to fetch UIKIT_PLUGIN_SHA=$(UIKIT_PLUGIN_SHA)."; \
		echo "Make sure that commit is pushed to GitHub."; \
		exit 1; \
	}
	@rm -rf "$(COPILOT_PLUGIN_DIR)"
	@mkdir -p "$(COPILOT_MARKETPLACE_DIR)"
	@cp -R "$(CLONE_DIR)/$(PLUGIN_SUBDIR)" "$(COPILOT_PLUGIN_DIR)"
	@printf '{\n  "name": "uikit-plugins",\n  "owner": {\n    "name": "Archon Research"\n  },\n  "plugins": [\n    {\n      "name": "uikit-agent-marketplace",\n      "version": "0.1.0",\n      "description": "UI skills and specialist agents for code assistants.",\n      "source": "./uikit-agent-marketplace"\n    }\n  ]\n}\n' > "$(COPILOT_MARKETPLACE_DIR)/marketplace.json"
	@if ! copilot plugin marketplace list | rg -q "uikit-plugins"; then \
		copilot plugin marketplace add "$(COPILOT_MARKETPLACE_DIR)"; \
	fi
	@copilot plugin marketplace update uikit-plugins
	@copilot plugin install uikit-agent-marketplace@uikit-plugins || copilot plugin update uikit-agent-marketplace
	@copilot plugin list
	@echo "Done. Copilot plugin synced (sha=$(UIKIT_PLUGIN_SHA))."
