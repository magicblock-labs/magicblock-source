.PHONY: help build build-plugin launch init-config clean

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
	PLUGIN_EXT := so
endif
ifeq ($(UNAME_S),Darwin)
	PLUGIN_EXT := dylib
endif
PLUGIN_PATH := target/release/libsolana_accountsdb_plugin_kafka.$(PLUGIN_EXT)
PLUGIN_CONFIG ?= plugin-config.json

help:
	@echo "Available targets:"
	@echo "  make init-config  - Create a local plugin config from configs/plugin-config.example.json"
	@echo "  make build-plugin - Compile the plugin (.so/.dylib)"
	@echo "  make launch       - Launch solana-test-validator with the plugin (depends on build-plugin)"
	@echo "  make clean        - Remove compiled artifacts"

init-config:
	@echo "Creating $(PLUGIN_CONFIG)..."
	@perl -0pe 's|target/release/libsolana_accountsdb_plugin_kafka\.(?:dylib|so)|$(PLUGIN_PATH)|g' configs/plugin-config.example.json > "$(PLUGIN_CONFIG)"
	@echo "✓ Created $(PLUGIN_CONFIG)"

build-plugin:
	@echo "Building plugin for $(UNAME_S)..."
	cargo build --release

launch: build-plugin
	@echo "Launching solana-test-validator with plugin..."
	@if [ ! -f "$(PLUGIN_CONFIG)" ]; then \
		echo "Error: $(PLUGIN_CONFIG) not found"; \
		echo "Please create a plugin config file first"; \
		exit 1; \
	fi
	solana-test-validator --log --reset --geyser-plugin-config "$(PLUGIN_CONFIG)"

clean:
	cargo clean
	@echo "Cleaned build artifacts"

build: build-plugin
