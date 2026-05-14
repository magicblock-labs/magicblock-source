.PHONY: help build build-plugin launch init-config clean

MAKEFILE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
	PLUGIN_EXT := so
endif
ifeq ($(UNAME_S),Darwin)
	PLUGIN_EXT := dylib
endif
PLUGIN_PATH := ../target/release/libsolana_accountsdb_plugin_kafka.$(PLUGIN_EXT)
VALIDATOR_CONFIG ?= plugin-config.json
PLUGIN_CONFIG ?= plugin-config.toml

help:
	@echo "Available targets:"
	@echo "  make init-config  - Create local validator JSON + plugin TOML configs"
	@echo "  make build-plugin - Compile the plugin (.so/.dylib)"
	@echo "  make launch       - Launch solana-test-validator with the plugin (depends on build-plugin)"
	@echo "  make clean        - Remove compiled artifacts"

build-plugin:
	@echo "Building plugin for $(UNAME_S)..."
	cargo build --release

launch: build-plugin
	@echo "Launching solana-test-validator with plugin..."
	@if [ ! -f "$(VALIDATOR_CONFIG)" ]; then \
		echo "Error: $(VALIDATOR_CONFIG) not found"; \
		echo "Please create the validator config first"; \
		exit 1; \
	fi
	@if [ ! -f "$(PLUGIN_CONFIG)" ]; then \
		cf=$$(perl -0ne 'print $$1 if /"config_file"\s*:\s*"([^"]+)"/' "$(VALIDATOR_CONFIG)"); \
		if [ -z "$$cf" ] || [ ! -f "$$cf" ]; then \
			echo "Error: plugin config not found"; \
			echo "  Tried $(PLUGIN_CONFIG) and config_file from $(VALIDATOR_CONFIG) ($$cf)"; \
			echo "Please create a plugin config file first"; \
			exit 1; \
		fi; \
	fi
	cd "$(MAKEFILE_DIR)" && \
	solana-test-validator --log --reset --geyser-plugin-config "$(VALIDATOR_CONFIG)"

init-config:
	@echo "Creating $(VALIDATOR_CONFIG) and $(PLUGIN_CONFIG) for $(UNAME_S) (.$(PLUGIN_EXT))..."
	@if [ -f "$(VALIDATOR_CONFIG)" ]; then \
		echo "Error: $(VALIDATOR_CONFIG) already exists, refusing to overwrite"; \
		exit 1; \
	fi
	@if [ -f "$(PLUGIN_CONFIG)" ]; then \
		echo "Error: $(PLUGIN_CONFIG) already exists, refusing to overwrite"; \
		exit 1; \
	fi
	@sed -e 's|path-to-plugin-library|$(PLUGIN_PATH)|' \
		-e 's|plugin-config.toml|$(PLUGIN_CONFIG)|' \
		./configs/plugin-config.example.json > "$(VALIDATOR_CONFIG)"
	@cp ./configs/plugin-config.example.toml "$(PLUGIN_CONFIG)"
	@echo "Created $(VALIDATOR_CONFIG) (libpath: $(PLUGIN_PATH))"
	@echo "Created $(PLUGIN_CONFIG)"

clean:
	cargo clean
	@echo "Cleaned build artifacts"

build: build-plugin
