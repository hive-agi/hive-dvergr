#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
: "${HIVE_WASMTIME:?Set HIVE_WASMTIME to an absolute Wasmtime executable path}"
: "${HIVE_CLJW_WASM:?Set HIVE_CLJW_WASM to your built cljw.wasm}"
export HIVE_WASMTIME HIVE_CLJW_WASM
export HIVE_SANDBOX_SENTINEL=must-not-reach-guest
clojure -Sdeps '{:paths ["src" "resources" "test"]}' -M -m hive-dvergr.sandbox-test
clojure -Sdeps '{:paths ["src" "resources" "test"]}' -M -m hive-dvergr.sandbox-integration-test
