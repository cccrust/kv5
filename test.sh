#!/bin/bash

set -e

echo "=== kv5 測試腳本 ==="

echo "執行 cargo fmt..."
cargo fmt --check

echo "執行 cargo clippy..."
cargo clippy -- -D warnings

echo "執行 cargo test..."
cargo test

echo "=== 所有測試通過 ==="