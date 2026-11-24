#!/usr/bin/env bash
# 切换到「离线本地化」Cargo 清单（仅适用于已本地化 zed/blade/font-kit 的内网 Linux 机器）。
set -euo pipefail
cd "$(dirname "$0")/.."
cp Cargo.offline.toml Cargo.toml
echo "已切换到离线本地化依赖（Cargo.offline.toml -> Cargo.toml）。"
echo "运行：cargo run --release"
