#!/usr/bin/env bash
# 切换到「可移植」Cargo 清单（上游 git 依赖，Windows / 联网 Linux 通用）。
set -euo pipefail
cd "$(dirname "$0")/.."
cp Cargo.portable.toml Cargo.toml
echo "已切换到可移植依赖（Cargo.portable.toml -> Cargo.toml）。"
echo "需要能访问 GitHub 拉取 gpui（zed v0.195.5）。"
