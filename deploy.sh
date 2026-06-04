#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
install_root="/syncthing/Sync/Provisioning"
binary_path="$install_root/bin/zwave"

if [[ -L "$binary_path" ]]; then
    unlink "$binary_path"
fi

cargo install --path "$repo_dir" --root "$install_root" --force
