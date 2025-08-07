#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"


echo "Syncing to host: $HOST"

# sync to build server, ignoring local builds and local/remote dev ledger
rsync -avh --delete --exclude target --exclude docker-output "$SCRIPT_DIR" core@ny-testnet-validator-3:~/
