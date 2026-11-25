#!/usr/bin/env bash
# Installs the public deployment pins and moves a downloaded bot key into the
# bot's private folder with owner-only permissions. Never prints the key.
# Usage: ./setup_key.sh [/mnt/c/Users/<you>/Downloads/<key-file>.json]
set -euo pipefail
cd "$(dirname "$(readlink -f "$0")")"
umask 077
mkdir -p private private/continuity logs
chmod 700 private private/continuity

PINS_SRC=kit/experimental/native-testnet/fairvenue_schema6/deployments/arbitrum-sepolia-ts2.json
PINS_SHA=ba25da81e3ebc9f6679a184993dbc4a64fd089684f54e174e8ad654b725232d4
if [ ! -e private/installation.json ]; then
  echo "$PINS_SHA  $PINS_SRC" | sha256sum -c --quiet
  cp "$PINS_SRC" private/installation.json
  chmod 600 private/installation.json
  echo "pins installed"
fi

if [ -e private/agent-key.json ]; then
  echo "a bot key is already installed (private/agent-key.json); not overwriting"
  exit 0
fi

SRC="${1:-}"
if [ -z "$SRC" ]; then
  # Newest file in the Windows Downloads folder that carries the TS2 key schema.
  SRC=$(grep -l -s '"fairvenue-ts2-testnet-agent-v1"' /mnt/c/Users/*/Downloads/*.json 2>/dev/null \
        | xargs -r ls -t 2>/dev/null | head -1 || true)
fi
if [ -z "$SRC" ] || [ ! -f "$SRC" ]; then
  echo "no TS2 bot key found. Create one at https://app.fairvenue.xyz/?setup=api-key#native-agent-key"
  echo "then run: ./setup_key.sh /mnt/c/Users/<you>/Downloads/<file>.json"
  exit 1
fi
grep -q '"fairvenue-ts2-testnet-agent-v1"' "$SRC" || { echo "not a TS2 season 2 bot key file"; exit 1; }
install -m 600 "$SRC" private/agent-key.json
echo "key installed from: $SRC"
echo "The downloaded copy is still in Downloads. Delete it yourself once the bot works."
