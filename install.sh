#!/bin/bash
# Builds whatsapp-forwarder and (re)starts it as a launchd agent.
# Logs: ~/Library/Logs/whatsapp-forwarder.log (panics: whatsapp-forwarder.crash.log)
set -euo pipefail
export PATH="/opt/homebrew/bin:/Users/vojto/.local/share/mise/shims:$PATH"

cd "$(dirname "$0")"
label="com.vojto.whatsapp-forwarder"
plist="$HOME/Library/LaunchAgents/$label.plist"

[[ -f .env ]] || { echo "Missing .env, copy .env.example and fill it in" >&2; exit 1; }

go test ./...
go build -o whatsapp-forwarder .
cp "$label.plist" "$plist"
launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true
# bootout returns before the old process is gone; bootstrap fails with error 5 until it is.
while launchctl print "gui/$(id -u)/$label" >/dev/null 2>&1; do sleep 0.5; done
launchctl bootstrap "gui/$(id -u)" "$plist"
echo "Started $label"
