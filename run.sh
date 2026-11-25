#!/usr/bin/env bash
# Usage: ./run.sh            dry run with the key (signed reads only, no orders)
#        ./run.sh --paper    public data only, simulated fills
#        ./run.sh --live     trade on the TESTNET; restarts the bot if it crashes
set -uo pipefail
cd "$(dirname "$(readlink -f "$0")")"
. .venv/bin/activate
if [ "${1:-}" != "--live" ]; then
  exec python bot.py "$@"
fi
while true; do
  python bot.py --live
  code=$?
  # 0 or 130: stopped on purpose (Ctrl+C). Anything else: restart after a pause.
  if [ $code -eq 0 ] || [ $code -eq 130 ]; then break; fi
  echo "bot exited with $code, restarting in 10 s (Ctrl+C to stop)"
  sleep 10 || break
done
