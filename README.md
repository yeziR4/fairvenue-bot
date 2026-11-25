# FairVenue hackathon bot (TESTNET, mock tokens)

Runs inside WSL (`Ubuntu-24.04`) at `~/fairvenue-bot`, because the FairVenue SDK is POSIX-only.
This Windows folder holds the source; the WSL copy is the one that runs.

## What it does

- **Stale-quote taker.** The reference liquidity provider quotes ±20 USDC around the *previous*
  oracle mark and re-quotes a few seconds after each 10-second mark update. When the fresh mark
  has moved past a resting quote by more than the taker fee plus `edge_open`, the bot takes it
  with a price-bounded LIMIT IOC. A miss costs nothing: the limit price is the stale quote.
- **Passive exit quote.** While it holds inventory it rests one post-only, reduce-only order
  `exit_half_spread` beyond the mark. Fills there count as maker volume.
- **Guards.** Inventory cap, per-minute command budget, back-off on `rate_limited`, a day-loss
  limit after which it only reduces, exact-original recovery before any new order, and
  cancel-all on shutdown. Resting orders live 90 seconds, so they expire by themselves if the
  bot dies.

## Commands (run inside WSL, in `~/fairvenue-bot`)

| Command | Effect |
| --- | --- |
| `./setup_key.sh` | Installs the pins and moves the newest downloaded bot key into `private/` (0600). Never prints it. |
| `./run.sh --paper` | Public data only, simulated fills. No key needed. |
| `./run.sh` | Dry run with the key: signed reads only, orders are logged, not sent. |
| `./run.sh --live` | Trades on the TESTNET and restarts after a crash. Ctrl+C stops it and cancels working orders. |
| `python test_bot.py` | Offline test of the order path against a fake client. |

Logs are in `logs/`: `bot-<mode>.log`, `fills.jsonl`, `equity-<mode>.csv` (one line a minute).

## Tuning (`config.json`)

`edge_open` (USDC per BTC beyond the fee to open), `edge_reduce`, `max_position`,
`max_order_qty`, `exit_half_spread`, `max_commands_per_min`, `max_day_loss`. Restart to apply.

## Scoring facts this is built around

- Scoring days are Nov 26, 27 and 28 (UTC), each scored separately.
- Volume prizes need day PnL > 0 and Sharpe > 1. Sharpe is mean/std of the 24 hourly returns
  times √8760, so Sharpe > 1 only needs mean/std > 0.0107.
- Only trades signed with the bot key count.
