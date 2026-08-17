# Robinhood Lighter Market Maker

This is a separate strategy from `aster_lighter_cycle`. It quotes one bid and
one ask on the Robinhood Chain Lighter BTC perpetual market.

The default policy is deliberately small and conservative:

- Binance Futures depth is read-only and supplies only a bounded bid/ask-pressure
  signal. The Lighter midpoint remains the absolute price scale, so a different
  quote asset or contract denomination does not create an absolute-price jump.
- Lighter orders are explicitly `post_only`; the relative Binance signal may
  shift the local center but quotes remain at least one tick from the opposite
  Lighter side.
- Binance trading is disabled by default. A hedge is only attempted after the
  combined inventory crosses `--hedge-threshold` and only when the operator
  explicitly enables it.
- The default leverage is `2x`; the live market maximum is checked before the
  leverage transaction is sent.
- On shutdown, only this process's own quotes are cancelled. Manual or other
  strategy orders are left untouched.
- Client order IDs are persisted before submission. A restart reconciles and
  cancels old process-owned quotes before making a new quote.
- A per-account lock prevents two copies of this maker from running at once.
- Runtime pause state is read from `configs/robinhood_market_maker.json`, not
  the Aster cycle's hot-update file.

## One-line start

From the repository root, after creating `robinhood.env` (or using
`.env.robinhood`/`/etc/perp/robinhood.env`), run:

```bash
python3 -m strategies.robinhood_lighter_market_maker
```

This is a real-order command. Start with one side for a canary:

```bash
python3 -m strategies.robinhood_lighter_market_maker --allowed-side buy --cycles 1
```

`--cycles 1` performs one quote iteration and then cancels the process-owned
quote during graceful shutdown. Remove it only after the canary is verified.

Use `--help` to print every available option. The wrapper automatically picks
the first readable environment file in this order:

1. `/etc/perp/robinhood.env`
2. `robinhood.env`
3. `.env.robinhood`
4. `.env`

An explicit file always wins:

```bash
python3 -m strategies.robinhood_lighter_market_maker --env-file robinhood.env
```

## Main parameters

| Option | Default | Meaning |
| --- | --- | --- |
| `--order-quantity` | `0.00020` | Base BTC per quote; runtime market minimums are applied. |
| `--spread-bps` | `5` | Half-spread in basis points around the locally scaled Lighter midpoint. |
| `--binance-depth-levels` | `10` | Binance depth levels used for the relative pressure signal. |
| `--binance-imbalance-max-bps` | `3` | Maximum quote-center shift caused by Binance depth pressure. |
| `--inventory-limit` | threshold | Hard Lighter inventory cap. |
| `--inventory-skew-bps` | `3` | Maximum quote-center shift used to pull inventory toward zero. |
| `--hedge-threshold` | `0.001` | Combined inventory at which an optional hedge becomes eligible. |
| `--hedge-cooldown-seconds` | `30` | Minimum interval between explicit cross-venue attempts. |
| `--max-hedge-quantity` | threshold | Maximum size of one Binance hedge attempt. |
| `--lighter-leverage` | `2` | Lighter leverage. Must not exceed live market maximum. |
| `--loop-sleep` | `2` | Seconds between quote refreshes. |
| `--cycles` | `0` | Stop after N successful quote iterations; `0` runs continuously. |
| `--order-refresh-ticks` | `2` | Price movement in Lighter ticks before replacing a quote. |
| `--order-refresh-bps` | `1` | Minimum reference move before a normal replacement. |
| `--min-quote-lifetime-seconds` | `5` | Minimum resting time, except for risk withdrawal or a large move. |
| `--order-ack-timeout-seconds` | `5` | Maximum wait for private/REST confirmation; no duplicate is placed while waiting. |
| `--binance-reference-timeout-seconds` | `1` | Timeout for one Binance depth request; a failure uses a neutral signal and the Lighter midpoint. |
| `--fill-cooldown-seconds` | `5` | Minimum delay after a fill on the same side. |
| `--ownership-state-file` | automatic | Override the crash-recovery state/lock location. |
| `--disable-binance-reference` | off | Disable the Binance depth signal and use only the Lighter midpoint. |
| `--enable-binance-hedge` | off | Explicitly enable authenticated Binance market hedging. |
| `--allow-existing-binance-position` | off | Required opt-in to manage a pre-existing Binance position; use a dedicated hedge account. |
| `--allowed-side buy/sell` | both | Restrict a canary to one side. Repeat to allow both. |

`--take-profit` is not a parameter of this maker. The maker earns from spread
capture and inventory control; it does not use the Aster cycle's compatibility
flag.

## Environment

Copy `env_robinhood_example.txt` to the selected file and fill in the
pre-created Robinhood Lighter API credentials. The strategy expects the RH
profile (`api.rh.lighter.xyz`, chain `466324`) and refuses a Core endpoint.
It also rejects reserved API key indexes outside `4..254` and placeholder or
malformed private keys before connecting.
Keep the file private (`chmod 600 robinhood.env`) and run only one maker process
for an account.

By default, crash-recovery state is stored under `logs/` using the Lighter
account index and ticker. This file is automatic and contains order IDs, not
private keys. Do not delete it after an abnormal exit until the Lighter active
orders have been checked. If a persisted ID is not visible consistently, the
program stops for manual review instead of assuming the order is gone. The
strategy also refuses to start when it finds an active order it cannot prove it
owns; use a dedicated clean account/market for this process.

The requested leverage is verified before quotes start. A maker-only Lighter
key may not be allowed to change leverage; in that case set the same leverage
in the Robinhood Lighter UI first, or provide a separately controlled key with
the required account-config permission. The process stops if it cannot confirm
the leverage operation.

The Binance depth signal requires no API key. Binance API credentials are needed
only when `--enable-binance-hedge` is explicitly supplied. Hedge mode refuses
to start with an existing Binance position unless
`--allow-existing-binance-position` is explicitly supplied, because the bot
cannot distinguish its own hedge from a manual position.

Emergency flatten uses reduce-only, marketable IOC orders on Lighter with a
finite attempt limit. When Binance hedging is enabled, it subsequently closes
the Binance hedge leg with a reduce-only market order and raises an error if
either venue cannot be confirmed flat.

## Tuning order

1. Set `--binance-symbol` to a liquid Binance contract for the same underlying;
   the symbols may use different quote assets or contract naming, but must not
   represent unrelated assets. Run one-sided with the minimum quantity and
   `--lighter-leverage 2`.
2. Increase `--spread-bps` only after checking fills, cancel latency, maker fee,
   and adverse selection in the logs.
3. Increase `--inventory-limit` only after verifying the account collateral and
   the observed inventory distribution.
4. Keep Binance hedging disabled unless the Lighter inventory threshold is a
   genuine risk limit; it is not part of the normal quote loop.

## Account economics

The strategy never changes the Robinhood Lighter account tier automatically.
Standard currently has zero maker/taker fees but slower maker/cancel handling;
Premium removes maker/cancel delay but charges tiered maker fees. Measure actual
fill edge after maker fee and adverse selection before tightening
`--spread-bps`. The five-second minimum quote lifetime and price hysteresis are
also intended to conserve Premium volume quota; ordinary cancel is preferred
over modify/cancel-all during normal operation.

Points are monitoring-only and are not an objective of this strategy. The
Lighter client sends self-trade prevention on every order; do not run another
strategy on the same account/market or generate wash/self-controlled volume.

Official references:

- https://apidocs.rh.lighter.xyz/docs/account-types
- https://apidocs.rh.lighter.xyz/docs/volume-quota
- https://apidocs.rh.lighter.xyz/docs/websocket
- https://apidocs.rh.lighter.xyz/docs/signing-transactions
