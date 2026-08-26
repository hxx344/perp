# Robinhood Lighter neutral account manager

`strategies.rh_neutral_manager` monitors two accounts on Robinhood Chain
Lighter and keeps their cross-margin collateral from becoming dangerously
imbalanced. It is deliberately a separate process from the maker and hedge
strategies.

The configured neutral layout is controlled by `RH_NEUTRAL_MAIN_LONG_SYMBOL`
(or `--main-long-symbol`). It accepts `SPY` or `QQQ`; the subaccount is always
generated as the exact opposite pair:

| Account | SPY | QQQ |
| --- | --- | --- |
| `main_long_symbol=SPY` | long | short |
| `sub` | short | long |

With `RH_NEUTRAL_MAIN_LONG_SYMBOL=QQQ`, the table reverses: `main` is short
SPY/long QQQ and `sub` is long SPY/short QQQ. The manager rejects any invalid
symbol or same-direction pair.

The signs are intentionally opposite. A layout where both accounts use the
same signs is directional, not a four-leg neutral position; the manager
rejects such a layout rather than silently monitoring it.

## Modes

The default mode is read-only. It uses public account reads and does not need
private keys. `--live` enables reduce-only close orders and same-master USDG
transfers. `--auto-transfer` additionally allows the monitor loop to execute a
calculated transfer; without it, the dashboard only displays the plan and an
operator can decide when to rebalance.

Do not enable both live flags until the read-only output has been checked for
at least one complete polling interval. Transfers are only considered after
both account responses are fresh and their `l1_address` values match.

## Configuration

Start from [`env_rh_neutral_example.txt`](../env_rh_neutral_example.txt). Keep
the real file outside the repository with mode `600`. Each account has its own
Lighter API key and nonce. Use explicit `RH_NEUTRAL_*` key maps; do not rely on
the single-account maker environment.

Create or enable one transaction-capable API key on each account and keep the
two private-key maps separate. The same-master transfer path does not need an
EVM wallet private key, but it still needs the source account's Lighter API
key. The value format is JSON, for example
`RH_NEUTRAL_MAIN_API_PRIVATE_KEYS={"4":"0x<64-or-80-hex-key>"}`; use a
separate map for `RH_NEUTRAL_SUB_API_PRIVATE_KEYS`. For a read-only canary,
leave both maps empty; the signer is not created until a live action is
requested.

The account indexes can be supplied explicitly. Alternatively,
`RH_NEUTRAL_L1_ADDRESS` can be used only when that address has exactly two
active tradable accounts (main account/subaccount). If it has more, startup stops and requires explicit
main/sub indexes instead of guessing (explicit indexes always take precedence).
The market IDs default to `0`, which makes the manager resolve `SPY` and `QQQ`
from the current RH perp catalogue at startup. Set them explicitly if an
operator wants a fixed canary. The endpoint, WebSocket URL, and SDK signing
chain are checked as one Robinhood profile (`api.rh.lighter.xyz`, chain
`466324`).

The `466324` value is the Lighter signer domain used by the RH API. It is not
the Robinhood Chain EVM network ID shown by wallet tools; do not substitute a
wallet/network chain ID in this configuration.

## Linux commands

From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-robinhood.txt
cp env_rh_neutral_example.txt /etc/perp/rh-neutral.env
chmod 600 /etc/perp/rh-neutral.env
```

Read-only canary:

```bash
python -m strategies.rh_neutral_manager \
  --env-file /etc/perp/rh-neutral.env
```

After the virtual environment is installed, the same entry point can be
shortened to:

```bash
bash scripts/run_rh_neutral.sh
```

Pass `--live`, `--auto-transfer`, or other manager flags after the script; set
`RH_NEUTRAL_ENV_FILE` to use a different environment-file path.

Live manual-action mode (automatic transfers remain off):

```bash
python -m strategies.rh_neutral_manager \
  --env-file /etc/perp/rh-neutral.env \
  --live
```

Only after the transfer plan has been reviewed:

```bash
python -m strategies.rh_neutral_manager \
  --env-file /etc/perp/rh-neutral.env \
  --live --auto-transfer
```

The process binds the dashboard to `127.0.0.1:8790` by default. From an
operator workstation, use an SSH tunnel instead of exposing the port:

```bash
ssh -N -L 8790:127.0.0.1:8790 user@server
```

Then open `http://127.0.0.1:8790/`. Set a dashboard token to enable write
buttons. A non-loopback bind requires both
`RH_NEUTRAL_DASHBOARD_ALLOW_PUBLIC=true` and Basic Auth credentials; put it
behind a TLS reverse proxy/VPN and restrict the firewall. Never expose an
unauthenticated plain-HTTP dashboard: the snapshot contains balances,
positions, margin and PnL.

## Transfer policy

For each account the manager compares the two fresh `available_balance`
values. If their difference is larger than
`RH_NEUTRAL_TRANSFER_HYSTERESIS_USDC`, it transfers half of the difference,
bounded by `RH_NEUTRAL_MIN_TRANSFER_USDC` and
`RH_NEUTRAL_MAX_TRANSFER_USDC`. This makes the post-transfer available
balances converge toward the same midpoint. The direction is bidirectional:
the manager can transfer `main -> sub` when the main account has more
available balance, or `sub -> main` when the subaccount has more. The transfer
fee is queried immediately before signing; a live transfer is refused if the
fee cannot be read. A position marked isolated is fail-closed because an
account-level transfer does not add collateral to that isolated position.

Transfers use a fail-closed circuit breaker. If either account cannot be read,
the REST refresh fails, a snapshot is older than
`RH_NEUTRAL_TRANSFER_SNAPSHOT_MAX_AGE_SECONDS` (default 15 seconds), or a
previous write has unknown status, all automatic and manual transfers are
blocked. After recovery, the manager requires
`RH_NEUTRAL_TRANSFER_RECOVERY_SUCCESSES` (default 3) consecutive complete
account snapshots before allowing a transfer again. The dashboard exposes the
state, reason, snapshot ages, and recovery progress.

The legacy `RH_NEUTRAL_MIN_MARGIN_RATIO`,
`RH_NEUTRAL_TARGET_MARGIN_RATIO`, and `RH_NEUTRAL_RESERVE_USDC` values remain
accepted for old env files but are not used to size transfers in balance mode.

Before any transfer, all four legs must be present with the configured signs.
For each symbol, the manager also compares the absolute main/sub position
notional and pauses transfers when the relative difference exceeds
`RH_NEUTRAL_NOTIONAL_TOLERANCE` (default `0.50`). This is a guardrail, not a
beta model; the operator remains responsible for choosing the SPY/QQQ hedge
ratio.

The dashboard displays maintenance and initial margin usage as percentages of
account equity. The backend also retains the raw requirement fields and legacy
ratio fields for API compatibility; those ratios are not shown in the
operator-facing dashboard. The dashboard also shows the combined equity of
the main and sub accounts as total account assets. These are account-health
indicators, not the market's configured IMR/MMR fractions.

The dashboard also displays the combined available-balance ratio:
`(main available balance + sub available balance) / (main equity + sub
equity)`. It is colored red below 10%, orange from 10% to 25%, yellow from
25% to 50%, and green at or above 50%. This is an operational warning
indicator, not a protocol liquidation threshold.

## Feishu reports

Set `RH_NEUTRAL_FEISHU_WEBHOOK_URL` to a Feishu incoming-bot webhook to send a
text summary at startup and every 10 minutes by default. Set
`RH_NEUTRAL_FEISHU_WEBHOOK_SECRET` when the bot has signature verification
enabled; the manager generates Feishu's timestamp/signature fields. The
interval is configurable with `RH_NEUTRAL_FEISHU_REPORT_INTERVAL_SECONDS`.
Reports contain only operational account data (equity, available balance,
positions, PnL, transfer state, and the balance delta), never private keys or
the full environment file. Webhook failures are logged and do not change the
transfer circuit state.

The exchange does not provide an atomic transaction spanning two accounts.
"Close both" submits two independent reduce-only IOC orders concurrently and
reports every leg separately. A partial result must be reviewed before any
retry. The manager never accepts a client-supplied side; it re-reads the live
position and derives sell-for-long/buy-for-short itself.

## Dashboard actions

The authenticated dashboard provides:

- account equity, available balance, maintenance requirement and ratio;
- all four configured legs and aggregate signed/gross notional;
- persistent transfer history with timestamp, direction, USDG amount, status,
  and balancing reason (newest 50 records);
- a calculated transfer plan and manual rebalance button;
- quantity or fraction close for one account/symbol;
- paired close for both accounts on one symbol;
- an explicit four-leg flatten button requiring `FLATTEN_ALL` confirmation.

Every write request requires Basic Auth, `X-Neutral-Action: dashboard`, and a
unique `request_id`. In read-only mode action routes return unavailable rather
than simulating a real order.

## State, locking, and uncertain writes

`RH_NEUTRAL_STATE_PATH` defaults to
`logs/rh_neutral_manager_state.json`. The manager stores action IDs, transfer
history, and timed-out writes there without storing private keys. Advisory
locks derived from the RH endpoint plus the L1 address and each explicit
account index are held for the process lifetime (discovered index locks are
added before writes), so a second manager cannot bypass the guard by choosing
another state-file path or discovery form.

If an SDK write exceeds `RH_NEUTRAL_ACTION_TIMEOUT_SECONDS`, its coroutine is
not cancelled because the exchange may already have received it. The action is
shown as `unknown_pending`, persisted, and all further live writes are blocked.
Even if the local task later returns, the record remains blocked until an
operator checks the RH account, transaction, order, and nonce state. Back up
the journal, reconcile the write, and only then mark that entry `acknowledged`
(or remove the reconciled entry while the service is stopped). Never delete
the state file merely to retry an uncertain write.

An API response with `accepted_pending_confirmation` is handled more narrowly:
the manager performs up to `RH_NEUTRAL_CONFIRMATION_ATTEMPTS` fresh account
reads (with `RH_NEUTRAL_CONFIRMATION_POLL_SECONDS` between reads). A transfer is
acknowledged only when both account balances move in opposite directions by the
requested amount; a close is acknowledged only when the authoritative market
position is reduced without changing sign. If those checks do not pass, the
accepted record remains blocked. This bounded read-back is not an atomic
transaction receipt and does not auto-acknowledge timeout/transport-unknown
records. If the journal itself becomes unavailable after a signer call, the
result is `unknown_journal` and the process remains write-blocked until durable
storage is restored and the account state is reconciled.

## Operational checks

Before enabling live mode, verify:

1. The two account indexes are distinct and the startup snapshot shows the
   same L1 master address.
2. The four positions have the intended opposite signs and the expected
   notional/beta ratio. Equal contract quantities do not necessarily mean
   equal risk.
3. Both API key maps belong to their respective accounts and are allowed to
   create reduce-only orders/transfer transactions. Keep a separate read-only
   deployment for monitoring when possible.
4. System time, DNS, HTTPS connectivity and the RH account/market responses
   are healthy.
5. A manual close is tested with the smallest market-valid quantity on a
   non-production account before using the paired or flatten action.
6. `RH_NEUTRAL_STATE_PATH` is on persistent local storage with directory mode
   `700`; no unresolved `pending_writes` are shown before live mode is enabled.

The process keeps running through transient read failures but suppresses all
transfer plans while either snapshot is stale/error. Check the dashboard
`last_refresh_error`, `pair_error`, per-leg `direction_ok`, and action history
after every write.

Official references: [RH account API](https://apidocs.rh.lighter.xyz/reference/account-1.md),
[same-master transfer](https://apidocs.rh.lighter.xyz/reference/transferfeeinfo.md),
[WebSocket channels](https://apidocs.rh.lighter.xyz/docs/websocket.md), and
[Lighter margin definitions](https://docs.lighter.xyz/trading/liquidations-and-llp-insurance-fund.md).
