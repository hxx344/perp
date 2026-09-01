# Aster XAUUSD1 Neutral Monitor

This is a separate program from the Robinhood Lighter neutral manager. It
monitors two Aster Futures accounts with the following required position
layout:

```text
main: XAUUSD1 long
sub:  XAUUSD1 short
```

The symbol is configurable, but the live deployment should use the exact
symbol returned by Aster `exchangeInfo` (the default is `XAUUSD1`). The two
accounts use separate Pro API V3 user/API-wallet signer credentials. Legacy
HMAC API key/secret pairs remain accepted as a fallback. Reads use the signed
Aster Futures account endpoint and never reuse the Lighter credentials.

For Pro API V3, configure `ASTER_NEUTRAL_*_USER_ADDRESS`,
`ASTER_NEUTRAL_*_SIGNER_ADDRESS`, and
`ASTER_NEUTRAL_*_SIGNER_PRIVATE_KEY`. The signer address is the `0x...` API
wallet shown in Aster's Pro API page, not an API key. Its private key is
required to produce the EIP-712 read signature. Use a read-only API wallet for
this monitor.

## Transfer calculation

The manager compares fresh `availableBalance` values:

```text
delta = main.availableBalance - sub.availableBalance
plan = abs(delta) / 2
```

The plan is suppressed inside `ASTER_NEUTRAL_TRANSFER_HYSTERESIS`, below the
minimum transfer, or when the four-leg sign layout is not present. The source
amount is additionally capped by its Aster `maxWithdrawAmount`, which Aster
documents as the maximum amount available for transfer out. A cooldown and a
stale-snapshot circuit breaker prevent rapid churn.

The effective trigger threshold shown in the dashboard is
`max(transfer_hysteresis, 2 * min_transfer)`, because the planned amount is
half of the balance difference. The source account's `maxWithdrawAmount` is a
separate per-account cap on the resulting transfer amount, not a trigger
threshold.

The default mode is read-only. It calculates and displays a transfer plan for
diagnostics, but submits a master/sub-account transfer only when all three
explicit switches are enabled: `ASTER_NEUTRAL_ENABLE_TRANSFERS=true`,
`ASTER_NEUTRAL_LIVE=true`, and `ASTER_NEUTRAL_AUTO_TRANSFER=true`.

Transfer failures are classified conservatively. A clear validation,
permission, or other HTTP 4xx rejection is recorded as
`rejected_before_submit` and releases the transfer lock. HTTP 503, other 5xx
responses, timeouts, connection failures, and malformed responses are recorded
as `unknown_pending` because Aster may have accepted the request. While an
unknown record is pending, the manager checks both accounts' authenticated
`TRANSFER` income records for the same transaction id and opposite amounts.
Checks are rate-limited to once every 10 seconds. Transient misses only create
warnings; new transfers are paused after 30 consecutive misses (about five
minutes). Balance changes caused by PnL, funding, or fees are never treated as
transfer confirmation.

## Transfer permissions

The two HMAC API key/secret pairs are used for legacy reads, or Pro API V3
signer credentials can be used for reads. Transfers require the master and sub
wallet addresses plus an approved Agent/API Wallet signer private key. By
default the main account's Pro API signer is reused for transfer signing, so
the transfer signer variables do not need duplicate values. Set dedicated
transfer signer variables only when you want a separate Agent. Do not
grant withdrawal permission to the Agent. The transfer path uses `USD1` for
`XAUUSD1`, caps the amount by Aster `maxWithdrawAmount`, and blocks after an
unknown response until the state is reconciled.

## Run

```bash
python -m strategies.aster_neutral_manager --env-file /etc/perp/aster-neutral.env
```

The dashboard shows the calculated plan. With all three live switches enabled
and a dashboard token configured, its rebalance action can execute one plan;
otherwise it remains read-only.

The dashboard listens on `127.0.0.1:8791` by default. It is intentionally
separate from the Lighter dashboard on port 8790. Use an SSH tunnel or an
authenticated HTTPS private proxy for remote access.

## Feishu

Set `ASTER_NEUTRAL_FEISHU_WEBHOOK_URL` and optionally
`ASTER_NEUTRAL_FEISHU_WEBHOOK_SECRET`. Reports are sent every 600 seconds by
default and include both accounts' equity, available balance, official
withdrawable amount, XAUUSD1 position, PnL, and transfer state. Webhook errors
are logged without changing transfer safety state.

The independent Aster state file also keeps the latest 50 transfer records
with direction, amount, timestamp, and exchange status.

## Aster API references

- [Futures account balance v3](https://asterdex.github.io/aster-api-website/futures-v3/account%26trades/)
- [Futures V3 authentication](https://asterdex.github.io/aster-api-website/futures-v3/general-info/)
- [Sub-account transfer](https://asterdex.github.io/aster-api-website/futures-v3/account%26trades/)
- [Aster sub-accounts](https://docs.asterdex.com/trading/sub-account)
