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
accounts use separate Aster API key/secret pairs. Reads use the signed Aster
Futures account endpoint and never reuse the Lighter credentials.

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

This program is monitor-only. It calculates and displays a transfer plan for
diagnostics, but never submits a master/sub-account transfer.

## Transfer permissions

No wallet private key is required for this monitor because it performs no
transfer. The two HMAC API key/secret pairs are used only for account reads.
The old wallet/Agent signer variables remain in the template only for
compatibility with an earlier experimental implementation and are ignored by
the monitor-only runtime.

## Run

```bash
python -m strategies.aster_neutral_manager --env-file /etc/perp/aster-neutral.env
```

The dashboard shows the calculated plan but has no transfer button. Any actual
master/sub-account transfer must be performed separately through Aster's
approved wallet/API workflow.

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
