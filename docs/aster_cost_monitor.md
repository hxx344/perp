# Aster Cost Monitor

This is a read-only, separate monitor for the Aster USD1 pairs shown in the
market list: `SKHYNIXUSD1`, `SPCXUSD1`, `CLUSD1`, `SNDKUSD1`, `XAUUSD1`, and
`MUUSD1`. It samples public BBO data and never places orders or transfers.

For each symbol it keeps a rolling window (default 15 minutes) and reports:

```text
average spread bps
spread cost = 10,000 * average spread bps / 10,000
round-trip fees = 10,000 * 0.009% * 2 = 1.8 USD1
total wear = spread cost + round-trip fees
```

The alert condition is intentionally descriptive rather than an execution
authorization: when the target symbol's average spread is at or below
`ASTER_COST_ALERT_MAX_SPREAD_BPS`, an optional Feishu alert is sent once per
configured interval. This monitor does not implement paired self-trading or
volume farming.

Run:

```bash
cp env_aster_cost_monitor_example.txt env_aster_cost_monitor.env
python -m strategies.aster_cost_monitor --env-file env_aster_cost_monitor.env
```

Open `http://127.0.0.1:8792` for the local dashboard. Configure the Feishu
webhook only if alerts are needed. No API key or private key is required.
