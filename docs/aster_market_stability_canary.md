# Aster Market Stability Canary

This is a bounded, single-account live test for measuring Aster BBO impact and
recovery. It is not a volume loop. The dedicated account must be flat before
every cycle.

Each cycle:

1. Reads and validates the current BBO and symbol limits.
2. Refuses wide spreads or estimated wear above configured caps.
3. Submits one small market order with `newOrderRespType=RESULT`.
4. Confirms the position appeared.
5. Sends a `reduceOnly` market order for the executed quantity.
6. Confirms the account returned to zero position.
7. Measures BBO recovery and writes a JSONL audit record.

HTTP/transport errors and incomplete fills are treated as unknown execution
state; the process stops instead of retrying. The default is dry-run and one
cycle. Real orders require both `ASTER_CANARY_LIVE=true` and
`ASTER_CANARY_CONFIRM_LIVE=true`.

```bash
cp env_aster_canary_example.txt /etc/perp/aster-canary.env
chmod 600 /etc/perp/aster-canary.env
python -m strategies.aster_market_stability_canary \
  --env-file /etc/perp/aster-canary.env
```

Only after reviewing the dry-run output, enable the two live switches. Use an
Agent with read and perpetual-trading permission, no withdrawal permission,
and an IP whitelist.
