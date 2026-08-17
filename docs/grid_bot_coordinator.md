# Grid Bot Coordinator

The generic `runbot.py` strategy can report position, notional value, volume,
active close quantity, and mismatch alerts to the lightweight coordinator
ported from `perp01`.

Start the coordinator on a private interface or behind TLS:

```bash
python -m services.coordinator \
  --host 127.0.0.1 \
  --port 8787
```

Start an agent with a unique ID:

```bash
python runbot.py \
  --exchange lighter \
  --ticker BTC \
  --env-file .env \
  --coordinator-url http://127.0.0.1:8787 \
  --coordinator-vps-id btc-node-01
```

Set `COORDINATOR_USER` and `COORDINATOR_PASSWORD` in the process environment
or the bot's protected `.env` file. Avoid passing the password on the command
line because it can be exposed through shell history and process listings.

The dashboard is at `/dashboard`. When credentials are configured, every
state-changing endpoint and every agent endpoint except `/healthz` requires
HTTP Basic authentication. Do not expose the coordinator directly to the
public internet; use a private network or an HTTPS reverse proxy.

This service belongs to the generic resting-order grid bot. The Robinhood
Lighter Aster cycle continues to use `strategies.hedge_coordinator`, whose
inventory recovery path is bounded IOC execution rather than resting-order
manual balance.
