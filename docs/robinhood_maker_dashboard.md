# Robinhood Lighter Maker Dashboard

`strategies.lighter_simple_market_maker` starts a read-only local dashboard by
default. It does not expose order, cancel, pause, or credential endpoints.

After the maker connects, open:

```text
http://127.0.0.1:8788/
```

The page polls `GET /api/snapshot` every two seconds and shows:

- Lighter depth-1 prices, quote center, target bid/ask, and post-only orders;
- own order status, quantity, age, account order count, and unmanaged orders;
- Lighter, Binance, and combined inventory with utilization against the cap;
- Binance depth imbalance, inventory skew, total quote offset, and next action;
- session realized/unrealized/combined PnL and base/quote volume.

The bind address is private by default. To select another local port:

```bash
python -m strategies.robinhood_lighter_market_maker \
  --env-file /etc/perp/robinhood.env \
  --dashboard-host 127.0.0.1 \
  --dashboard-port 8788
```

Use an SSH tunnel when the strategy runs on a server:

```bash
ssh -N -L 8788:127.0.0.1:8788 user@server
```

Then open `http://127.0.0.1:8788/` on the operator workstation. Do not bind
the dashboard to `0.0.0.0` without an authenticated reverse proxy and a
firewall rule.

For a deployment that must not start an HTTP listener, add:

```text
--no-dashboard
```
