# Robinhood Lighter Maker Dashboard

`strategies.lighter_simple_market_maker` starts a read-only local dashboard by
default. It has no order, cancel, pause, or credential endpoints. The Python
listener is intentionally plain HTTP on loopback; it is not an internet-facing
web server. Keep the trading environment file outside the repository with
permissions limited to the service account.

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

## Public access through Caddy (recommended)

For access from another computer, expose only an HTTPS reverse proxy. The
strategy must continue to listen on `127.0.0.1:8788`; do **not** use
`--dashboard-host 0.0.0.0`, publish port `8788`, or forward that port directly
from the cloud provider. Caddy terminates TLS and protects both the HTML page
and `/api/*` endpoints with Basic Auth. Caddy's automatic certificate issuance
requires a DNS record for the hostname and inbound ports 80 and 443.

1. Point a DNS name such as `maker.example.com` at the server, then install
   Caddy. The example uses the `basic_auth` directive, which requires Caddy
   2.8 or newer. The official stable package is the least surprising option
   on Ubuntu/Debian:

   ```bash
   sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https curl
   curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
     | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
   curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
     | sudo tee /etc/apt/sources.list.d/caddy-stable.list
   sudo chmod o+r /usr/share/keyrings/caddy-stable-archive-keyring.gpg \
     /etc/apt/sources.list.d/caddy-stable.list
   sudo apt update
   sudo apt install -y caddy
   caddy version
   ```

   If Caddy is already installed, keep it only when `caddy version` reports
   `2.8` or newer; otherwise upgrade it before validating the file.

2. Generate a bcrypt password hash in a private terminal. `caddy
   hash-password` prompts for the password; use a unique dashboard password,
   never a Robinhood/Lighter key:

   ```bash
   caddy hash-password
   ```

3. Install `deploy/robinhood/maker-dashboard.Caddyfile.example` as
   `/etc/caddy/Caddyfile`, then replace the hostname, username, and generated
   hash. The hash is an access credential, so keep the file readable only by
   root and the Caddy service group.

   ```bash
   sudo install -o root -g caddy -m 0640 \
     deploy/robinhood/maker-dashboard.Caddyfile.example \
     /etc/caddy/Caddyfile
   sudoedit /etc/caddy/Caddyfile
   ```

   ```caddyfile
   maker.example.com {
       encode zstd gzip

       basic_auth {
           operator $2a$14$REPLACE_WITH_CADDY_BCRYPT_HASH
       }

       reverse_proxy 127.0.0.1:8788 {
           # Keep the proxy password out of the Python dashboard process.
           header_up -Authorization
       }

       header {
           -Server
           Strict-Transport-Security "max-age=31536000; includeSubDomains"
           X-Content-Type-Options "nosniff"
           X-Frame-Options "DENY"
           Referrer-Policy "no-referrer"
           Permissions-Policy "camera=(), geolocation=(), microphone=(), payment=(), usb=()"
       }
   }
   ```

   Validate and reload Caddy:

   ```bash
   sudo caddy validate --config /etc/caddy/Caddyfile
   sudo systemctl reload caddy
   sudo systemctl enable caddy
   ```

4. Permit only the proxy and administration ports in the host and cloud
   firewall. Adapt `OpenSSH` if the server uses a non-standard SSH port; do
   not add an allow rule for `8788`:

   ```bash
   sudo ufw allow OpenSSH
   sudo ufw allow 80/tcp
   sudo ufw allow 443/tcp
   sudo ufw delete allow 8788/tcp || true
   sudo ufw deny 8788/tcp
   sudo ufw status numbered
   ```

   The expected listeners are Caddy on 80/443 and Python on
   `127.0.0.1:8788`. Check before opening access:

   ```bash
   sudo ss -ltnp | grep -E ':(80|443|8788)\b'
   ```

5. Verify that unauthenticated requests are rejected and authenticated
   requests use HTTPS. `curl -u operator` prompts for the password without
   placing it in shell history:

   ```bash
   curl -I https://maker.example.com/
   curl -u operator https://maker.example.com/api/healthz
   ```

The dashboard response contains operational telemetry only and is serialized
without environment variables or private keys. Nevertheless, PnL, balances,
orders, and inventory are sensitive, so keep the proxy authentication enabled
and use a VPN or an identity-aware gateway (for example, Cloudflare Access)
when available. Store the trading env file separately, for example:

```bash
sudo chown perp:perp /etc/perp/robinhood.env
sudo chmod 600 /etc/perp/robinhood.env
```

Use the actual `--service-user`/`--service-group` values if the deployment was
installed under a different account.

If remote access is occasional, an SSH tunnel avoids any public web endpoint:

```bash
ssh -N -L 8788:127.0.0.1:8788 user@server
```

Then open `http://127.0.0.1:8788/` on the operator workstation. For a
deployment that must not start an HTTP listener, add:

```text
--no-dashboard
```
