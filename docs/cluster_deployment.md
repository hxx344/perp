# 多 VPS 单边做市集群部署指南

本文档说明如何在四台 VPS 上部署 Lighter 做市策略的集群方案，通过多节点协同完成对冲与风险控制。方案包含一个 **协调器 (Coordinator)** 服务与四个 **代理 (Agent)** 节点：

- 1 台主做市 (Primary) VPS：负责在 Lighter 上做多或做空单边挂单。
- 3 台对冲 (Hedge) VPS：负责相反方向的挂单以分散风险。
- 协调器聚合所有节点的仓位和风险，自动下发 `RUN/PAUSE` 指令并在需要时翻转方向。

---

## 1. 前置条件

| 项目 | 要求 |
| --- | --- |
| 操作系统 | 任意支持 Python ≥3.10 的 Linux/Windows VPS |
| 依赖安装 | `pip install -r requirements.txt` (工作目录 `perp-dex-tools`)
| 环境变量 | Lighter API 凭证 (`API_KEY_PRIVATE_KEY`, `LIGHTER_ACCOUNT_INDEX`, `LIGHTER_API_KEY_INDEX`)；若需启用外部 Binance 对冲，可额外设置 `BINANCE_API_KEY`, `BINANCE_API_SECRET`
| 端口开放 | 协调器默认监听 `8080`，需要对四台 Agent 可达（建议仅内网或通过防火墙白名单） |
| 时间同步 | 所有 VPS 需保留 NTP 自动对时，避免请求签名因时间漂移失败 |

> **账户类型自动切换**：主节点与对冲节点在启动时会调用 Lighter 的 `changeAccountTier` 接口，将 `LIGHTER_ACCOUNT_INDEX` 对应账户切换为高级账户。默认目标 tier 为 `premium`（可通过 `LIGHTER_TARGET_ACCOUNT_TIER` 覆盖）；如需额外校验，可设置 `LIGHTER_TARGET_ACCOUNT_TIER_ID`（整数）并在切换后比对返回的 account type。请求时会自动生成 10 分钟有效的签名 token，同时将请求与返回码写入控制台/日志，方便排查权限或 tier 不一致的问题。

建议为协调器与 Agents 分别建立独立的 Python 虚拟环境，所有节点需共享相同的代码版本。

---

## 2. 协调器部署步骤

### 2.1 准备配置文件

在协调器 VPS 上进入仓库目录，复制示例配置：

```bash
cp cluster/config.example.json cluster/config.prod.json
```

按实际风险偏好调整以下字段：

- `global_exposure_limit`：全局净持仓绝对值阈值 (e.g. `1200` 表示 1200 合约)。达到该阈值时协调器会进入 **hedge_only** 模式：暂停所有主节点的下单指令，仅保留对冲节点继续工作以把净仓位压回安全区。
- `global_resume_ratio`：恢复阈值占比，`0.65` 表示净仓位回落到上限的 65% 时会自动让主节点恢复报单。
- `cycle_inventory_cap`：主节点单周期最大持仓，达到后会进入冷却并准备翻面。
- `cooldown_seconds`：翻面前的冷却期。
- `initial_primary_direction`：`"long"` 或 `"short"`，指定主节点首个方向。
- `primary_quantity_range` / `hedge_quantity_range`：协调器发给各角色的下单区间，Agent 会在区间内随机。
- `dashboard_username` / `dashboard_password`：可选，若设置则 `/dashboard` 页面启用 HTTP Basic 认证；留空表示无需登录。
- `flatten_tolerance`：紧急清仓完成判定的残余仓位容忍度，默认 `0.01`。当所有 VPS 仓位绝对值低于该值时，协调器会认为清仓完成并统一下发 `PAUSE`。

### 2.2 启动服务

激活虚拟环境后运行：

```bash
python -m cluster.coordinator --config cluster/config.prod.json --host 0.0.0.0 --port 8080
```

服务启动后可通过 `GET /status` 检查当前状态：

```bash
curl http://COORDINATOR_HOST:8080/status
```

推荐使用 `tmux`、`screen` 或 `systemd` 等方式将服务常驻运行。

---

## 3. Agent 节点部署

每台 Agent VPS 都需要：

1. 克隆仓库并安装依赖（与协调器步骤相同）。
2. 在仓库根目录创建 `.env`，写入 Lighter 与 Binance 的 API 凭证。
3. 准备策略运行参数，例如 Lighter 交易对、Binance 对应符号等。

### 3.1 启动命令说明

`cluster/agent_runner.py` 是策略包装器，启动时需传入 **协调器信息（Agent 参数）+ 策略参数**。所有命令行选项如下。

#### Agent 参数

| 参数 | 说明 |
| --- | --- |
| `--coordinator-url` | **必填**。协调器地址，例如 `http://10.0.0.5:8080` |
| `--vps-id` | **必填**。当前 VPS 的唯一 ID（如 `vps-primary-1`） |
| `--role` | 可选。`primary` / `hedge`，留空时由协调器自动分配 |
| `--command-poll-interval` | 默认 `1.0` 秒。轮询协调器指令的频率 |
| `--metrics-interval` | 默认 `2.0` 秒。上报净持仓的频率 |
| `--random-seed` | 可选。设定后可复现实例中的随机下单区间 |

#### 策略参数（透传）

所有未被 Agent 解释的参数会传给 `strategies/lighter_simple_market_maker.py`。该脚本支持以下选项：

| 参数 | 说明 |
| --- | --- |
| `--lighter-ticker` | **必填**。Lighter 合约代码，如 `ETH-PERP` |
| `--binance-symbol` | **必填**（建议用于兼容）。Binance 对应标的，如 `ETHUSDT` |
| `--order-quantity` | 初始下单数量（协调器随机区间的兜底值） |
| `--spread-bps` | 半边价差（bps），例如 `6` 表示 0.06% |
| `--hedge-threshold` | 单节点持仓阈值；若未指定 `--inventory-limit` 会作为兜底限制 |
| `--hedge-buffer` | 对冲时的缓冲数量，默认为 `0` |
| `--inventory-limit` | 覆盖默认库存上限；为空时回退到 `--hedge-threshold` |
| `--config-path` | 热更新 JSON 路径，默认 `configs/hot_update.json` |
| `--env-file` | 自定义 `.env` 路径 |
| `--loop-sleep` | 主循环休眠秒数，默认 `3.0` |
| `--order-refresh-ticks` | 价格偏离多少 tick 时替换订单，默认 `2` |
| `--metrics-interval` | 账户指标打印间隔（秒），默认 `30`，最低 `5` |
| `--no-console-log` | 关闭控制台日志输出 |
| `--allowed-side` | 可多次指定限制仅报买/卖方向 |
| `--order-quantity-min` | 随机下单区间下限 |
| `--order-quantity-max` | 随机下单区间上限 |
| `--disable-binance-hedge` | 关闭 Binance 对冲，完全依赖 VPS 间库存抵消 |

> **提示**：集群模式通常只依赖主/对冲节点互相抵消仓位，可配合 `--disable-binance-hedge` 省略 Binance API 凭证。`--binance-symbol` 仍建议填写与合约匹配的标的，以兼容旧配置或备用切换。

### 3.2 启动示例（主节点）

```bash
python cluster/agent_runner.py \
  --coordinator-url http://10.0.0.5:8080 \
  --vps-id vps-primary-1 \
  --role primary \
  --command-poll-interval 1.0 \
  --metrics-interval 2.0 \
  --lighter-ticker ETH-PERP \
  --binance-symbol ETHUSDT \
  --order-quantity 80 \
  --spread-bps 6 \
  --hedge-threshold 400 \
  --disable-binance-hedge
```

### 3.3 启动示例（对冲节点）

```bash
python cluster/agent_runner.py \
  --coordinator-url http://10.0.0.5:8080 \
  --vps-id vps-hedge-1 \
  --role hedge \
  --command-poll-interval 1.5 \
  --metrics-interval 3.0 \
  --lighter-ticker ETH-PERP \
  --binance-symbol ETHUSDT \
  --order-quantity 60 \
  --spread-bps 6 \
  --hedge-threshold 400 \
  --disable-binance-hedge
```

> 对冲节点可复制此命令，修改 `--vps-id` 即可，例如 `vps-hedge-2`、`vps-hedge-3`。

### 3.4 运行流程

1. Agent 启动后先向协调器 `POST /register` 注册，若角色未指定则自动分配。
2. 策略进入暂停状态，等待协调器推送 `RUN` 命令。
3. Agent 周期性向 `/command` 拉取命令：
   - `RUN`：解除暂停，采用协调器下发的方向与数量区间。
   - `PAUSE`：立即暂停挂单，等待下一步动作。
4. 每次上报仓位时由协调器计算全局风险，必要时进入冷却或翻面。

> **hedge_only 模式**：当全局净仓位达到 `global_exposure_limit` 时，协调器会选择**只保留能够削减当前净仓位的角色**继续运行。举例：
> - 集群整体偏多（净仓位为正）时，只让负责做空的一侧继续 `RUN`，另一侧全部 `PAUSE`。
> - 集群整体偏空（净仓位为负）时，会反过来暂停对冲节点，让主节点继续做多把净敞口拉回中性。
> 当净仓位回落到 `global_exposure_limit × global_resume_ratio` 以下时，两侧都会收到恢复指令重新进入 `running`。重新进入 `running` 后，如果主节点的存量仓位仍然超过 `cycle_inventory_cap`，只有在**当前方向继续交易会让库存进一步扩大**时才会再次触发冷却；若翻面的新方向正好能把库存打回中性，则主节点会被允许持续运行直到库存压回阈值以内。

---

## 4. 运维建议

- **监控**：使用 `/status` 查看集群阶段 (`initial/running/hedge_only/cooldown`) 与各 Agent 持仓。
- **监控**：
  - `http://COORDINATOR_HOST:8080/dashboard` 提供开箱即用的网页面板，可在 Windows 浏览器中实时查看阶段、净敞口、各 VPS 状态等信息（每 2 秒自动刷新）。若配置了 `dashboard_username` / `dashboard_password`，浏览器会弹出登录框，输入相应账户即可访问。页面提供三个常用控制：
    - **全局暂停**：立即向所有 Agent 下发 `PAUSE`，节点会撤掉未成交订单并停止访问 Lighter，保持待命状态直至手动恢复。
    - **恢复运行**：解除全局暂停，协调器会根据当前风控状态重新分发 `RUN` 或其他指令。
    - **紧急清仓**：在非暂停状态下可用，会向协调器发出清仓指令；所有 Agent 会暂停普通循环，按照做市时的随机数量规则反向挂出限价单（使用更激进的对手价偏移）逐步削减库存，直到仓位回到 `flatten_tolerance` 定义的容忍范围内，随后自动保持暂停。
  - `GET /status` 仍可用于程序化监控，返回相同的 JSON 结构。
- **日志**：
  - 协调器输出使用 `cluster.coordinator` logger。
  - Agent 日志通过 `cluster.agent` 与 `lighter-simple` tag 打印，建议收集到集中日志平台。
- **故障恢复**：
  - 若某个 Agent 重启，协调器会在下次注册时继续分配角色。
  - 协调器重启后，需要所有 Agent 重新注册；可使用进程管理工具保证自动重启。
- **安全性**：限定协调器 API 访问范围，必要时启用 VPN 或反向代理加身份验证。

---

## 5. 快速验证步骤

1. 启动协调器后调用 `GET /status`，确认 `phase` 为 `initial`。
2. 启动主节点与至少一个对冲节点，观察协调器日志出现 `initial start`，`phase` 切换为 `running`。
3. `RUN` 命令下发后，Agent 日志会展示恢复交易信息，并按随机区间重新下单。
4. 手动将某个 Agent 的 `.env` 中 API Key 置空测试失败容错，确保日志提示清晰，及时恢复真实密钥。

---

## 6. 系统化部署示例

### 6.1 systemd 服务模板（Linux）

`/etc/systemd/system/perp-coordinator.service`：

```ini
[Unit]
Description=Perp Cluster Coordinator
After=network.target

[Service]
WorkingDirectory=/opt/perp-dex-tools
ExecStart=/opt/perp-dex-tools/.venv/bin/python -m cluster.coordinator --config /opt/perp-dex-tools/cluster/config.prod.json --host 0.0.0.0 --port 8080
Restart=always
Environment="PYTHONUNBUFFERED=1"

[Install]
WantedBy=multi-user.target
```

Agent 节点可类似定义 `perp-agent@.service`，通过 `ExecStart` 将 `--vps-id` 设为 `%i`。

## 7. 常见问题

| 问题 | 排查建议 |
| --- | --- |
| Agent 一直等待 `WAIT` | 检查协调器日志是否进入 `hedge_only` 或主节点是否被暂停，以及 Agent 是否正确注册角色 |
| 协调器无法访问 | 确认端口开放、防火墙白名单；使用 `curl` 检查 |
| 下单数量未随机 | 确认协调器配置的区间，或 Agent 是否收到 `RUN` 命令；查看 Agent 日志中的数量区间 |
| Binance 下单失败（如已启用） | 检查 API 权限（需要 Futures 签名）、时间同步与最小手数 |

---

完成上述步骤后，即可在四台 VPS 上运行多角色的单边做市集群，并由协调器统一管理风险与节奏。
