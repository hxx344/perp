# 多 VPS 单边做市集群部署指南

本文档说明如何在四台 VPS 上部署 Lighter 做市策略与 Binance 对冲的集群方案。方案包含一个 **协调器 (Coordinator)** 服务与四个 **代理 (Agent)** 节点：

- 1 台主做市 (Primary) VPS：负责在 Lighter 上做多或做空单边挂单。
- 3 台对冲 (Hedge) VPS：负责相反方向的挂单以分散风险。
- 协调器聚合所有节点的仓位和风险，自动下发 `RUN/PAUSE` 指令并在需要时翻转方向。

---

## 1. 前置条件

| 项目 | 要求 |
| --- | --- |
| 操作系统 | 任意支持 Python ≥3.10 的 Linux/Windows VPS |
| 依赖安装 | `pip install -r requirements.txt` (工作目录 `perp-dex-tools`)
| 环境变量 | Lighter API 凭证 (`API_KEY_PRIVATE_KEY`, `LIGHTER_ACCOUNT_INDEX`, `LIGHTER_API_KEY_INDEX`)，Binance Futures API (`BINANCE_API_KEY`, `BINANCE_API_SECRET`)，可放入 `.env`
| 端口开放 | 协调器默认监听 `8080`，需要对四台 Agent 可达（建议仅内网或通过防火墙白名单） |
| 时间同步 | 所有 VPS 需保留 NTP 自动对时，避免请求签名因时间漂移失败 |

建议为协调器与 Agents 分别建立独立的 Python 虚拟环境，所有节点需共享相同的代码版本。

---

## 2. 协调器部署步骤

### 2.1 准备配置文件

在协调器 VPS 上进入仓库目录，复制示例配置：

```powershell
Copy-Item .\cluster\config.example.json .\cluster\config.prod.json
```

按实际风险偏好调整以下字段：

- `global_exposure_limit`：全局净持仓绝对值阈值 (e.g. `1200` 表示 1200 合约)。
- `global_resume_ratio`：恢复阈值占比，`0.65` 表示跌回 65% 时恢复交易。
- `cycle_inventory_cap`：主节点单周期最大持仓，达到后会进入冷却并准备翻面。
- `cooldown_seconds`：翻面前的冷却期。
- `initial_primary_direction`：`"long"` 或 `"short"`，指定主节点首个方向。
- `primary_quantity_range` / `hedge_quantity_range`：协调器发给各角色的下单区间，Agent 会在区间内随机。

### 2.2 启动服务

激活虚拟环境后运行：

```powershell
python -m cluster.coordinator --config cluster\config.prod.json --host 0.0.0.0 --port 8080
```

> **Linux 示例**：
>
> ```bash
> python -m cluster.coordinator --config cluster/config.prod.json --host 0.0.0.0 --port 8080
> ```

服务启动后可通过 `GET /status` 检查当前状态：

```powershell
Invoke-RestMethod -Uri "http://COORDINATOR_HOST:8080/status"
```

推荐使用 `tmux`、`screen` 或 `systemd` 等方式将服务常驻运行。

---

## 3. Agent 节点部署

每台 Agent VPS 都需要：

1. 克隆仓库并安装依赖（与协调器步骤相同）。
2. 在仓库根目录创建 `.env`，写入 Lighter 与 Binance 的 API 凭证。
3. 准备策略运行参数，例如 Lighter 交易对、Binance 对应符号等。

### 3.1 启动命令说明

`cluster/agent_runner.py` 是策略包装器，启动时需传入 **协调器信息 + 策略参数**。

常用选项：

| 参数 | 说明 |
| --- | --- |
| `--coordinator-url` | 协调器地址，例如 `http://10.0.0.5:8080` |
| `--vps-id` | 当前 VPS 的唯一 ID（如 `vps-primary-1`） |
| `--role` | 可选，`primary` 或 `hedge`；留空时由协调器分配 |
| `--command-poll-interval` | 轮询协调器指令的频率（秒） |
| `--metrics-interval` | 上报净持仓的频率（秒） |
| `--random-seed` | 可选；设定后随机下单区间可复现 |

后续所有未识别参数都会传给 `strategies/lighter_simple_market_maker.py`。常见参数：

- `--lighter-ticker ETH-PERP`
- `--binance-symbol ETHUSDT`
- `--order-quantity 80`（初始默认值，实际会被协调器覆盖随机区间）
- `--spread-bps 6`
- `--hedge-threshold 400`

### 3.2 启动示例（主节点）

```powershell
python cluster\agent_runner.py \
  --coordinator-url http://10.0.0.5:8080 \
  --vps-id vps-primary-1 \
  --role primary \
  --command-poll-interval 1.0 \
  --metrics-interval 2.0 \
  --lighter-ticker ETH-PERP \
  --binance-symbol ETHUSDT \
  --order-quantity 80 \
  --spread-bps 6 \
  --hedge-threshold 400
```

### 3.3 启动示例（对冲节点）

```powershell
python cluster\agent_runner.py \
  --coordinator-url http://10.0.0.5:8080 \
  --vps-id vps-hedge-1 \
  --role hedge \
  --command-poll-interval 1.5 \
  --metrics-interval 3.0 \
  --lighter-ticker ETH-PERP \
  --binance-symbol ETHUSDT \
  --order-quantity 60 \
  --spread-bps 6 \
  --hedge-threshold 400
```

> 对冲节点可复制此命令，修改 `--vps-id` 即可，例如 `vps-hedge-2`、`vps-hedge-3`。

### 3.4 运行流程

1. Agent 启动后先向协调器 `POST /register` 注册，若角色未指定则自动分配。
2. 策略进入暂停状态，等待协调器推送 `RUN` 命令。
3. Agent 周期性向 `/command` 拉取命令：
   - `RUN`：解除暂停，采用协调器下发的方向与数量区间。
   - `PAUSE`：立即暂停挂单，等待下一步动作。
4. 每次上报仓位时由协调器计算全局风险，必要时进入冷却或翻面。

---

## 4. 运维建议

- **监控**：使用 `/status` 查看集群阶段 (`initial/running/global_paused/cooldown`) 与各 Agent 持仓。
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

### 6.2 Windows Task Scheduler

1. 创建基本任务，触发条件选择“登录时/启动时”。
2. 操作选择“启动程序”，指向 `powershell.exe`。
3. 在“添加参数”里填写：

```powershell
-NoProfile -ExecutionPolicy Bypass -File "C:\perp\start_agent.ps1"
```

4. 在 `start_agent.ps1` 中调用上述启动命令并确保虚拟环境已激活。

---

## 7. 常见问题

| 问题 | 排查建议 |
| --- | --- |
| Agent 一直等待 `WAIT` | 检查协调器日志是否进入 `global_paused`，或 Agent 是否正确注册角色 |
| 协调器无法访问 | 确认端口开放、防火墙白名单；使用 `curl`/`Invoke-RestMethod` 检查 |
| 下单数量未随机 | 确认协调器配置的区间，或 Agent 是否收到 `RUN` 命令；查看 Agent 日志中的数量区间 |
| Binance 下单失败 | 检查 API 权限（需要 Futures 签名）、时间同步与最小手数 |

---

完成上述步骤后，即可在四台 VPS 上运行多角色的单边做市集群，并由协调器统一管理风险与节奏。
