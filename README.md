##### 关注我 **X (Twitter)**: [@yourQuantGuy](https://x.com/yourQuantGuy)

---

**English speakers**: Please read README_EN.md for the English version of this documentation.

## 📢 分享说明

**欢迎分享本项目！** 如果您要分享或修改此代码，请务必包含对原始仓库的引用。我们鼓励开源社区的发展，但请保持对原作者工作的尊重和认可。

---

## 自动交易机器人

一个支持多个交易所（目前包括 EdgeX, Backpack, Paradex, Aster, Lighter, GRVT）的模块化交易机器人。该机器人实现了自动下单并在盈利时自动平仓的策略，主要目的是取得高交易量。

## 邀请链接 (获得返佣以及福利)

#### EdgeX 交易所: [https://pro.edgex.exchange/referral/QUANT](https://pro.edgex.exchange/referral/QUANT)

永久享受 VIP 1 费率；额外 10% 手续费返佣；10% 额外奖励积分

#### Backpack 交易所: [https://backpack.exchange/join/quant](https://backpack.exchange/join/quant)

使用我的推荐链接获得 35% 手续费返佣

#### Paradex 交易所: [https://app.paradex.trade/r/quant](https://app.paradex.trade/r/quant)

使用我的推荐链接获得 10% 手续费返佣以及潜在未来福利

#### Aster 交易所: [https://www.asterdex.com/zh-CN/referral/5191B1](https://www.asterdex.com/zh-CN/referral/5191B1)

使用我的推荐链接获得 30% 手续费返佣以及积分加成

#### GRVT 交易所: [https://grvt.io/exchange/sign-up?ref=QUANT](https://grvt.io/exchange/sign-up?ref=QUANT)

获得 1.3x 全网最高的积分加成，未来的手续费返佣（官方预计 10 月中上线），以及即将开始的专属交易竞赛

## 安装

Python 版本要求（最佳选项是 Python 3.10 - 3.12）：

- grvt 要求 python 版本在 3.10 及以上
- Paradex 要求 python 版本在 3.9 - 3.12
- 其他交易所需要 python 版本在 3.8 及以上

1. **克隆仓库**：

   ```bash
   git clone <repository-url>
   cd perp-dex-tools
   ```

2. **创建并激活虚拟环境**：

   首先确保你目前不在任何虚拟环境中：

   ```bash
   deactivate
   ```

   创建虚拟环境：

   ```bash
   python3 -m venv env
   ```

   激活虚拟环境（每次使用脚本时，都需要激活虚拟环境）：

   ```bash
   source env/bin/activate  # Windows: env\Scripts\activate
   ```

3. **安装依赖**：
   首先确保你目前不在任何虚拟环境中：

   ```bash
   deactivate
   ```

   激活虚拟环境（每次使用脚本时，都需要激活虚拟环境）：

   ```bash
   source env/bin/activate  # Windows: env\Scripts\activate
   ```

   ```bash
   pip install -r requirements.txt
   ```

   **grvt 用户**：如果您想使用 grvt 交易所，需要额外安装 grvt 专用依赖：
   激活虚拟环境（每次使用脚本时，都需要激活虚拟环境）：

   ```bash
   source env/bin/activate  # Windows: env\Scripts\activate
   ```

   ```bash
   pip install grvt-pysdk
   ```

   **Paradex 用户**：如果您想使用 Paradex 交易所，需要额外创建一个虚拟环境并安装 Paradex 专用依赖：

   首先确保你目前不在任何虚拟环境中：

   ```bash
   deactivate
   ```

   创建 Paradex 专用的虚拟环境（名称为 para_env）：

   ```bash
   python3 -m venv para_env
   ```

   激活虚拟环境（每次使用脚本时，都需要激活虚拟环境）：

   ```bash
   source para_env/bin/activate  # Windows: para_env\Scripts\activate
   ```

   安装 Paradex 依赖

   ```bash
   pip install -r para_requirements.txt
   ```

4. **设置环境变量**：
   在项目根目录创建`.env`文件，并使用 env_example.txt 作为样本，修改为你的 api 密匙。

   **协调器（Dashboard / 风险告警 / 飞书推送）说明**：
   - `strategies/hedge_coordinator.py` 会在启动时自动加载**仓库根目录**（也就是 `perp/`）下的 `.env`。
   - 这意味着：即使你在 `perp-dex-tools/` 目录里启动 coordinator，只要 `.env` 放在 `d:/project8/.env`（仓库根目录）也会生效。
   - 已存在的进程环境变量优先级更高（不会被 `.env` 覆盖）。

   飞书 webhook 推送（PARA 风险快照）常用配置示例：

   ```text
   FEISHU_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/xxxx
   FEISHU_PARA_PUSH_ENABLED=true
   FEISHU_PARA_PUSH_INTERVAL=30
   ```

5. **Telegram 机器人设置（可选）**：
   如需接收交易通知，请参考 [Telegram 机器人设置指南](docs/telegram-bot-setup.md) 配置 Telegram 机器人。

## 多 VPS 单边做市集群

需要在多台 VPS 上协同运行 Lighter 做市与 Binance 对冲时，请参考新的 [集群部署指南](docs/cluster_deployment.md)。该文档覆盖协调器服务、四节点 Agent 配置、示例命令以及运维建议。

### Aster–Lighter 价差回归模拟器（测试模式）

`strategies/spread_reversion_strategy.py` 提供了一个全新的价差回归策略实现，能够在**完全离线的测试模式**下模拟下单并计算盈亏。逻辑与我们讨论的“价差扩大时双边开仓、价差缩小时双边平仓”一致，适合在正式接入撮合前先评估阈值与仓位配置。

- 策略要点：滚动统计 mid 价差，`|z| >= enter_z` 时建立对冲仓位，`|z| <= exit_z` 或发生反向穿越 / 时间超限时平仓。
- 模拟输出：逐笔成交的方向、价差变化、持仓 tick 数以及单笔/累计 PnL。
- 数据格式：CSV 含 `timestamp, aster_bid, aster_ask, lighter_bid, lighter_ask` 五列；若未提供文件，可使用内置的演示数据快速体验。

运行示例（Windows PowerShell）：

```powershell
cd d:/project8/perp-dex-tools
D:/project8/.venv/Scripts/python.exe strategies/spread_reversion_strategy.py --demo
```

若要载入实盘抓取的价差数据：

```powershell
cd d:/project8/perp-dex-tools
D:/project8/.venv/Scripts/python.exe strategies/spread_reversion_strategy.py --data path/to/your_spread_history.csv --enter-z 2.5 --exit-z 0.6 --quantity 3
```

模拟结果会打印到终端，并可在 `tests/test_spread_reversion_strategy.py` 查阅更多使用示例及单元测试。

## 策略概述

**重要提醒**：大家一定要先理解了这个脚本的逻辑和风险，这样你就能设置更适合你自己的参数，或者你也可能觉得这不是一个好策略，根本不想用这个策略来刷交易量。我在推特也说过，我不是为了分享而写这些脚本，而是我真的在用这个脚本，所以才写了，然后才顺便分享出来。
这个脚本主要还是要看长期下来的磨损，只要脚本持续开单，如果一个月后价格到你被套的最高点，那么你这一个月的交易量就都是零磨损的了。所以我认为如果把`--quantity`和`--wait-time`设置的太小，并不是一个好的长期的策略，但确实适合短期内高强度冲交易量。我自己一般用 40 到 60 的 quantity，450 到 650 的 wait-time，以此来保证即使市场和你的判断想法，脚本依然能够持续稳定地下单，直到价格回到你的开单点，实现零磨损刷了交易量。

该机器人实现了简单的交易策略：

1. **订单下单**：在市场价格附近下限价单
2. **订单监控**：等待订单成交
3. **平仓订单**：在止盈水平自动下平仓单
4. **持仓管理**：监控持仓和活跃订单
5. **风险管理**：限制最大并发订单数
6. **网格步长控制**：通过 `--grid-step` 参数控制新订单与现有平仓订单之间的最小价格距离
7. **停止交易控制**：通过 `--stop-price` 参数控制停止交易的的价格条件

#### ⚙️ 关键参数

- **quantity**: 每笔订单的交易数量
- **direction**: 脚本交易的方向，buy 表示看多，sell 表示看空
- **take-profit**: 止盈百分比（如 0.02 表示 0.02%）
- **max-orders**: 最大同时活跃订单数（风险控制）
- **wait-time**: 订单间等待时间（避免过于频繁交易）
- **grid-step**: 网格步长控制（防止平仓订单过于密集）
- **stop-price**: 当市场价格达到该价格时退出脚本
- **pause-price**: 当市场价格达到该价格时暂停脚本

#### 网格步长功能详解

`--grid-step` 参数用于控制新订单的平仓价格与现有平仓订单之间的最小距离：

- **默认值 -100**：无网格步长限制，按原策略执行
- **正值（如 0.5）**：新订单的平仓价格必须与最近的平仓订单价格保持至少 0.5% 的距离
- **作用**：防止平仓订单过于密集，提高成交概率和风险管理

例如，当看多且 `--grid-step 0.5` 时：

- 如果现有平仓订单价格为 2000 USDT
- 新订单的平仓价格必须低于 1990 USDT（2000 × (1 - 0.5%)）
- 这样可以避免平仓订单过于接近，提高整体策略效果

#### 📊 交易流程示例

假设当前 ETH 价格为 $2000，设置止盈为 0.02%：

1. **开仓**：在 $2000.40 下买单（略高于市价）
2. **成交**：订单被市场成交，获得多头仓位
3. **平仓**：立即在 $2000.80 下卖单（止盈价格）
4. **完成**：平仓单成交，获得 0.02% 利润
5. **重复**：继续下一轮交易

#### 🛡️ 风险控制

- **订单限制**：通过 `max-orders` 限制最大并发订单数
- **网格控制**：通过 `grid-step` 确保平仓订单有合理间距
- **下单频率控制**：通过 `wait-time` 确保下单的时间间隔，防止短时间内被套
- **实时监控**：持续监控持仓和订单状态
- **⚠️ 无止损机制**：此策略不包含止损功能，在不利市场条件下可能面临较大损失

## 示例命令：

### EdgeX 交易所：

ETH：

```bash
python runbot.py --exchange edgex --ticker ETH --quantity 0.1 --take-profit 0.02 --max-orders 40 --wait-time 450
```

ETH（带网格步长控制）：

```bash
python runbot.py --exchange edgex --ticker ETH --quantity 0.1 --take-profit 0.02 --max-orders 40 --wait-time 450 --grid-step 0.5
```

ETH（带停止交易的价格控制）：

```bash
python runbot.py --exchange edgex --ticker ETH --quantity 0.1 --take-profit 0.02 --max-orders 40 --wait-time 450 --stop-price 5500
```

BTC：

```bash
python runbot.py --exchange edgex --ticker BTC --quantity 0.05 --take-profit 0.02 --max-orders 40 --wait-time 450
```

### Backpack 交易所：

ETH 永续合约：

```bash
python runbot.py --exchange backpack --ticker ETH --quantity 0.1 --take-profit 0.02 --max-orders 40 --wait-time 450
```

ETH 永续合约（带网格步长控制）：

```bash
python runbot.py --exchange backpack --ticker ETH --quantity 0.1 --take-profit 0.02 --max-orders 40 --wait-time 450 --grid-step 0.3
```

ETH 永续合约（启用 Boost 模式）：

```bash
python runbot.py --exchange backpack --ticker ETH --direction buy --quantity 0.1 --boost
```

### Aster 交易所：

ETH：

```bash
python runbot.py --exchange aster --ticker ETH --quantity 0.1 --take-profit 0.02 --max-orders 40 --wait-time 450
```

ETH（启用 Boost 模式）：

```bash
python runbot.py --exchange aster --ticker ETH --direction buy --quantity 0.1 --boost
```

### GRVT 交易所：

BTC：

```bash
python runbot.py --exchange grvt --ticker BTC --quantity 0.05 --take-profit 0.02 --max-orders 40 --wait-time 450
```

## Aster-Lighter 对冲循环（新功能）

若需执行文档中所述的单轮对冲流程（Aster Maker → Lighter 反向 Taker → Aster 反向 Maker → Lighter 反向 Taker），可以使用 `strategies/aster_lighter_cycle.py` 脚本：

```bash
python strategies/aster_lighter_cycle.py \
   --aster-ticker ETH \
   --lighter-ticker ETH-PERP \
   --quantity 0.5 \
   --direction buy \
   --slippage 0.05 \
   --cycles 0 \
   --cycle-delay 2
```

**注意事项**：

- 运行前需在同一个 `.env` 文件中配置好 Aster 与 Lighter 的 API 凭证。
- `--slippage` 用于控制 Lighter Taker 单相对于对应 Aster 成交价的百分比偏移，数值越大下单越激进。
- `--cycles` 决定执行的循环次数，设置为 `0` 表示脚本会一直运行直到手动中断。
- `--cycle-delay` 用于在每次循环后增加额外的暂停时间（秒）。脚本现在会强制保证两次循环启动之间至少间隔 5 秒，该参数会叠加在这个基础间隔之上。
- `--aster-maker-depth` 用于指定 Aster Maker 挂单参考的深度级别（1-500，默认 10）。
- `--aster-leg1-depth` 可为第一条 Aster Maker 挂单单独设置深度，未提供时默认跟随 `--aster-maker-depth`。
- `--aster-leg3-depth` 可为第三条 Aster 反向 Maker 挂单单独设置深度，未提供时默认跟随 `--aster-maker-depth`。
- 每轮执行前脚本会自动检查配置的 L1 钱包，如果原生 USDC 余额大于 1 USDC，会先发起充值到 Lighter 账户后再开始下一轮。
- `--lighter-market-type {perp,spot}`：选择 Lighter 交易路由。`perp`（默认）走永续接口并应用 `--lighter-leverage`；`spot` 会跳过杠杆设定，直接使用现货余额下单，并在循环开始前按照可用底仓自动收紧每腿数量，避免出现“卖空现货”的情况。
- `--lighter-spot-market-id`：仅在 `--lighter-market-type spot` 时生效，强制 Lighter 使用指定的现货 `market_id`（默认固定为 `2048`，也可以通过环境变量 `LIGHTER_SPOT_MARKET_ID` 覆盖）。
- `--take-profit` 参数目前保留兼容性，但不会影响 Aster 反向 Maker 的挂单价格。
- 默认等待超时为 5 秒，可通过 `--max-wait` 调整。
- `--max-retries` 默认 100 次，`--retry-delay` 默认 5 秒，两者共同控制 Aster Maker 单的重试次数与重试间隔，避免超时直接退出。
- `--preserve-initial-position`：启动时记录 Lighter 初始净仓位；在每轮循环与退出阶段若检测到当前仓位与初始值不一致，会自动下单恢复为初始仓位（默认关闭，仅在显式加上该参数时生效）。
- `--coordinator-url`：可选参数，指向 `strategies/hedge_coordinator.py` 暴露的 HTTP 服务地址，用于实时上报指标供面板展示。
- 如果仅想在虚拟环境中监听价格并触发 Lighter Taker，可使用 `--virtual-aster-maker`；
   配合 `--virtual-maker-price-source bn` 可改为监听 Binance 永续合约的买四/卖四价格；
   选择 `--virtual-maker-price-source edgex` 则可使用 EdgeX 行情：若配置了 `EDGEX_ACCOUNT_ID` 与
   `EDGEX_STARK_PRIVATE_KEY` 会走官方 SDK，否则自动退回公开 WebSocket (`EDGEX_PUBLIC_WS_URL` 可覆写)。
   如需自定义监听合约，可通过 `--virtual-maker-symbol` 覆盖（默认沿用解析到的 Aster 合约 ID 或标的）。

### 现货快速脚本示例

如果想要一个更轻量、开箱即用的现货示例，可以直接运行 `examples/lighter_spot_cycle_example.py`。
该脚本会自动设置 Lighter 为 `spot` 路由、捕捉初始库存，并打印每一轮四腿对冲的摘要，适合作为
自定义开发的模板：

```powershell
cd d:/project8/perp-dex-tools
$env:SPOT_MAX_CYCLES=3            # 可选：执行 3 轮后退出，设 0 持续运行
$env:SPOT_QUANTITY=0.4            # 可选：覆盖每腿数量（Decimal 字符串）
D:/project8/.venv/Scripts/python.exe examples/lighter_spot_cycle_example.py
```

脚本会默认读取同目录下的 `.env`，可通过 `SPOT_ENV_FILE` 指定其它路径。其余可调环境变量：

- `SPOT_ASTER_TICKER` / `SPOT_LIGHTER_TICKER`：标的符号（默认 `ETH` / `ETH-PERP`）。
- `SPOT_DIRECTION`：`buy` / `sell`，决定初始 Maker 方向。
- `SPOT_SLIPPAGE_PCT`：Lighter Taker 相对 Aster 成交价的滑点百分比（默认 `0.08`）。
- `SPOT_CYCLE_DELAY`：两轮之间额外等待秒数（默认 1s，可设为 `0`）。
- `SPOT_VIRTUAL_MAKER=1`：仅监听行情，不在 Aster 真正下单，方便冷启动测试。
- `SPOT_LIGHTER_MARKET_ID`：强制脚本使用指定的 Lighter 现货 `market_id`（默认 2048）。

其余逻辑沿用 `HedgingCycleExecutor`，因此 `.env` 中仍需配置 Aster/Lighter 的完整凭证。可根据需要
复制该脚本并加入更多风控或日志。

### 可选：协调机与面板

若希望远程查看对冲执行情况，可先启动附带的轻量协调机：

```bash
python strategies/hedge_coordinator.py --host 0.0.0.0 --port 8899
```

随后运行对冲脚本时带上 `--coordinator-url http://<host>:8899`，即可在浏览器访问 `/dashboard`
查看当前 Lighter 仓位、累计循环次数、累计收益以及累计成交量等指标。

#### Hedge Metrics Dashboard（Para / GRVT 监控面板）

`strategies/hedge_coordinator.py` 同时提供一个用于多账户监控的前端页面（`strategies/hedge_dashboard.html`），用于汇总展示：

- **PARA Multi-Account Monitor**：聚合多个 Paradex 账户的 Equity / PnL / IM Req / 风险水平等
- **GRVT Multi-Account Monitor**：聚合多个 GRVT 账户的核心指标
- **风险告警（Bark）**：支持全局（global）与 PARA（para）两套独立阈值与历史记录
- **减仓建议**：当 PARA 仅配置两套对冲账户时，会给出“某币种双边减仓 size”的极简建议

说明：该页面核心展示口径以“卡片输出”为权威口径；部分告警历史会附带 `authority_*` 字段用于与卡片对齐。

##### 访问入口

- 协调机启动后：浏览器访问 `http://<host>:8899/dashboard`
- 若开启登录：会先跳转到 `/login`

##### PARA 风险告警（scope）

面板与接口将风险告警分为两套 scope：

- `global`：全局风险告警（旧版兼容）
- `para`：仅用于 PARA Multi-Account Monitor 的告警

两套告警拥有**独立的 enabled/threshold/reset/cooldown/bark_url** 配置与**独立历史记录**。

##### IM Req（初始保证金要求）口径

面板中用于风险与统计的 IM 指标优先取上报字段 `initial_margin_requirement`（权威口径）。
若数据源缺失，页面会回退到兼容字段（例如聚合后的 `initial_margin_total`），但建议尽量保证监控上报包含 `initial_margin_requirement`。

##### PARA 顶部汇总卡片与减仓建议

- PARA 顶部汇总卡片已移除 ETH/BTC PnL 子卡片，避免误导；新增 `Max IM Req` 汇总指标卡片。
- “减仓建议”区域默认只展示一行：`<SYMBOL> 双边减仓 <SIZE><SYMBOL>`（不显示详情）。

**面板登录保护**：

- 启动协调机时加入 `--dashboard-username admin --dashboard-password secret` 即可开启账号密码登录，浏览器会先跳转到 `/login` 表单。
- 登录成功后会发放 12 小时有效的会话 Cookie（可用 `--dashboard-session-ttl 7200` 调整为 2 小时等）。
- CLI 工具或脚本也可继续使用 HTTP Basic 头部访问受保护接口（与旧版本兼容）。
- 退出登录可点击页面中的退出按钮（若需自定义可直接 POST `/logout`）。

#### 自动平衡（Auto Balance）

如果两个 VPS 的 GRVT Equity 长期分叉，可以让协调机在后台自动触发面板里已有的转账流程。现在的配置入口完全通过面板/接口完成：

- `GET /auto_balance/config`：查看当前配置与最近一次触发状态，需要面板登录权限。
- `POST /auto_balance/config`：提交 JSON 配置或 `{"enabled": false}` 关闭自动平衡。

示例请求（Windows PowerShell）：

```powershell
Invoke-RestMethod -Method Post `
   -Uri http://<host>:8899/auto_balance/config `
   -Headers @{ Authorization = "Basic <base64>" } `
   -ContentType 'application/json' `
   -Body (@{
      agent_a = 'vps-grvt-1'
      agent_b = 'vps-grvt-2'
      threshold_ratio = 0.08
      min_transfer = 800
      max_transfer = 3000
      cooldown_seconds = 900
      currency = 'USDT'
      use_available_equity = $false
   } | ConvertTo-Json)
```

- 阈值按比例计算：`|equity_A - equity_B| / max(equity_A, equity_B)`，例如 `0.08` 表示差异超过 8% 才会触发。
- 触发后会在 Equity 较高的 VPS 上创建一次 `main_to_main` 转账请求，目标为另一台 VPS，金额默认为两者差值的一半，可通过 `max_transfer` 为单次自动转账设定上限。
- `min_transfer` 控制最小转账金额（USDT），避免频繁的小额搬砖；`cooldown_seconds` 则限制两次自动触发的间隔（秒）。
- `currency` 默认为 `USDT`，若想优先对比可划转余额可把 `use_available_equity` 设为 `true`，此时会优先使用“transferable balance”（即 `equity - initial_margin - max(total_pnl, 0)` 的近似值）计算差值。
- 转账请求依旧走 `/grvt/transfer` 接口，因此需要两台 VPS 都正确上报 `grvt_accounts.transfer_defaults`，所有自动触发也会在面板和日志里显示对应的 request_id、方向与原因，方便复查。

## GRVT-Lighter 对冲循环

如果想实现与 Aster 类似的流程，但将 Maker 端替换为 GRVT，可使用 `strategies/grvt_lighter_cycle.py`：

```bash
python strategies/grvt_lighter_cycle.py \
   --grvt-ticker BTC \
   --lighter-ticker BTC-PERP \
   --quantity 0.5 \
   --direction buy \
   --slippage 0.05 \
   --cycles 0 \
   --cycle-delay 2
```

**注意事项**：

- 需要在 `.env` 中同时配置 GRVT 与 Lighter 的凭证。
- GRVT Maker 单默认等待超时为 5 秒（`--max-wait`），Lighter Taker 单默认等待 120 秒（`--lighter-max-wait`）。
- `--grvt-quantity` / `--lighter-quantity` 可分别覆盖默认数量，未提供则回落到 `--quantity`。
- 其余参数含义与 Aster-Lighter 版本一致，可直接复用。

## 配置

### 环境变量

#### 通用配置

- `ACCOUNT_NAME`: 环境变量中当前账号的名称，用于多账号日志区分，可自定义，非必须

#### Telegram 配置（可选）

- `TELEGRAM_BOT_TOKEN`: Telegram 机器人令牌
- `TELEGRAM_CHAT_ID`: Telegram 对话 ID

#### EdgeX 配置

- `EDGEX_ACCOUNT_ID`: 您的 EdgeX 账户 ID
- `EDGEX_STARK_PRIVATE_KEY`: 您的 EdgeX API 私钥
- `EDGEX_BASE_URL`: EdgeX API 基础 URL（默认：https://pro.edgex.exchange）
- `EDGEX_WS_URL`: EdgeX WebSocket URL（默认：wss://quote.edgex.exchange）
- `EDGEX_PUBLIC_WS_URL`: EdgeX 公共行情 WebSocket（默认：wss://quote.edgex.exchange/api/v1/public/ws）
- `EDGEX_FORCE_PUBLIC`: 设置为 `1`/`true` 可强制虚拟 maker 使用公开行情，即使环境中存在私有凭证

#### Backpack 配置

- `BACKPACK_PUBLIC_KEY`: 您的 Backpack API Key
- `BACKPACK_SECRET_KEY`: 您的 Backpack API Secret

#### Paradex 配置

- `PARADEX_L1_ADDRESS`: L1 钱包地址
- `PARADEX_L2_PRIVATE_KEY`: L2 钱包私钥（点击头像，钱包，"复制 paradex 私钥"）

#### Aster 配置

- `ASTER_API_KEY`: 您的 Aster API Key
- `ASTER_SECRET_KEY`: 您的 Aster API Secret

#### Lighter 配置

- `LIGHTER_API_PRIVATE_KEYS`: 推荐写成 JSON（如 `{"0":"0xabc...","4":"0xdef..."}`）或以逗号/分号分隔的 `index:key` 列表（如 `0:0xabc...,4:0xdef...`），脚本会按索引一次性加载多把 API 私钥供新版 SignerClient 使用。
- `API_KEY_PRIVATE_KEY` 与 `LIGHTER_API_KEY_INDEX`: 向后兼容的单密钥字段；若已配置 `LIGHTER_API_PRIVATE_KEYS` 可留空。
- `LIGHTER_ACCOUNT_INDEX`: Lighter 账户索引

#### GRVT 配置

- `GRVT_TRADING_ACCOUNT_ID`: 您的 GRVT 交易账户 ID
- `GRVT_PRIVATE_KEY`: 您的 GRVT 私钥
- `GRVT_API_KEY`: 您的 GRVT API 密钥

**获取 LIGHTER_ACCOUNT_INDEX 的方法**：

1. 在下面的网址最后加上你的钱包地址：

   ```
   https://mainnet.zklighter.elliot.ai/api/v1/account?by=l1_address&value=
   ```

2. 在浏览器中打开这个网址

3. 在结果中搜索 "account_index" - 如果你有子账户，会有多个 account_index，短的那个是你主账户的，长的是你的子账户。

### 命令行参数

- `--exchange`: 使用的交易所：'edgex'、'backpack'、'paradex'、'aster'、'lighter'或'grvt'（默认：edgex）
- `--ticker`: 标的资产符号（例如：ETH、BTC、SOL）。合约 ID 自动解析。
- `--quantity`: 订单数量（默认：0.1）
- `--take-profit`: 止盈百分比（例如 0.02 表示 0.02%）
- `--direction`: 交易方向：'buy'或'sell'（默认：buy）
- `--env-file`: 账户配置文件 (默认：.env)
- `--max-orders`: 最大活跃订单数（默认：40）
- `--wait-time`: 订单间等待时间（秒）（默认：450）
- `--grid-step`: 与下一个平仓订单价格的最小距离百分比（默认：-100，表示无限制）
- `--stop-price`: 当 `direction` 是 'buy' 时，当 price >= stop-price 时停止交易并退出程序；'sell' 逻辑相反（默认：-1，表示不会因为价格原因停止交易），参数的目的是防止订单被挂在”你认为的开多高点或开空低点“。
- `--pause-price`: 当 `direction` 是 'buy' 时，当 price >= pause-price 时暂停交易，并在价格回到 pause-price 以下时重新开始交易；'sell' 逻辑相反（默认：-1，表示不会因为价格原因停止交易），参数的目的是防止订单被挂在”你认为的开多高点或开空低点“。
- `--boost`: 启用 Boost 模式进行交易量提升（仅适用于 aster 和 backpack 交易所）
  Boost 模式的下单逻辑：下 maker 单开仓，成交后立即用 taker 单关仓，以此循环。磨损为一单 maker，一单 taker 的手续费，以及滑点。

## 日志记录

该机器人提供全面的日志记录：

- **交易日志**：包含订单详情的 CSV 文件
- **调试日志**：带时间戳的详细活动日志
- **控制台输出**：实时状态更新
- **错误处理**：全面的错误日志记录和处理

## Q & A

### 如何在同一设备配置同一交易所的多个账号？

1. 为每个账户创建一个 .env 文件，如 account_1.env, account_2.env
2. 在每个账户的 .env 文件中设置 `ACCOUNT_NAME=`, 如`ACCOUNT_NAME=MAIN`。
3. 在每个文件中配置好每个账户的 API key 或是密匙
4. 通过更改命令行中的 `--env-file` 参数来开始不同的账户，如 `python runbot.py --env-file account_1.env [其他参数...]`

### 如何在同一设备配置不同交易所的多个账号？

将不同交易所的账号都配置在同一 `.env` 文件后，通过更改命令行中的 `--exchange` 参数来开始不同的交易所，如 `python runbot.py --exchange backpack [其他参数...]`

### 如何在同一设备用同一账号配置同一交易所的多个合约？

将账号配置在 `.env` 文件后，通过更改命令行中的 `--ticker` 参数来开始不同的合约，如 `python runbot.py --ticker ETH [其他参数...]`

## 贡献

1. Fork 仓库
2. 创建功能分支
3. 进行更改
4. 如适用，添加测试
5. 提交拉取请求

## 许可证

本项目采用非商业许可证 - 详情请参阅[LICENSE](LICENSE)文件。

**重要提醒**：本软件仅供个人学习和研究使用，严禁用于任何商业用途。如需商业使用，请联系作者获取商业许可证。

## 免责声明

本软件仅供教育和研究目的。加密货币交易涉及重大风险，可能导致重大财务损失。使用风险自负，切勿用您无法承受损失的资金进行交易。
