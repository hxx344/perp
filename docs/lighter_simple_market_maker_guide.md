# 简易 Lighter 做市 + Binance 对冲机器人使用手册

脚本 `strategies/lighter_simple_market_maker.py` 提供了一个最小可运行的做市 + 对冲循环：

- 在 Lighter 上同时挂出单档买卖限价单，根据中价与设定的基础点差生成报价；
- 每轮循环都会读取热更新配置（本地 `configs/hot_update.json` 或远程 JSON），可随时暂停或调整深度系数；
- 当 Lighter 净仓位超过阈值时，在 Binance USDT 本位合约上下反向市价单进行对冲；
- 仓位接近限制时暂停对应方向挂单，避免继续积累风险。

## 运行前准备

1. **安装依赖**（假设已创建虚拟环境 `.venv`）：

   ```powershell
   cd D:\project8\perp-dex-tools
   ..\.venv\Scripts\pip.exe install -r requirements.txt
   ```

2. **配置环境变量**（建议写入 `.env` 并由 `python-dotenv` 自动加载）：

   | 变量 | 说明 |
   | --- | --- |
   | `API_KEY_PRIVATE_KEY` | Lighter Signer 私钥 |
   | `LIGHTER_ACCOUNT_INDEX` | Lighter 账户索引 |
   | `LIGHTER_API_KEY_INDEX` | Lighter API Key 索引 |
   | `BINANCE_API_KEY` | Binance Futures API Key |
   | `BINANCE_API_SECRET` | Binance Futures API Secret |
   | `ACCOUNT_NAME` *(可选)* | 区分日志文件名 |

3. **热更新 JSON**：默认读取 `configs/hot_update.json`，示例：

   ```json
   {
     "aster_maker_depth_level": 14,
     "aster_leg1_depth_level": 14,
     "aster_leg3_depth_level": 14,
     "cycle_enabled": true
   }
   ```

   - `cycle_enabled=false` 时循环撤单并休眠；
   - `aster_maker_depth_level` 会乘到基础点差上（例如 20 代表点差翻倍）。

## 启动示例

```powershell
cd D:\project8\perp-dex-tools
..\.venv\Scripts\python.exe strategies\lighter_simple_market_maker.py \
    --lighter-ticker ETH-PERP \
    --binance-symbol ETHUSDT \
    --order-quantity 0.5 \
    --spread-bps 12 \
    --hedge-threshold 5 \
    --hedge-buffer 1
```

> **提示**：脚本会自动把仓库根目录加入 `PYTHONPATH`。如果你已经进入 `strategies/` 目录，也可以直接执行：

```bash
python lighter_simple_market_maker.py --lighter-ticker ETH-PERP --binance-symbol ETHUSDT --order-quantity 0.5 --spread-bps 12 --hedge-threshold 5 --hedge-buffer 1
```

### 常用参数

| 参数 | 说明 |
| --- | --- |
| `--lighter-ticker` | Lighter 市场符号 |
| `--binance-symbol` | Binance USDT 合约符号 |
| `--order-quantity` | 每笔挂单数量（基础资产） |
| `--spread-bps` | 单边点差（半边值，bps） |
| `--hedge-threshold` | 净仓位超过该值即对冲 |
| `--hedge-buffer` | 扣除的缓冲量，避免过度对冲 |
| `--inventory-limit` | 库存软上限，超过时暂停对应方向挂单（默认等同对冲阈值） |
| `--loop-sleep` | 主循环间隔（秒） |
| `--order-refresh-ticks` | 价格偏离多少个 tick 时撤单重挂 |
| `--config-path` | 热更新 JSON 文件路径或 URL |
| `--metrics-interval` | 监控日志输出的间隔（秒），默认 30 秒 |
| `--no-console-log` | 关闭控制台日志输出 |

## 工作流程概览

1. **热更新加载**：失败时沿用上一份成功配置；`cycle_enabled=false` 时撤单并休眠。
2. **报价维护**：读取最优买卖价，生成目标报价，仅保留每个方向一笔订单。
3. **库存控制**：仓位逼近限制时停挂对应方向；
4. **阈值对冲**：净仓位绝对值超过阈值时调用 Binance 市价单对冲，并记录日志。

## 监控输出

- 每隔 `--metrics-interval` 秒（默认 30 秒）打印一行 Lighter 账户快照，包含：
   - 当前合约净持仓、仓位价值；
   - 未实现/已实现盈亏、可用余额；
   - 24 小时成交量（`dailyVol`）及本次进程累计成交量（`sessionVol`，基于 Lighter 返回的 `total_volume` 计算）。
- 若配置了 Binance 对冲密钥，会额外输出对应合约在 Binance USDT 永续上的持仓、名义价值、未实现盈亏、钱包余额与可用余额，方便与 Lighter 侧对照。
- 监控日志会写入 `logs/` 目录，同时可选输出到控制台（默认开启，可用 `--no-console-log` 禁用）。

## 快速验证

```powershell
cd D:\project8\perp-dex-tools
..\.venv\Scripts\python.exe -m pytest tests\test_lighter_simple_market_maker.py -q
```

## 延伸建议

- 根据波动率 / 成交量信号动态调整点差与订单数量；
- 引入 Prometheus / Sentry 等监控，追踪库存、对冲结果与滑点；
- 将 Binance 对冲升级为限价、TWAP 或分批执行，降低冲击成本；
- 支持多交易对与多对冲市场，实现统一仓位控制。
