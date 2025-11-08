# 简易 Lighter 做市 + Binance 对冲机器人

该脚本 `strategies/lighter_simple_market_maker.py` 提供了一个最小可运行的做市循环：

- 在 Lighter 上同时挂出一档买 / 卖限价单，点差根据中价和设定的基础点差计算。
- 每次循环读取热更新配置（本地 `configs/hot_update.json` 或指定 URL），支持暂停循环或动态调整深度因子。
- 监控当前 Lighter 净仓位，超过阈值时在 Binance USDT 本位合约上下市价单进行对冲。
- 当库存逼近限制时，会自动停挂对应方向的订单。

## 运行前准备

1. **安装依赖**

   ```powershell
   cd D:\project8\perp-dex-tools
   ..\.venv\Scripts\pip.exe install -r requirements.txt
   ```

2. **环境变量**（可写入 `.env` 并由 `python-dotenv` 加载）：

   | 变量名 | 说明 |
   | --- | --- |
   | `API_KEY_PRIVATE_KEY` | Lighter Signer 私钥 |
   | `LIGHTER_ACCOUNT_INDEX` | Lighter 账户索引 |
   | `LIGHTER_API_KEY_INDEX` | Lighter API Key 索引 |
   | `BINANCE_API_KEY` | Binance Futures API Key |
   | `BINANCE_API_SECRET` | Binance Futures API Secret |
   | `ACCOUNT_NAME` *(可选)* | 用于日志文件命名 |

3. **热更新配置**：默认读取 `configs/hot_update.json`，结构示例：

   ```json
   {
     "aster_maker_depth_level": 14,
     "aster_leg1_depth_level": 14,
     "aster_leg3_depth_level": 14,
     "cycle_enabled": true
   }
   ```

   - `cycle_enabled` 设为 `false` 时循环进入休眠并撤单。
   - `aster_maker_depth_level` 会按比例放大 / 缩小基础点差。

## 启动示例

```powershell
cd D:\project8\perp-dex-tools
..\