# Robinhood Lighter Aster 虚拟循环迁移说明

## 迁移边界

本次迁移保留原策略的四段状态机：

1. Aster Maker 入场；启用 `--virtual-aster-maker` 时只等待参考行情触价，不下单。
2. Robinhood Lighter 以反方向限价单真实成交。
3. Aster 反向 Maker；虚拟模式仍只等待参考行情触价。
4. Robinhood Lighter 反向真实成交，使仓位回到启动基线或零。

没有改动虚拟成交判定、Lighter 私有订单 WebSocket 确认、仓位差回退确认、超时处理和紧急回仓的核心流程。迁移发生在 Lighter 适配层，不复制策略文件。

## Robinhood 运行档

`--lighter-environment robinhood` 会原子选择以下参数：

| 参数 | 值 |
| --- | --- |
| REST | `https://api.rh.lighter.xyz` |
| WebSocket | `wss://api.rh.lighter.xyz/stream` |
| 签名 chain ID | `466324` |
| BTC market ID | 启动时按符号动态查询，当前为 `1` |
| BTC 数量精度 | 启动时动态查询，当前为 5 位 |
| BTC 价格精度 | 启动时动态查询，当前为 1 位 |
| BTC 最小数量 | 运行时校验 `max(0.00020 BTC, 10 USDG / price)` |

程序会拒绝 REST、WebSocket 和 chain ID 的混搭。Robinhood 档也不会执行原策略面向 Core Lighter 的 Arbitrum 开户、充值或余额归集逻辑；API 凭证必须事先创建，`--l1-private-key` 会在修改凭证文件前被拒绝。

SDK 锁定到官方 `lighter-python` v1.1.2 对应提交。真实 Lighter 腿使用 IOC 限价单，避免超时订单继续留在盘口；下单显式启用主账户范围的自成交保护，并使用不取模的 48 位单调客户端订单号。同一 Lighter 账户只运行一个策略进程。

## 首次只读与小额验证

先基于 [env_robinhood_example.txt](../env_robinhood_example.txt) 创建本地环境文件并填入 Robinhood Lighter 凭证。不要把环境文件提交到 Git。

先检查 CLI 和配置，不发送订单：

```powershell
python strategies/aster_lighter_cycle.py --help
```

首个真实 canary 使用有限循环、BTC 最小有效数量和保守杠杆：

```powershell
python strategies/aster_lighter_cycle.py `
  --env-file .env.robinhood `
  --lighter-environment robinhood `
  --aster-ticker BTC `
  --lighter-ticker BTC `
  --quantity 0.00020 `
  --lighter-leverage 2 `
  --direction buy `
  --virtual-aster-maker `
  --virtual-maker-price-source bn `
  --aster-maker-depth 10 `
  --slippage 0.02 `
  --lighter-max-wait 10 `
  --cycles 1
```

命令会发送 Robinhood Lighter 真实订单。上线前应先验证账户所在地区、交易权限、费用档位、API key 权限、余额和市场状态。若主体位于 Robinhood Lighter 条款列出的受限地区，只能使用只读行情，不能运行上述命令。

## 经济与风控说明

这个循环在 Aster 虚拟模式下不是跨所对冲：Aster 不持有真实反向仓位，第二段到第四段之间存在单边 Lighter 库存风险。因此应从单循环、最小数量开始，观察成交确认、滑点、逆向选择和回仓成功率，再逐步增加规模。

Standard 与 Premium 的费用和延迟不同。当前官方表中 Standard 的 maker/taker 费率为 0，但 taker 路径延迟为 300ms；小于 100 万美元 14 日成交量的 Premium taker 费率为 3.5bp，路径延迟为 200ms。四段循环包含两次真实 taker 成交，切换 Premium 前必须把双边费用、滑点和逆向选择计入期望成本。

程序不会根据成交量自动切换账户档位，也不会把积分或成交量奖励放入收益目标。任何自成交、关联账户互刷、spoofing、layering、quote stuffing 或以刷分为主的循环都不属于本策略的允许用途。

启用现有 `--coordinator-url` 后，RB 策略会上报带方向的 Lighter 仓位、仓位名义价值、活动平仓量，以及相对启动基线的库存恢复预览。Linux 服务也可在受限权限的 Robinhood 环境文件中设置 `HEDGE_COORDINATOR_URL`、`HEDGE_COORDINATOR_AGENT`、`HEDGE_COORDINATOR_USERNAME` 和 `HEDGE_COORDINATOR_PASSWORD`，避免密码出现在命令行和进程列表中。预览只给出残余量和建议买卖方向，不会从仪表板直接触发交易；实际回仓继续使用策略内置、限次且可核验的 IOC 恢复流程。

出现以下任一情况时停止新增循环并人工核对：

- WebSocket nonce 不连续并反复重建盘口；
- 私有订单状态与仓位差确认不一致；
- 实际仓位未回到启动基线；
- 下单数量被市场最小量拒绝；
- 紧急 IOC 部分成交后仍有残仓；低于最小量时程序会阻断下一轮并要求人工核对；
- Sequencer 拒单、nonce 异常或 API 限频；
- 实际每轮成本持续高于预期边际。

## 官方资料

- [Robinhood Lighter API Get Started](https://apidocs.rh.lighter.xyz/docs/get-started)
- [WebSocket](https://apidocs.rh.lighter.xyz/docs/websocket)
- [Signing Transactions](https://apidocs.rh.lighter.xyz/docs/signing-transactions)
- [Account Types](https://apidocs.rh.lighter.xyz/docs/account-types)
- [Rate Limits](https://apidocs.rh.lighter.xyz/docs/rate-limits)
- [Robinhood Chain Points Terms](https://docs.lighter.xyz/points-program/lighter-on-robinhood-chain-points)
