# Robinhood Lighter Linux 部署手册

本文只适用于以下策略组合：

- Robinhood Chain Lighter 永续合约是真实成交腿；
- Aster 两条 maker 腿是虚拟触发，不向 Aster 提交订单；
- 虚拟成交价格来自 Binance U 本位合约公共行情；
- 默认与原策略一致持续循环；首次真实验证应显式使用 `--cycles 1`。

`deploy/robinhood` 中的安装和预检不会发送订单。执行 `run.sh --confirm-live`、
显式启动 systemd 服务，或使用下方 Python 原地运行命令，都会向 Lighter
发送真实 IOC 订单。

## 最简原地运行

手动运行不需要复制代码到 `/opt/perp`，也不需要创建 `perp` 用户或安装
systemd。只需在当前仓库和现有虚拟环境中准备一次 `.env.robinhood`，然后执行：

```bash
python -m strategies.robinhood_lighter_cycle --quantity 0.00020 --randomize-direction --slippage 0.3 --max-wait 3
```

这就是实盘运行命令，不含额外确认步骤；Aster 腿是虚拟的，Lighter 腿是真实的。
这个薄入口直接复用 `aster_lighter_cycle`，只自动补齐 BTC、Robinhood 端点、
`.env.robinhood`、Binance 虚拟 Aster 行情、2 倍杠杆、10 档深度和 10 秒 Lighter
等待时间。命令行中继续传入同名参数即可覆盖数值；`--quantity` 保留为必填，避免
误用默认实盘数量。没有指定 `--cycles` 时与原程序相同，会持续运行直到停止；首次
真实验证应增加 `--cycles 1`。

后续 `/opt/perp`、专用用户和 systemd 内容只用于无人值守的长期守护部署，不是
运行策略的前置条件。

## 1. 主机和网络要求

建议使用 Ubuntu 24.04（默认 Python 3.12）或 Debian 12+（默认 Python 3.11），
x86_64，运行时 Python >=3.11。Ubuntu 22.04 默认 Python 3.10，只有在已经通过
可信来源独立提供 Python 3.11+ 时才属于支持范围。wheelhouse 必须由与目标机相同
的 Python 小版本和 CPU 架构构建。
服务器必须保持 NTP 同步，并允许 DNS 与出站 TCP 443 访问：

| 主机 | 用途 |
| --- | --- |
| `api.rh.lighter.xyz` | Robinhood Lighter REST 和 WebSocket |
| `fapi.binance.com` | Binance 公共领先行情 |
| `fapi.asterdex.com` | Aster 合约元数据和公共行情 |
| `fstream.asterdex.com` | Aster 公共 WebSocket |
| `github.com` | 首次下载固定提交的 Lighter SDK 归档 |

不需要开放任何入站端口。建议选择到上述端点延迟稳定的机房，并安装基础组件：

```bash
sudo apt-get update
sudo apt-get install -y \
  ca-certificates git python3 python3-venv python3-dev \
  build-essential libffi-dev libssl-dev util-linux logrotate chrony
sudo systemctl enable --now chrony
```

## 2. 专用账户和代码目录

每个 Robinhood Lighter 账户只能由一个策略进程管理。不要让不同服务、
容器或手工会话共用同一账户和 API nonce。

```bash
sudo useradd --system --create-home --home-dir /var/lib/perp \
  --shell /usr/sbin/nologin perp
sudo install -d -o perp -g perp -m 0750 /opt/perp
```

把当前已经审核的 checkout 部署到 `/opt/perp`，保留 `.git` 元数据，并让
`perp:perp` 拥有该目录。部署应绑定明确的 commit/tag，不要让启动脚本自动
执行 `git pull`。安装器默认拒绝未提交的工作区；`--allow-dirty` 只应用于临时
诊断，不能用于正式 systemd 发布。确认代码后，以专用账户安装依赖：

```bash
sudo -u perp -H bash /opt/perp/deploy/robinhood/install.sh \
  --project-root /opt/perp \
  --python python3
```

脚本只会从当前 checkout 的 `requirements-robinhood.txt` 创建/更新 `.venv`，
不会安装私有 EdgeX SDK，也不会克隆仓库、修改密钥、启用服务或启动交易。
正式多机发布仍应复用同一台 canary 机器验证过的 wheel 产物，避免传递依赖变化。

网络不稳定或多机发布时，在与目标机相同架构、相同 Python 小版本的联网 Linux
主机上构建一次 wheelhouse。输出目录必须为空：

```bash
sudo -u perp -H bash /opt/perp/deploy/robinhood/build-wheelhouse.sh \
  --python python3 \
  --output /var/lib/perp/wheelhouse-rh
```

将整个目录（包括 `SHA256SUMS`）传到目标机后离线安装：

```bash
sudo -u perp -H bash /opt/perp/deploy/robinhood/install.sh \
  --project-root /opt/perp \
  --python python3 \
  --wheelhouse /var/lib/perp/wheelhouse-rh
```

安装器会先验证 wheelhouse 文件集合与 `SHA256SUMS` 完全一致，并验证每个 wheel
的 SHA-256，再使用 `--no-index`；目标机不再访问 GitHub 或 PyPI。不要混用其他
Python 次版本或 CPU 架构构建的 wheelhouse。

## 3. 凭证文件

创建独立的策略凭证文件：

```bash
sudo install -d -o root -g perp -m 0750 /etc/perp
sudo install -o perp -g perp -m 0600 \
  /opt/perp/env_robinhood_example.txt /etc/perp/robinhood.env
sudoedit /etc/perp/robinhood.env
```

必须保持下面四项为一个不可拆分的 Robinhood 端点组：

```dotenv
LIGHTER_ENVIRONMENT=robinhood
LIGHTER_BASE_URL=https://api.rh.lighter.xyz
LIGHTER_WS_URL=wss://api.rh.lighter.xyz/stream
LIGHTER_CHAIN_ID=466324
```

另外填写：

- `LIGHTER_ACCOUNT_INDEX`：已充值的 Robinhood Lighter 账户索引；
- `LIGHTER_API_PRIVATE_KEYS`：JSON 对象，暂时使用 `4..254` 的 key index；
- API key 必须能提交 IOC/taker 订单，不能是 maker-only key；
- 不要填写 `L1_WALLET_PRIVATE_KEY` 或 `LIGHTER_L1_PRIVATE_KEY`；
- 虚拟 Aster 模式不需要 `ASTER_API_KEY` 和 `ASTER_SECRET_KEY`。

可选的协调机遥测也只能写入这个受限文件：

```dotenv
HEDGE_COORDINATOR_URL=https://coordinator.example
HEDGE_COORDINATOR_AGENT=rh-btc-01
HEDGE_COORDINATOR_USERNAME=agent
HEDGE_COORDINATOR_PASSWORD=REPLACE_IN_PROTECTED_FILE
```

用户名和密码必须成对配置；带认证的远程协调机必须使用 HTTPS。本部署 runner
不会把这些值转换为 `--coordinator-*` 命令行参数，因此密码不会出现在进程列表。
不要把用户名或密码嵌入 `HEDGE_COORDINATOR_URL`，预检和策略都会拒绝这种配置。
`/etc/perp/robinhood-service.env` 中禁止出现任何 `HEDGE_COORDINATOR_*` 项。

不要在命令行、shell history、systemd unit、聊天或日志中粘贴私钥。预检只会
报告密钥结构是否正确，不会回显密钥。

## 4. 只读预检

以下命令不会创建 signer、发送认证请求或提交订单：

```bash
sudo -u perp -H env \
  PERP_PROJECT_ROOT=/opt/perp \
  PERP_VENV=/opt/perp/.venv \
  PERP_ENV_FILE=/etc/perp/robinhood.env \
  bash /opt/perp/deploy/robinhood/preflight.sh
```

预检会阻止以下问题：

- 凭证文件可被 group/other 读取、文件由错误用户拥有或是符号链接；
- Robinhood REST、WS、chain ID 或环境名称不匹配；
- 账户索引或 API key JSON/格式不合法；
- Core L1 自动充值密钥混入 Robinhood 配置；
- Python、SDK 导入或策略 CLI 失败；
- DNS、TCP/TLS、公共 HTTPS 或主机时钟异常。
- runner 指定数量不符合实时 size step、`min_base_amount` 或经 1% 保守折扣后的
  `min_quote_amount`。

`--skip-network` 只用于隔离诊断；使用它产生的结果不能作为上线依据。

预检有意不执行认证读取，因此还必须在 Robinhood Lighter UI 中人工确认：

1. 账户已经充值，保证金足以覆盖两条真实 Lighter 腿和手续费；
2. 账户没有旧挂单；
3. 初始仓位符合预期，首次部署建议为零；
4. API key 权限正确，费率和账户等级已核实。

## 5. 第一次真实 canary

第一次只运行一个循环，并使用 Binance 虚拟触发和 2 倍 Lighter 杠杆。下面的
`0.00020 BTC` 只是初始示例，必须先确认它不低于交易所当前最小下单量：

```bash
sudo -u perp -H /usr/bin/flock \
  --no-fork --exclusive --nonblock \
  /opt/perp/logs/robinhood-strategy.lock \
  /usr/bin/bash /opt/perp/deploy/robinhood/run.sh \
  --env-file /etc/perp/robinhood.env \
  --ticker BTC \
  --quantity 0.00020 \
  --direction buy \
  --leverage 2 \
  --slippage 0.02 \
  --lighter-max-wait 10 \
  --aster-maker-depth 10 \
  --mode canary \
  --confirm-live
```

`--confirm-live` 表示确认 Lighter 腿是真实交易。runner 固定添加
`--virtual-aster-maker --virtual-maker-price-source bn`、
`--preserve-initial-position` 和 `--cycles 1`，不允许通过额外参数覆盖。
循环上限按“尝试次数”计算：即使 IOC 被取消/拒绝或发生网络错误，本次 canary
也会在完成回仓检查后退出，不会自动开始第二次真实尝试。

canary 完成后核对：

- 只有预期的两条 Lighter IOC 腿，没有 Aster 私有订单；
- 最终 Lighter 仓位回到启动基线；
- 实际成交量、滑点和手续费与日志一致；
- 没有 `nonce gap`、部分成交残留、紧急恢复失败或 WS 持续重连；
- `logs/` 和 `journalctl` 中没有私钥或认证载荷。

任何一项不满足都应停止，不要用增大重试次数或直接重启来掩盖残仓。

## 6. 安装 systemd 和日志轮转

手工 canary 验证后，安装 unit。这个动作不会 enable/start 服务：

```bash
sudo bash /opt/perp/deploy/robinhood/install.sh \
  --project-root /opt/perp \
  --venv /opt/perp/.venv \
  --skip-dependencies \
  --install-systemd \
  --service-user perp \
  --service-group perp
```

检查非敏感运行配置 `/etc/perp/robinhood-service.env`。它不应包含任何私钥。
首次保留：

```dotenv
PERP_ENV_FILE=/etc/perp/robinhood.env
PERP_VENV=/opt/perp/.venv
PERP_TICKER=BTC
PERP_QUANTITY=0.00020
PERP_RUN_MODE=canary
```

验证渲染结果后才允许启动：

```bash
sudo systemd-analyze verify /etc/systemd/system/perp-robinhood.service
sudo logrotate --debug /etc/logrotate.d/perp-robinhood
sudo systemctl start perp-robinhood.service
sudo systemctl status perp-robinhood.service
sudo journalctl -u perp-robinhood.service -f
```

unit 的行为是：

- 外层 `flock` 与手工 canary 共用 `logs/robinhood-strategy.lock`，策略内部还按账户持有第二层锁；
- systemd 停止时先发送 `SIGINT`，最多等待 120 秒完成仓位恢复；
- 默认 canary 无论成功或失败都不自动重启，确保一次启动最多一次循环尝试；
- 单实例锁冲突码 73 和残仓恢复阻断码 78 始终不自动重启；
- 代码和系统目录只读，只有项目 `logs/` 可写；
- 日志按日轮转，保留 14 份并压缩。

不要使用 `Restart=always`，也不要同时从 cron、tmux、Docker 和 systemd 启动。

## 7. 连续运行闸门

至少积累多次单轮 canary，覆盖买卖两个方向，并确认停机恢复以后，才编辑
`/etc/perp/robinhood-service.env`：

```dotenv
PERP_RUN_MODE=continuous
PERP_CONTINUOUS_ACK=I_ACKNOWLEDGE_CONTINUOUS_LIVE_TRADING
```

连续模式把 `--cycles` 设置为 `0`。修改前先执行 `systemctl stop` 并确认仓位；
修改后不需要 `daemon-reload`，但需要显式 `systemctl start`。启用开机启动也应与
首次连续启动分开执行：

```bash
sudo systemctl enable perp-robinhood.service
sudo systemctl start perp-robinhood.service
```

连续模式稳定运行后，如需只对明确分类的临时网络故障（退出码 75）自动重启，
再安装可选 drop-in；canary 阶段不要安装：

```bash
sudo install -d -o root -g root -m 0755 \
  /etc/systemd/system/perp-robinhood.service.d
sudo install -o root -g root -m 0644 \
  /opt/perp/deploy/robinhood/network-restart.conf.example \
  /etc/systemd/system/perp-robinhood.service.d/network-restart.conf
sudo systemctl daemon-reload
```

该 drop-in 只强制重启退出码 75；普通运行错误、重复实例 73 和库存阻断 78
仍保持停止。systemd 的 `StartLimitBurst=3` 会限制连续启动次数。

最低监控项包括：服务退出、连续 WS 重连、nonce 不连续、API 拒单/限流、
Lighter 仓位偏离基线、紧急恢复被阻断、磁盘空间和日志增长。生产告警没有配置
完成之前，不应无人值守运行。

## 8. 停止和故障处理

```bash
sudo systemctl stop perp-robinhood.service
sudo journalctl -u perp-robinhood.service --since '-30 min'
```

停止后必须到交易所或通过独立只读工具确认最终仓位。若发现残仓、部分成交、
nonce 错误或恢复阻断：

1. 禁止重启服务并执行 `systemctl disable perp-robinhood.service`；
2. 保存日志、订单 ID、成交 ID、启动基线和当前仓位；
3. 按账户风险人工处理仓位，不用循环脚本盲目追单；
4. 找到原因并重新完成单轮 canary 后再恢复。

部署完成只代表程序具备运行条件，不代表交易、费率、法律或地区合规已经通过。
