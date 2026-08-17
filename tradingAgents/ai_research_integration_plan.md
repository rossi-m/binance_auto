# TradingAgents AI 研究与多交易对持仓管理方案

文档状态：

```text
更新时间：2026-08-17
运行目录：/home/ubuntu/binance
交易环境：Binance USD-M Demo
当前 AI 模式：manage
当前状态：脚本 ETH 与任意手动 USD-M 持仓均已接入逐交易对 TradingAgents 研究
```

## 1. 当前结论

`ai_market_bias.py` 仍然是独立的研究结果生产脚本，本身不下单，也不直接管理仓位。

现在已经存在完整连接：`bian_new.py` 确认脚本 ETH 或手动 USD-M 仓位后，会异步启动 `run_symbol_research.py`；后者按实际交易对运行 TradingAgents 和 `ai_market_bias.py`，生成该交易对自己的 bias 文件，再由 `bian_new.py` 校验并使用。

例如：

```text
持仓 SYMBOL（例如 BTCUSDT）
  -> run_symbol_research.py --symbol SYMBOL
  -> analyze_eth_tradingagents.py --ticker BASE-USD
  -> ai_market_bias.py --symbol SYMBOL
  -> .ai_research/latest_bias_SYMBOL.json
  -> bian_new.py 校验后最多执行受限的部分减仓
```

这套实现没有手动仓位币种白名单。只要 Binance USD-M 账户返回的是有效非零仓位，BTC、SOL、PEPE 等交易对都可以进入接管流程；脚本自己的 ETH 仓位使用同一套研究调度器，但仓位和止损仍由原 `trade_state` 管理。

## 2. 安全边界

职责分工：

```text
TradingAgents / ai_market_bias.py：
  生成研究报告和结构化 bias。
  不持有交易所执行权。

bian_new.py：
  读取账户仓位。
  计算和更新动态止损。
  校验 bias。
  决定是否执行受控减仓。
  负责所有交易所订单操作和最终安全约束。
```

AI 的硬限制：

```text
不能凭空开仓。
不能增加已有仓位。
不能完全平仓。
不能自动反手。
不能放宽已经生效的动态止损。
不能修改杠杆。
bias 缺失、损坏、过期或不匹配时必须 fail-open。
```

这里的 fail-open 是指 AI 不影响原策略或 ATR 止损管理，不是指取消保护单，也不是指强制开仓。

当前脚本明确调用：

```python
exchange.enable_demo_trading(True)
```

AI 代码不能修改 Demo/正式环境开关。

## 3. 文件职责

| 文件 | 当前职责 |
|---|---|
| `bian_new.py` | ETH 原策略、全账户手动仓位发现、动态止损、订单归属、固定三时点逐交易对 AI 调度和受限减仓 |
| `tradingAgents/run_symbol_research.py` | 单交易对后台任务入口；负责 symbol 转换和 `flock` 防重入 |
| `tradingAgents/analyze_eth_tradingagents.py` | TradingAgents 多 Agent 研究入口；文件名保留历史名称，但已支持任意有效 USD/USDT/USDC 加密交易对 |
| `tradingAgents/ai_market_bias.py` | 读取 TradingAgents 报告和辅助数据，生成 `latest_bias_SYMBOL.json` |
| `tradingAgents/ai_research_guard.py` | 配置校验、bias 校验、fail-open、候选过滤、部分减仓计算和 JSONL 审计 |

本地工作目录另外保留 `test_bian_manual_takeover.py` 和 `test_tradingagents_multi_symbol.py` 做回归验证；按当前仓库约定，这两个测试文件不上传到 Git。

## 4. 实际运行链路

### 4.1 自动链路

```text
bian_new.py 每轮读取 Binance USD-M 全账户非零仓位
  |
  +-- 原策略创建的 ETH 仓位 -> 原 ETH trade_state 管理
  |       +-- ETH bias 无效时立即补跑，到固定批次时再次运行
  |       +-- 生成并读取 latest_bias_ETHUSDT.json
  |
  +-- 其他仓位 -> manual_position_states 独立管理
          |
          +-- 先选择周期并创建 TAM_ 动态保护止损
          |
          +-- bias 无效时立即补跑，到固定批次时异步启动 run_symbol_research.py
                  |
                  +-- 每个 symbol 使用独立 flock
                  |
                  +-- ai_market_bias.py --run-tradingagents
                          |
                          +-- analyze_eth_tradingagents.py --ticker BASE-USD
                          +-- symbol 相关的新闻、宏观和 Binance 行情
                          +-- latest_bias_SYMBOL.json 原子写入
          |
          +-- bian_new.py 下轮读取并严格校验该 symbol 的 bias
          +-- 满足条件时最多执行受限的部分减仓
          +-- ATR/结构动态止损始终独立运行
```

后台研究不会阻塞每秒交易循环。脚本 ETH 和手动 BTC 同时持仓时，会分别启动 ETHUSDT、BTCUSDT 任务；两个进程可以并行，分别生成独立 JSON，不会互相覆盖。首次发现手动仓位时，脚本先尝试创建保护止损，然后才启动研究任务。

调度使用“即时补缺 + 固定批次”：bias 缺失、损坏、过期、symbol 不匹配或 schema 校验失败时立即生成一次；只要该交易对仍有持仓，之后重新对齐北京时间 `05:50`、`13:50`、`20:50` 三个固定研究批次。新仓建立前已经错过的批次不补跑；任务失败按重试间隔再次尝试。

### 4.2 固定批次判定

固定时点是 TradingAgents 任务的启动时间，不再像旧 cron 方案那样把“报告”和“bias”拆成相差 10 分钟的两个任务。`run_symbol_research.py` 会在一个进程链路内先完成 TradingAgents 报告，再生成 bias，因此 JSON 通常会晚于固定时点数分钟写入。

以 ETH 在北京时间 10:00 开仓为例：

```text
10:00 没有有效 ETH JSON：立即启动一次 ETHUSDT 研究。
13:49：不提前刷新。
13:50：即使 10:00 的 JSON 仍有效，也启动 13:50 固定批次。
20:50：启动 20:50 固定批次。
次日 05:50：启动 05:50 固定批次，继续保持固定节奏。
```

如果 ETH 在 14:00 才开仓且已有有效 JSON，13:50 是开仓前的批次，不会补跑；下一次固定刷新是 20:50。

是否完成某个批次不依赖内存标记，而是比较 bias 的 `generated_at` 和最近已到达的固定时点。因此 `bian_new.py` 重启后，只要仓位还在，也能继续判断该批次是否已经生成成功。

### 4.3 空仓、失败和进程边界

```text
当前没有任何持仓：不为固定时点主动运行 TradingAgents。
ETH 空仓候选或 pending：只读取已有 ETH bias，不为候选单独启动研究。
新持仓没有有效 bias：立即启动，不等待下一个固定时点。
同一 symbol 已有任务运行：本轮复用运行状态，不重复启动。
任务失败：从上一次启动尝试起至少等待 1800 秒后重试。
任务运行期间仓位关闭：当前不会强制杀掉子进程；该次报告/JSON可以正常完成。
研究失败或仍在运行：ATR、结构止损和原策略继续工作，AI 按 fail-open 处理。
```

### 4.4 分交易对隔离

以 BTC 和 SOL 同时存在为例：

```text
.ai_research/latest_bias_BTCUSDT.json
.ai_research/latest_bias_SOLUSDT.json
.ai_research/jobs/BTCUSDT.lock
.ai_research/jobs/SOLUSDT.lock
.ai_research/logs/manual_ai_BTCUSDT.log
.ai_research/logs/manual_ai_SOLUSDT.log
```

`bian_new.py` 的进程表和 `run_symbol_research.py` 的 `flock` 提供两层同 symbol 防重。同一交易对同一时间只允许一个研究任务；不同交易对没有全局串行锁，因此 ETH、BTC 等任务可以同时运行。`flock` 随进程退出自动释放，锁文件本身保留不代表任务仍在运行。

## 5. 手动仓位接管规则

### 5.1 发现范围

`bian_new.py` 通过 Binance USD-M position-risk/positions 接口读取账户内全部非零仓位，不配置 symbol 白名单。

每笔状态按交易对、持仓模式和方向隔离，避免 BTC、SOL、ETH 或 hedge-mode 多空仓相互覆盖。

如果脚本已有策略 ETH 仓位，同时又手动建立 BTC 仓位：

```text
ETH 继续由原策略管理。
BTC 由 manual_position_states 接管并动态更新 TAM_ 止损。
两笔仓位可以同时被管理。
```

只要账户存在任何仓位，脚本暂停寻找新的 ETH 开仓条件，并撤销脚本自己的未成交 ETH 入场单，避免在管理现有风险时继续加新仓。

### 5.2 初始周期选择

手动仓位首次被发现时同时评估两套配置：

| 配置 | 背景趋势 | 开仓/平仓及动态止损周期 |
|---|---|---|
| `4h_1h` | 4H | 1H |
| `1h_15m` | 1H | 15M |

两套配置都必须先产生有效且位于当前价格安全一侧的止损候选。

选择规则：

```text
多单：选择更高的有效止损，因为距离当前价更近、预期亏损更小。
空单：选择更低的有效止损，因为距离当前价更近、预期亏损更小。
```

选中后，该笔仓位生命周期内锁定对应的出场周期。选中 `1h_15m` 后继续使用 15M 动态止损；选中 `4h_1h` 后继续使用 1H 动态止损，不会每轮来回切换。

### 5.3 ATR 盈利保护

首次接管时会把入场价、当前标记价、最高价/最低价带入两套止损计算。

如果手动仓位在被发现时已经盈利超过 1 ATR，不会从最宽的初始亏损止损重新开始，而是立即应用现有利润锁定阶梯：

| 最大浮盈 | 默认保留利润比例 |
|---|---:|
| `>= 1 ATR` | 30% |
| `>= 2 ATR` | 50% |
| `>= 3 ATR` | 70% |
| `>= 4 ATR` | 90% |

利润锁定、强 K 结构和 4H 支撑阻力只能继续收紧止损，不能把止损向亏损方向放宽。

## 6. 保护单归属和清理

### 6.1 Client Order ID 前缀

```text
TAS_：原 ETH 策略创建的保护单。
TAM_：手动仓位接管模块创建的保护单。
```

持仓仍存在时，刷新动态止损或同步部分平仓后的数量，只撤销和重建脚本自己带前缀的保护单。用户手工创建的止损、止盈和其他条件委托会保留。

部分平仓示例：

```text
BTC 多单从 2 BTC 减到 1 BTC：
  脚本只把 TAM_ 止损数量刷新为剩余 1 BTC。
  用户自己的条件止损/止盈不被撤销。
```

### 6.2 完全平仓后的清理

当手动平仓或任一条件委托使仓位归零时，脚本不会仅凭一次查询缺失立即清理。流程是：

```text
连续 3 轮未发现该仓位
  -> 再调用标准 positions 接口复查
  -> 确认该 symbol/side 数量确实为 0
  -> 清理该 symbol/side 的全部止损和止盈出场条件单
```

这里的“全部出场条件单”包括用户创建和脚本创建的止损/止盈，目的是避免无仓位后残留条件单以后误触发。普通限价开仓委托不在这次清理范围内。

区别必须明确：

```text
持仓存在期间：只撤脚本自己的 TAS_/TAM_ 保护单。
确认完全平仓后：清理该交易对该方向的全部止损/止盈出场条件单。
```

## 7. 平仓后的 20 分钟冷却

`POST_EXIT_COOLDOWN_SECONDS` 当前等于 4 根 5M K 线，即 20 分钟。

| 场景 | 是否写入 ETH 开仓冷却 |
|---|---|
| 原策略 ETH 平仓 | 是 |
| 手动 ETH 平仓或条件单平仓 | 是 |
| 手动 BTC/SOL/其他非 ETH 平仓 | 否 |

非 ETH 手动仓位关闭后不会让 ETH 策略额外等待 20 分钟；但是只要账户里还有其他活跃仓位，新的 ETH 开仓扫描仍然保持暂停。

## 8. TradingAgents 多交易对支持

### 8.1 Symbol 转换

常见输入会转换为统一目标：

| 输入 | TradingAgents ticker | Bias symbol |
|---|---|---|
| `BTC/USDT:USDT` | `BTC-USD` | `BTCUSDT` |
| `SOLUSDT` | `SOL-USD` | `SOLUSDT` |
| `PEPE/USDT` | `PEPE-USD` | `PEPEUSDT` |

`analyze_eth_tradingagents.py` 的历史文件名没有改，但代码已取消基础币白名单。只要有有效基础币和 USD/USDT/USDC 报价格式，就按加密资产处理。

### 8.2 新闻相关性

新闻关键词和 GDELT 查询会跟随实际 symbol：

```text
BTC -> BTC / bitcoin + 通用 crypto 宏观词
SOL -> SOL / solana + 通用 crypto 宏观词
未知币种 -> 直接使用其基础币代码 + 通用 crypto 宏观词
```

别名表用于提高召回率，不是交易对白名单。未知币种仍然可以运行研究，只是新闻源可能较少，最终 confidence 应相应降低。

当前数据源包括：

```text
Binance USD-M 公共 K 线和多周期摘要
Finnhub crypto news
RSS
GDELT
FRED 宏观数据
TradingAgents 多 Agent 报告
```

`Underweight` 或 `SELL` 只表示投资组合立场，不自动等于期货做空；`Overweight` 或 `BUY` 也不自动等于期货做多。

## 9. Bias JSON 协议

每个交易对写独立文件：

```text
tradingAgents/.ai_research/latest_bias_SYMBOL.json
```

示例：

```json
{
  "symbol": "BTCUSDT",
  "generated_at": "2026-08-17T12:00:00+08:00",
  "expires_at": "2026-08-17T21:00:00+08:00",
  "portfolio_stance": "neutral",
  "futures_bias": "mixed",
  "time_horizon_hours": 9,
  "explicit_long_signal": false,
  "explicit_short_signal": false,
  "confidence": 0.55,
  "allow_long": true,
  "allow_short": true,
  "risk_multiplier": 1.0,
  "reason": "short reason",
  "risk_events": [],
  "news_used": [],
  "source_counts": {}
}
```

执行层会验证：

```text
symbol 必须和当前持仓交易对一致。
generated_at / expires_at 必须是带时区的 ISO 时间。
文件不能来自超出容差的未来，也不能已经过期。
枚举、布尔、数组和数值范围必须合法。
confidence 和 risk_multiplier 必须在 0 到 1 之间。
任何异常都返回 fail-open guard。
```

`risk_multiplier` 是目标保留比例，只能用于缩小已有仓位，不能参与扩大仓位或补回已经减掉的数量。

`expires_at` 默认是生成后 9 小时，用来保证执行层不会使用陈旧结论；固定三时点是研究调度规则。两者同时存在：到固定时点会刷新尚未过期的 JSON，而 JSON 如果提前失效或校验失败，则不等固定时点立即补跑。

## 10. manage 模式

当前 `.env.local` 的 AI 相关配置是：

```env
AI_RESEARCH_MODE=manage
AI_ALLOW_POSITION_REDUCTION=1
MANUAL_AI_RESEARCH_ENABLED=1
```

未显式覆盖时，关键默认值由 `ai_research_guard.py` 加载：

```env
AI_BIAS_FILE=/home/ubuntu/binance/tradingAgents/.ai_research/latest_bias_ETHUSDT.json
AI_FILTER_MIN_CONFIDENCE=0.75
AI_REDUCE_MIN_CONFIDENCE=0.75
AI_REVERSE_SIGNAL_RETAINED_POSITION_RATIO=0.5
AI_MIN_RETAINED_POSITION_RATIO=0.5
AI_MAX_POSITION_REDUCTION=0.5
AI_ALLOW_POSITION_REDUCTION=0
AI_ALLOW_FULL_CLOSE=0
AI_ALLOW_REVERSE=0
AI_BIAS_FUTURE_TOLERANCE_SECONDS=300
```

持仓研究调度默认值：

```env
MANUAL_AI_RESEARCH_HOURS=9
MANUAL_AI_RESEARCH_RETRY_SECONDS=1800
```

固定批次直接由 `bian_new.py` 按北京时间判断，不依赖 cron：

```text
05:50
13:50
20:50
```

脚本使用固定 `UTC+8` 时区构造批次时间，因此服务器系统时区不同也不会改变这三个北京时间；服务器系统时钟仍必须保持准确。

`MANUAL_AI_RESEARCH_ENABLED` 是历史兼容名称，现在实际控制所有持仓交易对（包括脚本 ETH）的自动研究。没有显式设置时，只要 `AI_RESEARCH_MODE != off` 就默认启用；显式设为 `0` 时只读取已有 bias，不启动后台研究。

模式能力：

| mode | 读取/审计 | ETH 候选过滤 | 已有仓位部分减仓 | ETH pending 撤销 |
|---|---:|---:|---:|---:|
| `off` | 否 | 否 | 否 | 否 |
| `log` | 是 | 否 | 否 | 否 |
| `reduce` | 是 | 否 | 是 | 否 |
| `filter` | 是 | 是 | 否 | 否 |
| `filter_reduce` | 是 | 是 | 是 | 否 |
| `manage` | 是 | 是 | 是 | 是 |

`manage` 不是无限授权。实际部分减仓仍需同时满足：

```text
bias 有效且属于当前 symbol。
达到减仓置信度门槛。
存在明确反向期货信号，或 risk_multiplier 要求降低仓位。
AI_ALLOW_POSITION_REDUCTION=1。
同一 generated_at 没有重复执行过。
精度和最小下单量合法。
减仓后仍保留安全底线以上的仓位。
```

`AI_ALLOW_FULL_CLOSE=1` 和 `AI_ALLOW_REVERSE=1` 当前会在配置加载时直接报错，不能进入交易循环。`bian_new.py` 在下单前还会再次硬性拒绝减仓数量达到当前全部仓位的请求。

手动 BTC/SOL 等仓位没有“AI 新开仓候选”阶段，因此 AI 对它们只做已有仓位评估；脚本 ETH 才会在空仓候选和 pending 阶段读取 `allow_long` / `allow_short`。

## 11. 运行命令

### 11.1 推荐的逐交易对入口

```bash
python tradingAgents/run_symbol_research.py --symbol BTCUSDT --hours 9
```

它会自动选择 `BTC-USD`，先运行 TradingAgents，再生成：

```text
tradingAgents/.ai_research/latest_bias_BTCUSDT.json
```

`run_symbol_research.py` 当前通过 `ai_market_bias.py` 的默认参数运行 DeepSeek，TradingAgents 的 deep/quick 模型均为 `deepseek-v4-flash`，LLM timeout 为 300 秒、重试 1 次、行情最多 60 行、新闻最多 12 条、每条摘要最多 500 字符。

### 11.2 直接运行 ai_market_bias.py

只抓取研究输入，不调用 DeepSeek：

```bash
python tradingAgents/ai_market_bias.py --symbol BTCUSDT --tradingagents-ticker BTC-USD --hours 9 --fetch-only
```

使用已有新鲜 TradingAgents 报告生成 bias：

```bash
python tradingAgents/ai_market_bias.py --symbol BTCUSDT --tradingagents-ticker BTC-USD --hours 9
```

先运行 TradingAgents 再生成 bias：

```bash
python tradingAgents/ai_market_bias.py \
  --symbol BTCUSDT \
  --tradingagents-ticker BTC-USD \
  --hours 9 \
  --run-tradingagents \
  --asset-type crypto
```

正常运行 `bian_new.py` 时通常不需要手工执行这些命令；任何当前持仓交易对的 bias 无效时会立即后台补跑，之后在北京时间 `05:50`、`13:50`、`20:50` 固定刷新。

## 12. 环境变量加载

`bian_new.py` 从仓库根目录 `.env.local` 加载 Binance、邮件和 AI 执行配置，已存在的系统环境变量优先。

TradingAgents 研究脚本按以下顺序尝试加载，先加载的同名值优先：

```text
1. TRADINGAGENTS_ENV_FILE 指定文件
2. tradingAgents/.env
3. /home/ubuntu/binance/tradingAgents/.env（兼容路径）
4. 仓库根目录 .ai_env.local
5. 仓库根目录 .env.local
6. 当前工作目录 .env
```

研究层至少需要可用的 LLM API key；Finnhub、FRED 等 key 缺失时，对应数据源会降级，但不能因此绕过 bias 的严格校验。

真实 API key 和运行态 bias、报告、缓存、日志不得提交到 Git。

## 13. 运行产物

```text
tradingAgents/.ai_research/latest_bias_SYMBOL.json
tradingAgents/.ai_research/jobs/SYMBOL.lock
tradingAgents/.ai_research/logs/manual_ai_SYMBOL.log
tradingAgents/.ai_research/logs/ai_research_decisions_YYYY-MM.jsonl
tradingAgents/.ai_research/cache/
tradingAgents/outputs/BASE-USD_YYYY-MM-DD_summary.md
tradingAgents/outputs/BASE-USD_YYYY-MM-DD_state.json
tradingAgents/outputs/BASE-USD_YYYY-MM-DD_decision.json
tradingAgents/.tradingagents/cache/
tradingAgents/.tradingagents/logs/
tradingAgents/.tradingagents/memory/
```

这些都是运行产物，不属于源码。

`manual_ai_SYMBOL.log` 的文件名前缀是为兼容已有运行目录保留的历史名称，现在脚本 ETH 和手动交易对的后台研究都使用这个命名格式。

## 14. 当前验证状态

已覆盖的自动测试包括：

```text
任意 USD/USDT/USDC crypto symbol 识别。
CCXT 永续 symbol 规范化。
逐交易对 bias 文件路径。
逐交易对新闻关键词。
后台研究命令使用实际持仓 symbol。
同一 symbol 的 flock 防重入入口。
无有效 JSON 时立即补跑，随后回归固定三时点。
13:49 不提前刷新、13:50 到点刷新。
新仓不补跑开仓前已经错过的批次。
20:50 到次日 05:50 的跨日批次判断。
脚本 ETH 与手动 BTC 使用独立并行研究任务和 JSON。
手动仓位双周期止损选择和周期锁定。
已有 1 ATR 浮盈立即进入利润保护。
TAS_/TAM_ 订单归属识别。
部分平仓只刷新脚本保护单数量。
确认全平后的出场条件单清理。
ETH 与非 ETH 的 20 分钟冷却差异。
脚本 ETH 和手动 BTC 同时管理。
有仓位时暂停新的 ETH 入场扫描。
AI 只能执行 reduceOnly/对应 hedge-side 的部分减仓。
```

Demo 全账户只读查询已验证可以正常返回仓位快照。验证过程不创建真实订单。

## 15. 仍需观察的风险

当前实现完成了代码接入，但仍需继续在 Binance Demo 长时间观察：

```text
不同币种最小下单量和精度边界。
hedge mode 下 LONG/SHORT 双边仓位的长期行为。
网络抖动时 position-risk 和标准 positions 复查的一致性。
条件单成交与仓位归零之间的短暂竞态。
TradingAgents 调用耗时、模型费用和 API rate limit。
小币种新闻不足时 confidence 是否足够保守。
AI 部分减仓后 TAM_ 止损数量刷新是否持续稳定。
```

明确不在当前范围：

```text
AI 主动全平。
AI 自动反手。
AI 自主加仓。
用 AI 替代 ATR/结构止损。
切换到 Binance 正式环境。
```
