# TradingAgents AI 研究层方案

## 1. 目标和边界

当前实盘交易主程序仍然是仓库根目录的 `bian_new.py`。它负责 ETHUSDT 的实时行情、技术信号、入场、止损、目标、订单和仓位管理。

`tradingAgents/` 目录下的代码只做低频研究，不直接参与实盘执行。

核心边界：

```text
可以做：
  生成 TradingAgents 多 Agent 研究报告
  汇总新闻、宏观、行情摘要
  生成方向偏见 JSON
  做观察和复盘

不能做：
  直接下单
  撤单
  修改止损
  扩大杠杆
  绕过 bian_new.py 风控
  在数据缺失时强行给方向
```

当前阶段：

```text
第 1 阶段：只做观察，不拦截交易。
```

`bian_new.py` 当前没有读取 `.ai_research/latest_bias_ETHUSDT.json`，所以 AI 研究层不会影响实盘交易。

## 2. 当前文件

```text
tradingAgents/
  analyze_eth_tradingagents.py
  ai_market_bias.py
  ai_research_integration_plan.md
  .env.example
```

### analyze_eth_tradingagents.py

职责：

```text
1. 启动 TradingAgents 多 Agent 工作流。
2. 在当前 Python 进程里 patch TradingAgents vendor 路由。
3. 避免默认 yfinance / Alpha Vantage 路径造成限流或数据不可控。
4. 生成完整研究报告和状态文件。
```

输出：

```text
outputs/ETH-USD_YYYY-MM-DD_summary.md
outputs/ETH-USD_YYYY-MM-DD_state.json
outputs/ETH-USD_YYYY-MM-DD_decision.json
```

### ai_market_bias.py

职责：

```text
1. 读取最新 TradingAgents summary。
2. 拉取辅助上下文：Finnhub / RSS / GDELT / FRED / Binance。
3. 调 DeepSeek，把 TradingAgents 报告归一化为结构化 bias JSON。
4. 校验 fail-open 规则。
5. 写 .ai_research/latest_bias_ETHUSDT.json。
```

重要：`ai_market_bias.py` 是 TradingAgents-first。它默认要求有新鲜 TradingAgents summary，不应该退化成单 DeepSeek 模型自己判断行情。

### 文件清单和用途

#### 仓库内应该上传的文件

| 文件 | 由谁产生 | 用途 | 是否上传 git |
|---|---|---|---|
| `tradingAgents/analyze_eth_tradingagents.py` | 当前实现 | TradingAgents 研究入口；运行时 patch 数据源；输出多 Agent 报告 | 是 |
| `tradingAgents/ai_market_bias.py` | 当前实现 | 读取 TradingAgents 报告，结合辅助数据，生成 bias JSON | 是 |
| `tradingAgents/ai_research_integration_plan.md` | 当前实现 | 当前方案文档、运行命令、下一步计划 | 是 |
| `tradingAgents/.env.example` | 当前实现 | 环境变量模板，只放占位符，不放真实 key | 是 |

#### 运行后会产生但不上传的文件和目录

| 路径 | 由谁产生 | 用途 | 为什么不上传 |
|---|---|---|---|
| `tradingAgents/.env` | 用户手工创建 | 存真实 API key，例如 `DEEPSEEK_API_KEY` | 含密钥，不能进 git |
| `tradingAgents/.ai_research/latest_bias_ETHUSDT.json` | `ai_market_bias.py` | 最新 AI bias 输出，未来可给观察/实盘读取 | 运行态结果，会频繁变化 |
| `tradingAgents/.ai_research/cache/` | `ai_market_bias.py` | 新闻、FRED、Binance、TradingAgents 输入快照缓存 | 缓存数据，体积和内容会变 |
| `tradingAgents/.ai_research/logs/` | 计划中的观察日志脚本 / cron | AI bias 观察日志、cron 日志 | 运行日志，不属于源码 |
| `tradingAgents/outputs/*_summary.md` | `analyze_eth_tradingagents.py` | 人看的 TradingAgents 研究报告 | 每次运行结果，不属于源码 |
| `tradingAgents/outputs/*_state.json` | `analyze_eth_tradingagents.py` | TradingAgents 全流程状态，内容最全 | 文件大、运行态输出 |
| `tradingAgents/outputs/*_decision.json` | `analyze_eth_tradingagents.py` | TradingAgents 最终决策 | 每次运行结果 |
| `tradingAgents/.tradingagents/cache/` | TradingAgents 框架 | TradingAgents 数据缓存 | 缓存数据 |
| `tradingAgents/.tradingagents/logs/` | TradingAgents 框架 | TradingAgents 内部运行日志 | 日志文件 |
| `tradingAgents/.tradingagents/memory/` | TradingAgents 框架 | TradingAgents memory log | 运行态记忆，不适合提交 |
| `tradingAgents/__pycache__/` | Python | Python 字节码缓存 | 可再生成，无需提交 |


## 3. 当前数据流

```text
analyze_eth_tradingagents.py
        |
        v
outputs/ETH-USD_YYYY-MM-DD_summary.md
outputs/ETH-USD_YYYY-MM-DD_state.json
outputs/ETH-USD_YYYY-MM-DD_decision.json
        |
        v
ai_market_bias.py
        |
        v
.ai_research/latest_bias_ETHUSDT.json
```

当前没有这一步：

```text
.ai_research/latest_bias_ETHUSDT.json -> bian_new.py
```

也就是说，bias 文件目前只用于观察和测试。

## 4. TradingAgents 数据源替换方式

没有修改 `site-packages/tradingagents` 源码。

替换方式是在 `analyze_eth_tradingagents.py` 运行时 patch：

```python
install_no_yfinance_adapters(TradingAgentsGraph)
```

这个函数会在当前 Python 进程里替换 TradingAgents 的 vendor 方法：

```text
get_stock_data
get_indicators
get_fundamentals
get_balance_sheet
get_cashflow
get_income_statement
get_news
get_global_news
get_insider_transactions
```

影响范围：

```text
通过 analyze_eth_tradingagents.py 启动 TradingAgents：
  使用本方案的数据源。

直接使用 TradingAgents 原始 CLI 或其他脚本：
  不受影响，仍用 TradingAgents 默认数据源。
```

换服务器时，只要带上 `tradingAgents/` 目录并安装 TradingAgents 包即可，不需要改 TradingAgents 源码。

## 5. 当前使用的数据源

只列当前已经接入并测试过的数据源。

### 5.1 Crypto 行情和技术

```text
Binance USD-M Futures public klines
```

用途：

```text
ETHUSDT OHLCV
TradingAgents market analyst
TradingAgents technical indicators
ai_market_bias.py Binance 多周期摘要
```

### 5.2 Crypto 新闻

```text
Finnhub crypto news
RSS feeds
GDELT
```

用途：

```text
TradingAgents news/sentiment 输入
ai_market_bias.py 新闻上下文
```

### 5.3 宏观数据

```text
FRED
```

当前 series：

```text
FEDFUNDS
CPIAUCSL
UNRATE
DGS10
DGS2
```

用途：

```text
ai_market_bias.py 宏观上下文
```

### 5.4 美股行情

用于股票研究，不接入 ETHUSDT 实盘。

fallback 顺序：

```text
Finnhub stock candle
Nasdaq historical
Yahoo chart direct
Stooq daily CSV
```

### 5.5 美股基本面

```text
SEC companyfacts
```

用途：

```text
company overview
balance sheet
cashflow
income statement
```

### 5.6 美股 insider

```text
SEC submissions
```

用途：

```text
Form 3 / Form 4 / Form 5 filing list
```

## 6. 环境变量

从 `.env` 读取。仓库只上传 `.env.example`，真实 `.env` 不上传。

`.env.example`：

```env
DEEPSEEK_API_KEY=your_deepseek_key_here
FINNHUB_API_KEY=
FRED_API_KEY=
ALPHA_VANTAGE_API_KEY=
```

当前默认 no_yfinance 路径不依赖 Alpha Vantage。`ALPHA_VANTAGE_API_KEY` 保留只是为了兼容手动选择原生 vendor 的情况。

必需：

```text
DEEPSEEK_API_KEY
```

建议配置：

```text
FINNHUB_API_KEY
FRED_API_KEY
```

## 7. TradingAgents 运行命令

当前推荐命令：

```bash
python tradingAgents/analyze_eth_tradingagents.py \
  --ticker ETH-USD \
  --asset-type crypto \
  --provider deepseek \
  --deep deepseek-v4-pro \
  --quick deepseek-v4-flash \
  --offline \
  --llm-timeout 300 \
  --llm-max-retries 1 \
  --max-data-rows 60 \
  --max-news-items 12 \
  --max-news-summary-chars 500
```

如果在 `tradingAgents/` 目录内运行：

```bash
python analyze_eth_tradingagents.py \
  --ticker ETH-USD \
  --asset-type crypto \
  --provider deepseek \
  --deep deepseek-v4-pro \
  --quick deepseek-v4-flash \
  --offline \
  --llm-timeout 300 \
  --llm-max-retries 1 \
  --max-data-rows 60 \
  --max-news-items 12 \
  --max-news-summary-chars 500
```

参数说明：

```text
--deep deepseek-v4-pro
  深度分析模型，用于更重要的 TradingAgents agent。

--quick deepseek-v4-flash
  快速模型，用于轻量 agent。

--offline
  关闭 TradingAgents 原生 online tools。
  注意：这不等于完全断网。本方案的 adapter 仍会访问 Binance/Finnhub/RSS/GDELT/FRED/SEC 等数据源。

--llm-timeout 300
  单次 LLM 请求最多等待 300 秒。

--llm-max-retries 1
  LLM 超时或失败后最多重试 1 次。

--max-data-rows 60
  返回给 LLM 的 OHLCV 最多 60 行。
  技术指标内部仍可使用完整拉取数据。

--max-news-items 12
  返回给 LLM 的新闻最多 12 条。

--max-news-summary-chars 500
  每条新闻摘要最多 500 字符。
```

为什么要限制数据量：

```text
TradingAgents 会把工具输出拼进多个 LLM agent prompt。
OHLCV CSV 和新闻过长时，DeepSeek V4 容易在响应读取阶段变慢或超时。
限制输出体积后，TradingAgents 仍保留多 Agent 流程，但稳定性明显提高。
```

## 8. ai_market_bias.py 命令

只抓数据，不调用 DeepSeek：

```bash
python tradingAgents/ai_market_bias.py --symbol ETHUSDT --hours 8 --fetch-only
```

调用 DeepSeek 归一化，但不写 latest bias：

```bash
python tradingAgents/ai_market_bias.py --symbol ETHUSDT --hours 8 --dry-run
```

写研究层 bias 文件：

```bash
python tradingAgents/ai_market_bias.py --symbol ETHUSDT --hours 8
```

输出：

```text
tradingAgents/.ai_research/latest_bias_ETHUSDT.json
```

当前写出这个文件也不会影响实盘，因为 `bian_new.py` 尚未读取它。

## 9. Bias JSON 规则

schema：

```json
{
  "symbol": "ETHUSDT",
  "generated_at": "2026-07-07T21:02:22+08:00",
  "expires_at": "2026-07-08T05:02:22+08:00",
  "bias": "bullish|bearish|neutral|mixed",
  "confidence": 0.0,
  "allow_long": true,
  "allow_short": true,
  "size_multiplier": 1.0,
  "reason": "short reason",
  "risk_events": [],
  "news_used": [],
  "source_counts": {}
}
```

每个 key 的作用：

| key | 类型 | 作用 | 当前是否用于实盘 |
|---|---|---|---|
| `symbol` | string | bias 对应的交易标的。当前固定为 `ETHUSDT`。 | 否 |
| `generated_at` | ISO datetime | bias 生成时间，统一使用实盘交易时区：北京时间/UTC+8，必须带时区偏移，例如 `+08:00`。 | 否 |
| `expires_at` | ISO datetime | bias 过期时间，统一使用实盘交易时区：北京时间/UTC+8，必须带时区偏移。未来接入时，过期必须 fail-open。 | 否 |
| `bias` | string | 方向偏见，只允许 `bullish`、`bearish`、`neutral`、`mixed`。 | 否 |
| `confidence` | number | 置信度，范围 `0.0` 到 `1.0`。低于阈值不允许拦截。 | 否 |
| `allow_long` | boolean | 未来接入时是否允许策略开多。当前只记录，不执行。 | 否 |
| `allow_short` | boolean | 未来接入时是否允许策略开空。当前只记录，不执行。 | 否 |
| `size_multiplier` | number | 未来可能用于降仓，范围通常 `0.0` 到 `1.0`。当前不影响仓位。 | 否 |
| `reason` | string | 简短解释，说明为什么给出该 bias。用于人工复盘。 | 否 |
| `risk_events` | array[string] | 需要关注的风险事件，例如宏观事件、监管、重大新闻。 | 否 |
| `news_used` | array[string] | DeepSeek 归一化时认为关键的新闻标题或主题。用于追溯来源。 | 否 |
| `source_counts` | object | 本次输入里各数据源数量，例如 Finnhub/RSS/GDELT。用于判断数据覆盖度。 | 否 |

注意：

```text
当前这些 key 都不会被 bian_new.py 读取。
它们只是观察期记录和未来接入时的协议草案。
时间字段只保留一套口径：和 `bian_new.py` 的 `EXCHANGE_TZ` 一致，使用北京时间/UTC+8 ISO 时间。
未来实盘读取时必须按 timezone-aware datetime 解析，不能去掉 `+08:00` 后用字符串或 naive datetime 比较。
```

fail-open 规则：

```text
confidence < 0.55:
  allow_long = true
  allow_short = true

bias = neutral 或 mixed:
  allow_long = true
  allow_short = true

bias = bullish 且 confidence >= 0.65:
  allow_long = true
  allow_short = false

bias = bearish 且 confidence >= 0.65:
  allow_long = false
  allow_short = true
```

解释：

```text
低置信度不拦截。
mixed/neutral 不拦截。
只有高置信度且方向明确，未来才可能用于过滤或降仓。
```

## 10. 当前测试结果

### 10.1 数据源测试

已通过：

```text
ETH OHLCV / Binance
ETH 新闻 / Finnhub + RSS + GDELT
AAPL OHLCV / Nasdaq fallback
AAPL SEC income statement
AAPL SEC balance sheet
AAPL SEC cashflow
AAPL SEC insider filings
FRED macro cache
Binance 多周期摘要
```

### 10.2 TradingAgents 完整流程

已成功生成：

```text
outputs/ETH-USD_2026-07-07_summary.md
outputs/ETH-USD_2026-07-07_state.json
outputs/ETH-USD_2026-07-07_decision.json
```

使用参数：

```bash
--deep deepseek-v4-pro
--quick deepseek-v4-flash
--llm-timeout 300
--max-data-rows 60
--max-news-items 12
--max-news-summary-chars 500
```

结果：

```text
TradingAgents final decision: Overweight
```

### 10.3 ai_market_bias.py

已通过：

```text
--fetch-only
--dry-run
正常写 latest_bias_ETHUSDT.json
JSON schema 校验
fail-open 规则校验
```

基于 2026-07-07 TradingAgents 报告的 dry-run 输出：

```json
{
  "bias": "bullish",
  "confidence": 0.55,
  "allow_long": true,
  "allow_short": true,
  "size_multiplier": 0.5
}
```

因为 `confidence = 0.55` 未达到 `0.65`，所以不拦截任何方向。

## 11. 运行产物目录

这些目录是运行产物，不属于方案源码。

### .ai_research/

`ai_market_bias.py` 的运行目录。

```text
.ai_research/latest_bias_ETHUSDT.json
.ai_research/cache/
.ai_research/logs/
```

### outputs/

`analyze_eth_tradingagents.py` 导出的报告目录。

```text
outputs/ETH-USD_YYYY-MM-DD_summary.md
outputs/ETH-USD_YYYY-MM-DD_state.json
outputs/ETH-USD_YYYY-MM-DD_decision.json
```

### .tradingagents/

TradingAgents 框架自己的缓存、日志和 memory。

```text
.tradingagents/cache/
.tradingagents/logs/
.tradingagents/memory/
```

## 12. 调度建议

先只跑观察任务。

```cron
50 23,7,15 * * * cd /home/ubuntu/binance_auto && python tradingAgents/analyze_eth_tradingagents.py --ticker ETH-USD --asset-type crypto --provider deepseek --deep deepseek-v4-pro --quick deepseek-v4-flash --offline --llm-timeout 300 --llm-max-retries 1 --max-data-rows 60 --max-news-items 12 --max-news-summary-chars 500 >> tradingAgents/.ai_research/logs/tradingagents.log 2>&1
0 0,8,16 * * * cd /home/ubuntu/binance_auto && python tradingAgents/ai_market_bias.py --symbol ETHUSDT --hours 8 >> tradingAgents/.ai_research/logs/cron.log 2>&1
```

注意：

```text
ai_market_bias.py 默认要求有最新 TradingAgents summary。
如果 summary 缺失或过期，应失败，而不是偷偷变成单模型分析。
```

## 13. 下一步

### A. 观察期日志化

优先级最高。

目标：不改变交易行为，只把 AI bias 和市场状态记录下来。

要做：

```text
1. 新增独立观察日志脚本，不改 bian_new.py 主流程。
2. 读取 .ai_research/latest_bias_ETHUSDT.json。
3. 读取最新 TradingAgents summary/decision。
4. 读取 Binance 当前多周期摘要。
5. 追加写 .ai_research/logs/ai_bias_YYYY-MM.csv。
```

每条记录至少包含：

```text
generated_at
expires_at
bias
confidence
allow_long
allow_short
size_multiplier
reason
risk_events
source_counts
latest_price
1h trend
4h trend
1d trend
tradingagents_decision
tradingagents_summary_path
```

验收：

```text
.ai_research/logs/ai_bias_YYYY-MM.csv 每天稳定追加。
脚本异常不影响 bian_new.py。
不存在任何下单、撤单、改止损逻辑。
```

### B. TradingAgents 稳定性观察

目标：确认低频报告能稳定落盘。

要做：

```text
1. 连续跑 7 天。
2. 记录每次是否成功、耗时、final decision。
3. 如果仍有 DeepSeek timeout：
   - max-data-rows 降到 45
   - max-news-items 降到 8
   - max-news-summary-chars 降到 350
4. 如果稳定，再考虑每天 1 次启用 debate-rounds 1 / risk-rounds 1。
```

验收：

```text
连续 7 天至少每天成功生成 1 份 ETH-USD summary。
失败日志能明确区分数据源失败或 LLM timeout。
```

### C. bian_new.py 只记录接入

观察期后再做。

目标：`bian_new.py` 读取 AI bias，但不拦截，只写日志。

要做：

```text
1. 在开仓候选生成后读取 latest_bias_ETHUSDT.json。
2. 校验 schema 和 expires_at。
3. 写日志：
   candidate side
   AI bias
   confidence
   allow_long / allow_short
   size_multiplier
   reason
4. 不跳过开仓，不改仓位，不改止损。
```

验收：

```text
bias 缺失、过期、解析失败时 fail-open。
交易行为和接入前完全一致。
日志可用于后续复盘。
```

### D. 复盘后再决定是否过滤

目标：用样本决定 AI 是否值得参与过滤。

要做：

```text
1. 收集 30-60 天样本。
2. 对比 AI 支持/反对/mixed 的信号后续表现。
3. 先评估降仓，不优先硬拦截。
4. 只有 confidence >= 0.65 且样本表现稳定，才考虑启用方向过滤。
```

## 15. 参考来源

- Binance USD-M Futures common definitions/rate limits: https://developers.binance.com/docs/derivatives/usds-margined-futures/common-definition
- FRED API key documentation: https://fred.stlouisfed.org/docs/api/api_key.html
- GDELT overview/free open platform: https://www.gdeltproject.org/
- Finnhub API/pricing: https://finnhub.io/pricing
- SEC EDGAR APIs: https://www.sec.gov/search-filings/edgar-application-programming-interfaces
