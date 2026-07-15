# TradingAgents AI 研究层方案

文档状态：

```text
更新时间：2026-07-11
当前运行目录：/home/ubuntu/binance/tradingAgents
当前阶段：三天观察期，不接入实盘执行
当前协议：portfolio_stance 与 futures_bias 分离的 JSON v2
```

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
  ai_bias_observer.py
  ai_research_failure_classifier.py
  test_ai_market_bias.py
  test_ai_research_failure_classifier.py
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
3. 调 DeepSeek，把 TradingAgents 报告拆分为 portfolio_stance 与短周期 futures_bias，并生成结构化 JSON。
4. 校验 fail-open 规则。
5. 写 .ai_research/latest_bias_ETHUSDT.json。
```

重要：`ai_market_bias.py` 是 TradingAgents-first。它默认要求有新鲜 TradingAgents summary，不应该退化成单 DeepSeek 模型自己判断行情。

重要语义边界：

```text
Underweight / SELL / 减仓不等于期货 short。
Overweight / BUY / 分批买入不等于期货 long。
没有明确短周期期货信号时必须使用 futures_bias=neutral 或 mixed。
```

### ai_research_failure_classifier.py

职责：

```text
1. 读取每轮独立运行日志。
2. 区分 data_source_failure、llm_timeout、authentication_failure 等失败。
3. 供观察脚本写入 tradingagents_failures_YYYY-MM.csv。
```

### 文件清单和用途

#### 仓库内应该上传的文件

| 文件 | 由谁产生 | 用途 | 是否上传 git |
|---|---|---|---|
| `tradingAgents/analyze_eth_tradingagents.py` | 当前实现 | TradingAgents 研究入口；运行时 patch 数据源；输出多 Agent 报告 | 是 |
| `tradingAgents/ai_market_bias.py` | 当前实现 | 读取 TradingAgents 报告，结合辅助数据，生成 bias JSON | 是 |
| `tradingAgents/ai_bias_observer.py` | 当前实现 | 读取 latest bias、TradingAgents 报告和 Binance 摘要，追加观察期 CSV 日志 | 是 |
| `tradingAgents/ai_research_failure_classifier.py` | 当前实现 | 对失败运行日志分类：数据源、LLM timeout、认证或未知失败 | 是 |
| `tradingAgents/test_ai_market_bias.py` | 当前实现 | 验证新 JSON 协议、fail-open 和 CSV schema 迁移 | 是 |
| `tradingAgents/test_ai_research_failure_classifier.py` | 当前实现 | 验证失败分类规则 | 是 |
| `tradingAgents/ai_research_integration_plan.md` | 当前实现 | 当前方案文档、运行命令、下一步计划 | 是 |
| `tradingAgents/.env.example` | 当前实现 | 环境变量模板，只放占位符，不放真实 key | 是 |

#### 运行后会产生但不上传的文件和目录

| 路径 | 由谁产生 | 用途 | 为什么不上传 |
|---|---|---|---|
| `tradingAgents/.env` | 用户手工创建 | 存真实 API key，例如 `DEEPSEEK_API_KEY` | 含密钥，不能进 git |
| `tradingAgents/.ai_research/latest_bias_ETHUSDT.json` | `ai_market_bias.py` | 最新 AI bias 输出，未来可给观察/实盘读取 | 运行态结果，会频繁变化 |
| `tradingAgents/.ai_research/cache/` | `ai_market_bias.py` | 新闻、FRED、Binance、TradingAgents 输入快照缓存 | 缓存数据，体积和内容会变 |
| `tradingAgents/.ai_research/logs/` | `ai_bias_observer.py` / cron | AI bias 观察日志、cron 日志 | 运行日志，不属于源码 |
| `tradingAgents/.ai_research/run_ai_research_observation.sh` | 当前服务器运行配置 | 串行执行研究、bias、observer，带锁和观察截止时间 | 服务器运行脚本，不属于方案源码 |
| `tradingAgents/outputs/*_summary.md` | `analyze_eth_tradingagents.py` | 人看的 TradingAgents 研究报告 | 每次运行结果，不属于源码 |
| `tradingAgents/outputs/*_state.json` | `analyze_eth_tradingagents.py` | TradingAgents 全流程状态，内容最全 | 文件大、运行态输出 |
| `tradingAgents/outputs/*_decision.json` | `analyze_eth_tradingagents.py` | TradingAgents 最终决策 | 每次运行结果 |
| `tradingAgents/.tradingagents/cache/` | TradingAgents 框架 | TradingAgents 数据缓存 | 缓存数据 |
| `tradingAgents/.tradingagents/logs/` | TradingAgents 框架 | TradingAgents 内部运行日志 | 日志文件 |
| `tradingAgents/.tradingagents/memory/` | TradingAgents 框架 | TradingAgents memory log | 运行态记忆，不适合提交 |
| `tradingAgents/__pycache__/` | Python | Python 字节码缓存 | 可再生成，无需提交 |

### 环境变量加载顺序

当前观察任务实际运行代码在 `/home/ubuntu/binance/tradingAgents/`。

`/home/ubuntu/binance_auto/tradingAgents/` 是部署副本之一，但当前观察 cron 不使用它；此前该副本曾因环境变量缺失或无效 key 运行失败。

为了兼容当前服务器已有配置，TradingAgents 研究脚本会按下面顺序加载环境变量，先加载到的 key 优先：

```text
1. TRADINGAGENTS_ENV_FILE 指定的文件
2. /home/ubuntu/binance_auto/tradingAgents/.env
3. /home/ubuntu/binance/tradingAgents/.env
4. /home/ubuntu/binance_auto/.ai_env.local
5. /home/ubuntu/binance_auto/.env.local
6. 当前执行目录下的 .env
```

当前服务器实际使用的是：

```text
/home/ubuntu/binance/tradingAgents/.env
```

这个文件里放 `DEEPSEEK_API_KEY`、`FINNHUB_API_KEY`、`FRED_API_KEY` 等真实密钥，不上传 git。


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

#### 5.2.1 新闻时间窗口和使用方

ETH 资产新闻默认使用最近约 7 天的滚动窗口。一天运行 3 次时，每次都会重新读取最近 7 天，不是只读取上次运行之后的新新闻，因此相邻两次运行的新闻会有较高重合度。

新闻主要提供给：

```text
资产新闻 -> Sentiment Analyst -> sentiment_report
资产新闻 / 全球新闻 -> News Analyst -> news_report
news_report + sentiment_report -> Bull / Bear Researchers 和 Risk Agents
```

`--max-news-items 12` 是单次新闻工具返回上限：

```text
ETH 资产新闻最多 12 条。
News Analyst 如果另外调用全球新闻工具，全球新闻可以再返回最多 12 条。
它不是整个 TradingAgents 流程绝对只能读取 12 条新闻。
```

#### 5.2.2 新闻候选汇总

ETH 新闻选择前先汇总当前已经抓到的候选：

```text
Finnhub crypto：当前最多抓取 max-news-items 条候选。
RSS：CoinDesk、Cointelegraph、Decrypt、Ethereum Blog，每个 feed 当前最多抓取 4 条候选。
GDELT：当前最多抓取 max-news-items 条候选。
```

候选必须满足时间窗口和 crypto/ETH 关键词条件。GDELT 自身使用 Ethereum、ETH、Bitcoin、crypto、spot ETF、stablecoin 等查询条件。

汇总后不再按 `Finnhub -> RSS -> GDELT` 的固定顺序直接取前 12 条，而是统一去重、评分和分配来源配额。

#### 5.2.3 去重规则

```text
1. 标题转为小写。
2. 删除标点、空格等非单词字符。
3. 使用标准化结果的前 120 个字符作为去重 key。
4. 相同 key 只保留综合评分更高的版本。
5. DATA_UNAVAILABLE 等错误占位项不计入新闻数量。
```

同一事件但标题文字明显不同的报道，当前仍可能同时保留。后续可以增加 URL canonicalization、标题相似度和事件聚类。

#### 5.2.4 综合评分

每条候选新闻按四个维度计算确定性分数。这里不调用额外 LLM 预筛选，评分规则可以测试和复现。

##### A. 时效性，最高 40 分

| 发布时间距离当前时间 | 分数 |
|---|---:|
| 6 小时以内 | 40 |
| 6 到 24 小时 | 34 |
| 24 到 72 小时 | 26 |
| 72 到 168 小时 | 16 |
| 168 到 336 小时 | 6 |
| 无法解析发布时间 | 4 |
| 超过 336 小时 | 0 |

##### B. ETH 相关度，最高 30 分

直接 ETH 词包括：

```text
Ethereum、Ether、ETH、staking、Pectra、Layer 2、rollup、EIP
```

广义 crypto 词包括：

```text
Bitcoin、BTC、crypto、stablecoin、DeFi、token、Binance
```

| 命中位置和类型 | 分数 |
|---|---:|
| 标题命中直接 ETH 词 | 30 |
| 摘要或全文命中直接 ETH 词 | 24 |
| 标题只命中广义 crypto 词 | 12 |
| 摘要或全文只命中广义 crypto 词 | 7 |
| 都未命中 | 0 |

##### C. 来源可信度，最高 15 分

来源分数来自新闻 `source` 字段中的发布机构或域名。若同时命中多个规则，取最高分，例如 `finnhub:Reuters` 取 Reuters 的 14 分，而不是 Finnhub 的 10 分。

| 来源 | 分数 |
|---|---:|
| Ethereum 官方博客 | 15 |
| Reuters / Bloomberg | 14 |
| CoinDesk | 13 |
| Decrypt | 12 |
| Cointelegraph | 11 |
| Finnhub 中未识别具体媒体的内容 | 10 |
| 普通 RSS | 8 |
| GDELT | 7 |
| 无法识别 | 5 |

这个分数只代表预设的来源基础权重，不代表已验证文章事实。当前不会自动检查记者署名、转载关系、匿名消息、历史准确率或多来源交叉验证。

##### D. 事件影响力，最高 25 分

标题和摘要按事件类别匹配，每命中一个不同类别加 5 分：

| 事件类别 | 关键词示例 |
|---|---|
| ETF 与资金流 | ETF、inflow、outflow |
| 监管与法律 | SEC、regulation、approval、lawsuit |
| 宏观与利率 | Fed、interest rate、inflation、CPI、yield |
| 安全与故障 | hack、exploit、breach、attack、outage |
| 协议与质押 | upgrade、hard fork、EIP、staking |
| 衍生品与大额资金 | liquidation、whale、open interest、funding rate |
| 地缘政治 | war、conflict、sanction、geopolitical |

计算规则：

```text
正文事件类别分：每类 5 分，先限制到最高 21 分。
只要标题直接命中任意事件类别，再加 4 分。
最终事件影响力最高 25 分。
```

事件影响力只表示“可能影响市场”，不判断利多、利空，也不判断新闻真假。

#### 5.2.5 来源配额和最终选择

在 `max-news-items=12` 且 Finnhub、RSS、GDELT 都有有效候选时：

```text
1. 每个可用来源组先保留该组评分最高的 2 条。
2. 剩余名额按所有候选的综合评分竞争。
3. 正常填充阶段单一来源组最多占 6 条，即总上限的一半。
4. 其他来源没有足够候选时，允许高分来源突破软上限补足 12 条。
5. 最终结果按综合评分从高到低返回给 Agent。
```

该规则解决 Finnhub 先返回 12 条时 RSS 和 GDELT 完全无法进入的问题，同时保留质量不足时由其他来源补位的能力。

#### 5.2.6 当前局限

```text
来源可信度是静态表，不是实时媒体质量评估。
事件影响力依赖关键词，可能有误命中或漏命中。
GDELT source 当前有时只有国家信息，无法精确识别原始媒体。
不同标题描述同一事件时，简单标题去重无法完全合并。
没有执行正文抓取、事实核查或多个独立来源交叉确认。
没有判断新闻是首发、转载、评论还是旧事件重复报道。
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
  单次新闻工具最多返回 12 条。
  ETH 资产新闻和全球新闻工具分别计算上限。

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
python tradingAgents/ai_market_bias.py --symbol ETHUSDT --hours 9 --fetch-only
```

调用 DeepSeek 归一化，但不写 latest bias：

```bash
python tradingAgents/ai_market_bias.py --symbol ETHUSDT --hours 9 --dry-run
```

写研究层 bias 文件：

```bash
python tradingAgents/ai_market_bias.py --symbol ETHUSDT --hours 9
```

先跑 TradingAgents，完成后再写研究层 bias 文件：

```bash
python tradingAgents/ai_market_bias.py --symbol ETHUSDT --hours 9 --run-tradingagents --tradingagents-ticker ETH-USD --asset-type crypto --tradingagents-provider deepseek --tradingagents-deep deepseek-v4-pro --tradingagents-quick deepseek-v4-flash --tradingagents-offline --tradingagents-llm-timeout 300 --tradingagents-llm-max-retries 1 --tradingagents-max-data-rows 60 --tradingagents-max-news-items 12 --tradingagents-max-news-summary-chars 500
```

输出：

```text
tradingAgents/.ai_research/latest_bias_ETHUSDT.json
```

当前写出这个文件也不会影响实盘，因为 `bian_new.py` 尚未读取它。

## 9. ai_bias_observer.py 命令

目的：进入观察期后，把每次 AI bias 和当时市场状态记录下来，形成复盘数据集。

它解决的问题：

```text
只看 latest_bias_ETHUSDT.json 只能知道最新一次判断。
无法复盘过去每次 AI 判断当时的价格、趋势、置信度和 TradingAgents 报告。

ai_bias_observer.py 会把这些信息追加到月度 CSV。
后面才能判断 AI bias 是否有帮助，是否经常误判，是否适合接入 bian_new.py。
```

只打印观察记录，不写 CSV：

```bash
python tradingAgents/ai_bias_observer.py --symbol ETHUSDT --dry-run
```

追加写观察日志：

```bash
python tradingAgents/ai_bias_observer.py --symbol ETHUSDT
```

输出：

```text
tradingAgents/.ai_research/logs/ai_bias_YYYY-MM.csv
```

每行记录包括：

```text
observed_at
symbol
bias_generated_at
bias_expires_at
portfolio_stance
futures_bias
time_horizon_hours
explicit_long_signal
explicit_short_signal
bias
confidence
allow_long
allow_short
risk_multiplier
size_multiplier
reason
risk_events
news_used
source_counts
latest_price
trend_1h
pct_change_1h
trend_4h
pct_change_4h
trend_1d
pct_change_1d
binance_errors
tradingagents_available
tradingagents_age_hours
tradingagents_summary_path
tradingagents_modified_at
tradingagents_decision_path
tradingagents_decision
```

边界：

```text
ai_bias_observer.py 不导入 bian_new.py。
不下单，不撤单，不改止损，不改仓位。
它只读 latest bias、TradingAgents 输出和 Binance 公开行情，然后追加 CSV。
```

## 10. Bias JSON 规则

schema：

```json
{
  "symbol": "ETHUSDT",
  "generated_at": "2026-07-07T21:02:22+08:00",
  "expires_at": "2026-07-08T05:02:22+08:00",
  "portfolio_stance": "overweight|neutral|underweight",
  "futures_bias": "long|short|neutral|mixed",
  "time_horizon_hours": 9,
  "explicit_long_signal": false,
  "explicit_short_signal": false,
  "bias": "bullish|bearish|neutral|mixed",
  "confidence": 0.0,
  "allow_long": true,
  "allow_short": true,
  "risk_multiplier": 1.0,
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
| `portfolio_stance` | string | 投资组合观点：`overweight`、`neutral`、`underweight`。减仓观点不等于期货做空。 | 否 |
| `futures_bias` | string | 未来时间窗口内的期货方向：`long`、`short`、`neutral`、`mixed`。 | 否 |
| `time_horizon_hours` | integer | 期货方向对应的时间窗口，当前为 9 小时。 | 否 |
| `explicit_long_signal` | boolean | 报告是否明确支持当前时间窗口做多，而不只是 Overweight 或分批买入。 | 否 |
| `explicit_short_signal` | boolean | 报告是否明确支持当前时间窗口做空，而不只是 Underweight、SELL 或减仓。 | 否 |
| `bias` | string | `futures_bias` 的兼容字段：`long -> bullish`、`short -> bearish`。 | 否 |
| `confidence` | number | 置信度，范围 `0.0` 到 `1.0`。越高代表证据越一致、方向越明确；它不是胜率，也不代表保证盈利。 | 否 |
| `allow_long` | boolean | 只有明确 short 信号、足够置信度并明确要求禁止多单时才可能为 `false`。 | 否 |
| `allow_short` | boolean | 只有明确 long 信号、足够置信度并明确要求禁止空单时才可能为 `false`。 | 否 |
| `risk_multiplier` | number | 已有仓位相对初始成交量的目标保留比例，范围 `0.0` 到 `1.0`；不参与初始开仓量计算。 | 否 |
| `size_multiplier` | number | `risk_multiplier` 的兼容字段，值保持一致。 | 否 |
| `reason` | string | 简短解释，说明为什么给出该 bias。用于人工复盘。 | 否 |
| `risk_events` | array[string] | 需要关注的风险事件，例如宏观事件、监管、重大新闻。 | 否 |
| `news_used` | array[string] | DeepSeek 归一化时认为关键的新闻标题或主题。用于追溯来源。 | 否 |
| `source_counts` | object | 本次输入里各数据源数量，例如 Finnhub/RSS/GDELT。用于判断数据覆盖度。 | 否 |

### 10.1 portfolio_stance / futures_bias / bias

`portfolio_stance` 和 `futures_bias` 必须分开：

```text
Underweight / SELL / 减少现货多头:
  portfolio_stance 可以是 underweight
  不能仅凭这些词得到 futures_bias = short

Overweight / BUY / 分批买入:
  portfolio_stance 可以是 overweight
  不能仅凭这些词得到 futures_bias = long
```

`bias` 是兼容旧消费者的字段，由 `futures_bias` 映射生成：

```text
long    -> bullish
short   -> bearish
neutral -> neutral
mixed   -> mixed
```

`futures_bias` 是研究层对未来 9 小时期货环境的方向偏见，不是交易信号，也不是订单指令。

| futures_bias | 含义 | 未来接入时的默认态度 |
|---|---|---|
| `long` | 明确支持未来 9 小时期货偏多。 | 仍需 explicit long 信号和明确禁止空单才能限制做空。 |
| `short` | 明确支持未来 9 小时期货偏空。 | 仍需 explicit short 信号和明确禁止多单才能限制做多。 |
| `neutral` | 中性。没有足够证据支持明确方向，或者市场处于等待状态。 | 不拦截多空，交给原策略判断。 |
| `mixed` | 分歧。不同证据互相冲突，例如技术偏多但新闻/宏观偏空，或 TradingAgents 内部观点不一致。 | 不拦截多空，交给原策略判断。 |

重要区别：

```text
futures_bias=long 不等于立刻开多。
futures_bias=short 不等于立刻开空。
neutral/mixed 不等于禁止交易。

AI bias 未来最多只能做过滤、降仓或观察提示。
真正入场、止损、止盈仍由 bian_new.py 的原策略决定。
```

### 10.2 confidence 置信度含义

`confidence` 表示这次研究结论的证据一致性和方向明确程度。

它不是胜率，不是收益率，也不是“越高越一定赚钱”。它只回答一个问题：

```text
这次 TradingAgents 多 Agent 报告 + 新闻 + 宏观 + 行情摘要，对同一个方向的支持有多一致？
```

取值解释：

| confidence 范围 | 含义 | 未来接入时的处理原则 |
|---|---|---|
| `0.00 - 0.54` | 低置信度。证据不足、数据缺失、观点冲突或方向不明确。 | 必须 fail-open，不拦截多空。 |
| `0.55 - 0.64` | 中等置信度。有一定方向，但还不足以影响实盘交易方向。 | 默认不拦截，可用于观察和复盘。 |
| `0.65 - 0.79` | 较高置信度。方向较明确，多个输入互相支持。 | 未来才可能用于过滤反向开仓或降低反向仓位。 |
| `0.80 - 1.00` | 高置信度。证据高度一致，风险事件也相对明确。 | 仍不能直接下单，只能更强地参与过滤或降仓规则。 |

当前阈值：

```text
confidence < 0.55:
  低置信度，不允许影响交易。

confidence >= 0.65、futures_bias 明确、对应 explicit 信号为 true，
并且模型明确要求禁止反向开仓:
  未来才可能影响 allow_long/allow_short。

0.55 <= confidence < 0.65:
  可以记录，但不应用来拦截方向。
```

### 10.3 allow_long / allow_short 含义

`allow_long` 和 `allow_short` 是未来接入实盘时的方向许可位。当前 `bian_new.py` 没有读取它们，所以它们只用于观察。

| allow_long | allow_short | 含义 |
|---|---|---|
| `true` | `true` | 多空都允许。AI 不干预原策略。 |
| `true` | `false` | 只允许做多。必须同时有高置信度、明确 long 信号和明确禁止空单。 |
| `false` | `true` | 只允许做空。必须同时有高置信度、明确 short 信号和明确禁止多单。 |
| `false` | `false` | 当前规则不主动生成这种状态。未来即使出现，也应按 fail-open 或禁止新开仓单独评估，不能未经验证直接接实盘。 |

### 10.4 risk_multiplier / size_multiplier 含义

`risk_multiplier` 是未来接入实盘后用于已有仓位的目标保留比例，`size_multiplier` 是兼容别名，当前观察程序不影响任何实盘仓位。

```text
1.0 = 保留初始成交仓位，不减仓。
0.8 = 已有仓位最多保留到初始成交量的 80%。
0.5 = 已有仓位最多保留到初始成交量的 50%，但仍受执行层最低保留比例限制。
```

使用原则：

```text
risk_multiplier 不参与初始开仓数量计算。
候选通过方向过滤后，仍按原策略计算的完整数量开仓。
risk_multiplier 只能缩紧已有仓位，不能放大仓位，也不能补回已经减掉的仓位。
不能因为 AI 高置信度就把 risk_multiplier 设成大于 1.0。
实盘接入前必须单独测试 reduceOnly 减仓、止损重挂和记账逻辑。
```

### 10.5 reason / risk_events / news_used / source_counts 含义

| key | 详细说明 |
|---|---|
| `reason` | 给人看的短解释。重点写为什么是这个 bias，以及哪些证据最关键。不能作为程序交易条件直接解析。 |
| `risk_events` | 风险事件列表，例如美联储、CPI、ETF、监管、交易所故障、重大安全事件。用于人工复盘和未来风控提示。 |
| `news_used` | DeepSeek 认为影响判断的新闻标题或主题。用于追溯判断来源，不代表这些新闻一定真实完整，需要结合原始链接复核。 |
| `source_counts` | 本次输入中各类数据源数量，例如 `{"finnhub": 8, "rss": 12, "gdelt": 20}`。数量低说明覆盖不足，应降低信任。 |

注意：

```text
当前这些 key 都不会被 bian_new.py 读取。
它们只是观察期记录和未来接入时的协议草案。
时间字段只保留一套口径：和 `bian_new.py` 的 `EXCHANGE_TZ` 一致，使用北京时间/UTC+8 ISO 时间。
未来实盘读取时必须按 timezone-aware datetime 解析，不能去掉 `+08:00` 后用字符串或 naive datetime 比较。
```

### 10.6 fail-open 和方向过滤规则

#### fail-open 规则

fail-open 的意思是：AI 不拦截，不改变 `bian_new.py` 原策略。它不是“不开仓”，而是“AI 放行，是否开仓仍由原策略决定”。

```text
数据异常、过期、缺字段、解析失败:
  allow_long = true
  allow_short = true

confidence < 0.55:
  allow_long = true
  allow_short = true

futures_bias = neutral 或 mixed:
  allow_long = true
  allow_short = true
```

#### 高置信度方向过滤规则

下面两条不是 fail-open，而是未来可能接入的方向过滤。

它们不会让 AI 主动开仓，只会在 `bian_new.py` 原策略已经出现开仓候选后，决定是否允许这个方向继续。

```text
futures_bias = long 且 confidence >= 0.65
且 explicit_long_signal = true
且模型明确返回 allow_short = false:
  allow_short = false

futures_bias = short 且 confidence >= 0.65
且 explicit_short_signal = true
且模型明确返回 allow_long = false:
  allow_long = false

其他情况:
  allow_long = true
  allow_short = true
```

未来接入后的含义：

```text
如果原策略没有开仓信号:
  AI bias 不能自己开仓。

如果原策略给出多单候选:
  allow_long = true  -> 允许继续走原策略下单流程。
  allow_long = false -> 跳过这次多单候选。

如果原策略给出空单候选:
  allow_short = true  -> 允许继续走原策略下单流程。
  allow_short = false -> 跳过这次空单候选。

当前阶段:
  bian_new.py 还没有读取这些字段，所以不会影响实盘。
```

## 11. 当前测试结果

### 11.1 数据源测试

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

### 11.2 TradingAgents 完整流程

已成功生成：

```text
outputs/ETH-USD_2026-07-08_*
outputs/ETH-USD_2026-07-09_*
outputs/ETH-USD_2026-07-10_*
outputs/ETH-USD_2026-07-11_*
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

截至 2026-07-11 05:58 的结果：

```text
完整任务：10
成功：10
Underweight：9
Overweight：1
LLM timeout：0
```

### 11.3 ai_market_bias.py

已通过：

```text
--fetch-only
--dry-run
正常写 latest_bias_ETHUSDT.json
JSON schema 校验
fail-open 规则校验
旧 schema 兼容校验
观察 CSV schema 自动迁移
```

基于 2026-07-11 TradingAgents Underweight 报告的新协议 dry-run 输出：

```json
{
  "portfolio_stance": "underweight",
  "futures_bias": "neutral",
  "time_horizon_hours": 9,
  "explicit_long_signal": false,
  "explicit_short_signal": false,
  "bias": "neutral",
  "confidence": 0.55,
  "allow_long": true,
  "allow_short": true,
  "risk_multiplier": 0.7,
  "size_multiplier": 0.7
}
```

这次 TradingAgents 的投资组合观点是 Underweight，但没有明确的 9 小时期货做空信号，因此期货方向为 neutral，双向放行。

已修复旧协议中的问题：

```text
Underweight / SELL 不再自动映射成 futures short。
bearish + confidence >= 0.65 不再自动覆盖 allow_long=false。
只有 explicit 信号、置信度和明确禁止反向开仓三项同时满足时，才可能限制方向。
```

测试命令：

```bash
python -m unittest -v test_ai_market_bias.py test_ai_research_failure_classifier.py
```

当前共 12 个测试通过。

### 11.4 三天观察阶段结果

截至 2026-07-11 05:58，北京时间共记录 10 轮完整任务：

```text
TradingAgents 成功：10 / 10
任务退出码 0：10 / 10
平均耗时：约 460 秒
最短耗时：411 秒
最长耗时：515 秒
Binance 摘要错误：0
```

每天均成功生成 ETH-USD summary、state 和 decision。

旧协议 bias 与 Binance ETHUSDT 5 分钟 K 线对齐后的结果：

| 预测窗口 | 方向样本 | 正确率 | 平均方向收益 |
|---|---:|---:|---:|
| 1 小时 | 8 | 25.0% | -0.110% |
| 4 小时 | 8 | 50.0% | -0.127% |
| 9 小时 | 7 | 42.9% | -0.423% |

结论：

```text
运行稳定性已验证。
旧 bias 方向没有表现出可直接用于开多/开空的优势。
不能把 TradingAgents 的 Underweight 直接理解为期货 short。
当前仍不接入 bian_new.py，不影响实盘交易行为。
```

## 12. 运行产物目录

这些目录是运行产物，不属于方案源码。

### .ai_research/

`ai_market_bias.py` 的运行目录。

```text
.ai_research/latest_bias_ETHUSDT.json
.ai_research/cache/
.ai_research/logs/
.ai_research/logs/ai_bias_YYYY-MM.csv
.ai_research/logs/tradingagents_stability_YYYY-MM.csv
.ai_research/logs/tradingagents_failures_YYYY-MM.csv
.ai_research/logs/runs/
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

## 13. 当前调度

当前只跑观察任务。

按北京时间/UTC+8 每天三次：

```text
05:50 启动串行任务：先生成 TradingAgents 报告，完成后生成 latest bias

13:50 启动串行任务：先生成 TradingAgents 报告，完成后生成 latest bias

20:50 启动串行任务：先生成 TradingAgents 报告，完成后生成 latest bias
```

不要再拆成两条 cron。`ai_market_bias.py --run-tradingagents` 会先同步运行 TradingAgents；只有 TradingAgents 正常结束并写出报告后，才会继续生成 bias。bias 成功写出后，再运行 `ai_bias_observer.py` 追加观察日志。

`--hours 9` 同时控制新闻回看窗口和 bias 有效期。bias 的 `generated_at` 以实际写入时间为准，`expires_at = generated_at + 9 小时`。

如果 TradingAgents 大约 10 分钟完成，目标覆盖窗口是：

```text
06:00 -> 15:00
14:00 -> 23:00
21:00 -> 次日 06:00
```

如果 TradingAgents 跑得更久，bias 会顺延生成，不会提前读取旧报告或未写完的报告。

```cron
SHELL=/bin/bash
TZ=Asia/Shanghai
50 5,13,20 * * * /home/ubuntu/binance/tradingAgents/.ai_research/run_ai_research_observation.sh
```

运行脚本内部负责：

```text
flock 防止任务重叠。
串行运行 TradingAgents -> bias -> observer。
每轮保存独立日志。
写 TradingAgents 稳定性 CSV。
失败时分类并写 failures CSV。
```

本轮三天观察窗口：

```text
开始：2026-07-08 14:10 CST
截止：2026-07-11 14:10 CST
截止后脚本 no-op，不再生成新观察。
```

注意：

```text
这条 cron 按北京时间/UTC+8 口径运行。
ai_market_bias.py 默认要求有最新 TradingAgents summary。
如果 summary 缺失或过期，应失败，而不是偷偷变成单模型分析。
如果 TradingAgents 执行失败，整条串行任务失败，不会生成新的 latest bias。
如果 latest bias 没有生成，`ai_bias_observer.py` 不会执行。
```

## 14. 当前状态与下一步

### A. 观察期日志运行

状态：三天观察窗口进行中；截至 2026-07-11 05:58 已有 10 条记录，最后一轮计划在 13:50 运行。

目标：不改变交易行为，连续记录 AI bias 和市场状态，积累可复盘样本。

要做：

```text
1. 每次 latest_bias_ETHUSDT.json 生成后运行 ai_bias_observer.py。
2. 追加写 .ai_research/logs/ai_bias_YYYY-MM.csv。
3. 连续记录 3 天。
4. 对比 bias、confidence、价格变化和 TradingAgents 报告。
5. 统计明确的 futures long/short 信号是否真的有过滤价值。
```

每条记录至少包含：

```text
observed_at
symbol
bias_generated_at
bias_expires_at
portfolio_stance
futures_bias
time_horizon_hours
explicit_long_signal
explicit_short_signal
bias
confidence
allow_long
allow_short
risk_multiplier
size_multiplier
reason
risk_events
news_used
source_counts
latest_price
trend_1h
pct_change_1h
trend_4h
pct_change_4h
trend_1d
pct_change_1d
binance_errors
tradingagents_available
tradingagents_age_hours
tradingagents_modified_at
tradingagents_decision
tradingagents_decision_path
tradingagents_summary_path
```

验收：

```text
.ai_research/logs/ai_bias_YYYY-MM.csv 每天稳定追加。
脚本异常不影响 bian_new.py。
不存在任何下单、撤单、改止损逻辑。
```

当前验收状态：

```text
CSV 每轮稳定追加：通过。
必填字段完整：通过。
Binance 摘要错误为 0：通过。
不影响 bian_new.py：通过。
三天时间窗口：将在 2026-07-11 14:10 正式结束。
```

### B. TradingAgents 稳定性观察

目标：确认低频报告能稳定落盘。

状态：截至 2026-07-11 05:58，10 / 10 轮成功，未出现 LLM timeout。

要做：

```text
1. 连续跑 3 天。
2. 记录每次是否成功、耗时、final decision。
3. 如果仍有 DeepSeek timeout：
   - max-data-rows 降到 45
   - max-news-items 降到 8
   - max-news-summary-chars 降到 350
4. 如果稳定，再考虑每天 1 次启用 debate-rounds 1 / risk-rounds 1。
```

验收：

```text
连续 3 天至少每天成功生成 1 份 ETH-USD summary。
失败日志能明确区分数据源失败或 LLM timeout。
```

当前验收状态：

```text
每天至少一份 ETH-USD summary：通过。
10 / 10 轮 exit=0：通过。
成功、耗时、final decision 结构化记录：通过。
数据源失败故障注入分类：data_source_failure，通过。
真实低超时链路分类：llm_timeout，通过。
历史无效 key 日志分类：authentication_failure，通过。
```

运行时会为每轮任务保留独立日志：

```text
.ai_research/logs/runs/ai_research_YYYYMMDDTHHMMSS+ZZZZ.log
```

失败时由 `ai_research_failure_classifier.py` 分类，并追加：

```text
.ai_research/logs/tradingagents_failures_YYYY-MM.csv
```

当前分类至少包括：

```text
data_source_failure
llm_timeout
authentication_failure
timeout_unclassified
unclassified_failure
```

故障分类器可用以下命令验证：

```bash
python -m unittest -v test_ai_research_failure_classifier.py
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
   portfolio_stance
   futures_bias
   explicit_long_signal / explicit_short_signal
   兼容 bias
   confidence
   allow_long / allow_short
   risk_multiplier / size_multiplier
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
4. 只有 confidence >= 0.65、对应 explicit 信号为 true，且样本表现稳定，才考虑启用方向过滤。
```

## 15. bian_new.py 实盘接入方案

### 15.1 接入结论

AI 研究层可以接入 bian_new.py，但首次接入不应同时启用所有交易动作。

推荐顺序：

~~~text
第 1 步：读取 + 校验 + 日志，不改变交易。
第 2 步：原策略保持原开仓数量，AI 不缩放初始开仓。
第 3 步：出现明确反向信号或 risk_multiplier 要求缩紧时，对已有仓位执行 reduceOnly 部分减仓。
第 4 步：经过额外样本验证后，才允许硬过滤新开仓候选方向。
第 5 步：AI 主动全平默认长期关闭。
~~~

任何阶段都必须保持：

~~~text
AI 不能凭空创建开仓候选。
AI 不能把 long 候选反转成 short，或把 short 反转成 long。
AI 不能增加仓位。
AI 不能放宽止损。
AI 不能修改杠杆。
AI 文件异常时必须 fail-open。
~~~

当前 bian_new.py 仍明确调用：

~~~python
exchange.enable_demo_trading(True)
~~~

因此当前是 Binance Demo 环境。AI 接入改动和切换真实资金环境必须分成两次独立发布，不能在同一次修改中同时完成。

### 15.2 建议配置开关

新增环境变量，默认全部保持保守状态：

~~~text
AI_RESEARCH_MODE=off
AI_BIAS_FILE=/home/ubuntu/binance/tradingAgents/.ai_research/latest_bias_ETHUSDT.json
AI_FILTER_MIN_CONFIDENCE=0.75
AI_REDUCE_MIN_CONFIDENCE=0.75
AI_REVERSE_SIGNAL_RETAINED_POSITION_RATIO=0.75
AI_MIN_RETAINED_POSITION_RATIO=0.75
AI_MAX_POSITION_REDUCTION=0.25
AI_ALLOW_POSITION_REDUCTION=0
AI_ALLOW_FULL_CLOSE=0
AI_ALLOW_REVERSE=0
AI_BIAS_FUTURE_TOLERANCE_SECONDS=300
~~~

AI_RESEARCH_MODE：

| mode | 行为 |
|---|---|
| off | 完全不读取 AI 文件。 |
| log | 读取并写日志，但不改变候选、仓位或订单。 |
| reduce | 在 log 基础上，允许按目标保留比例缩紧已有仓位，不改变初始开仓量。 |
| filter | 在 log 基础上，允许过滤满足全部严格条件的新开仓候选。 |
| manage | 同时允许经过验证的 reduce、pending 撤单和保护动作；AI 全平仍需独立开关。 |

真实资金和 Demo 环境建议改为独立环境变量，例如：

~~~text
BINANCE_DEMO_TRADING=1
~~~

只有人工确认后才能设为 0。AI 代码不得修改该值。

### 15.3 AI bias 读取与校验

新增统一入口：

~~~python
load_ai_research_guard(now_dt) -> dict
~~~

建议返回：

~~~json
{
  "valid": true,
  "fail_open": false,
  "symbol": "ETHUSDT",
  "generated_at": "...",
  "expires_at": "...",
  "portfolio_stance": "underweight",
  "futures_bias": "neutral",
  "time_horizon_hours": 9,
  "explicit_long_signal": false,
  "explicit_short_signal": false,
  "confidence": 0.55,
  "allow_long": true,
  "allow_short": true,
  "risk_multiplier": 0.8,
  "reason": "...",
  "error": ""
}
~~~

必须校验：

~~~text
文件存在且是合法 JSON。
symbol 必须等于 ETHUSDT。
generated_at / expires_at 必须带时区。
generated_at 不能超过当前时间 5 分钟以上。
当前时间必须早于 expires_at。
futures_bias 只能是 long / short / neutral / mixed。
portfolio_stance 只能是 overweight / neutral / underweight。
confidence 必须在 0 到 1。
risk_multiplier 必须在 0 到 1。
explicit 和 allow 字段必须是 boolean。
~~~

任何校验失败：

~~~text
valid = false
fail_open = true
allow_long = true
allow_short = true
risk_multiplier = 1.0
不取消 pending
不减仓
不平仓
~~~

执行层只使用 JSON v2 字段。兼容字段 bias 和 size_multiplier 只记录，不参与新执行逻辑。

### 15.4 开仓候选接入点

接入位置在 _run_strategy_impl()：

~~~text
build_entry_candidates()
        ↓
选择 candidate = top_candidates[0]
        ↓
原有价格、支撑阻力、目标利润校验
        ↓
新增 evaluate_ai_entry_candidate(candidate, ai_guard)
        ↓
place_pending_entry_order()
~~~

AI 只能评估已经由原策略产生的 candidate。

建议返回：

~~~json
{
  "allowed": true,
  "action": "allow",
  "reason": "AI did not block candidate",
  "bias_generated_at": "..."
}
~~~

过滤 long 候选必须同时满足：

~~~text
AI_RESEARCH_MODE 至少为 filter。
AI guard 有效且未过期。
futures_bias = short。
explicit_short_signal = true。
confidence >= AI_FILTER_MIN_CONFIDENCE，初始建议 0.75。
allow_long = false。
~~~

过滤 short 候选必须同时满足：

~~~text
AI_RESEARCH_MODE 至少为 filter。
AI guard 有效且未过期。
futures_bias = long。
explicit_long_signal = true。
confidence >= AI_FILTER_MIN_CONFIDENCE。
allow_short = false。
~~~

其他所有情况都放行。

portfolio_stance 不参与候选方向过滤。Underweight 不能单独拦截 long，Overweight 不能单独拦截 short。

### 15.5 新开仓数量保持不变

当前仓位在 calculate_candidate_amount() 中根据：

~~~text
account_equity * MARGIN_RATE * LEVERAGE / entry_price
~~~

计算。

~~~text
AI 不修改 calculate_candidate_amount() 的结果。
AI 不修改 MARGIN_RATE 或 LEVERAGE。
AI 不因为同方向信号增加仓位。
risk_multiplier 不应用于初始开仓数量。
如果候选未被硬过滤，place_pending_entry_order() 使用原策略计算的完整 amount。
~~~

需要把开仓时的 AI snapshot 写入 candidate 或 entry_meta，再保存到 pending 和 trade_state，用于成交后判断 bias 是否发生反向变化。

注意：当前基础配置 MARGIN_RATE=0.6、LEVERAGE=10，本身属于高风险暴露。切换真实资金前必须单独审计基础仓位和杠杆；AI multiplier 不能替代基础风险控制。

### 15.6 pending STOP_LIMIT 管理

接入位置在 manage_pending_entry()。

每次管理未成交 pending 时重新读取最新 AI guard。

只有满足全部硬过滤条件且订单尚未成交时，才能：

~~~python
cancel_pending_entry_order("AI明确反向过滤，撤销未成交pending")
~~~

规则：

~~~text
AI 变成 neutral/mixed：保留 pending。
AI 文件过期或读取失败：保留 pending，fail-open。
只有 portfolio_stance 改变：保留 pending。
只有 risk_multiplier 降低：首次版本保留 pending，不动态改量。
pending 已部分成交：不得按 AI 直接平掉已成交部分，继续走原有部分成交保护流程。
明确反向信号且满足硬过滤：只撤销尚未成交的剩余订单。
~~~

首次真实资金版本建议不开启 pending AI 撤单，先记录如果启用本应撤销的次数和结果。

### 15.7 持仓阶段的处理原则

现有持仓管理入口：

~~~text
monitor_position_new()
apply_new_exit_rules()
update_stop_if_tighter()
close_position()
~~~

log / filter 模式：

~~~text
AI 不主动平仓。
AI 不主动部分减仓。
AI 不改变目标位。
AI 不放宽保护止损。
AI 只记录持仓方向与最新 futures_bias 是否一致。
~~~

reduce / manage 模式允许的第一种持仓动作是“缩紧已有仓位”，其次才是收紧止损，不能立即反手：

~~~text
long 持仓遇到明确 short 信号：
  即使 risk_multiplier=1.0，也按反向信号默认目标保留比例执行 reduceOnly 部分减仓。
  可同时使用原策略 K 线和 ATR 算出的更紧 stop。

short 持仓遇到明确 long 信号：
  即使 risk_multiplier=1.0，也按反向信号默认目标保留比例执行 reduceOnly 部分减仓。
  可同时使用原策略 K 线和 ATR 算出的更紧 stop。

futures_bias 没有反向，但 risk_multiplier 明确低于当前持仓比例：
  只缩紧已有仓位，不改变方向。
~~~

必须调用现有 update_stop_if_tighter()，确保：

~~~text
long 的新 stop 只能更高。
short 的新 stop 只能更低。
AI 不能直接提供任意 stop 价格。
止损更新失败继续走现有保护性平仓和失败计数。
~~~

### 15.8 部分减仓方案

当前代码没有通用的主动部分减仓函数，close_position() 是全量市价平仓。

需要新增：

~~~python
reduce_position_fraction(fraction, reason, ai_guard)
~~~

安全流程：

~~~text
1. 重新调用 get_position_risk() 获取交易所真实仓位。
2. 获取实际 side 和实际 position amount，不能只信本地 trade_state。
3. fraction 限制在 0 到 AI_MAX_POSITION_REDUCTION。
4. reduce amount 按交易所精度处理并检查最小数量。
5. 使用持仓反方向 MARKET 订单。
6. params 必须包含 reduceOnly=true。
7. hedge 模式下必须沿用交易所真实仓位的 positionSide。
8. 不允许 closePosition 和 reduceOnly 混用。
9. 成交后重新读取真实剩余仓位。
10. 撤销旧保护止损，并按剩余数量重新挂 STOP_MARKET。
11. 更新 trade_state.amount、手续费、累计已减仓数量和通知日志。
~~~

示例：

~~~text
long 持仓减仓：
  side = sell
  reduceOnly = true

short 持仓减仓：
  side = buy
  reduceOnly = true
~~~

必须在实际成交时保存：

~~~text
entry_initial_amount = 实际初始成交数量
~~~

对部署前已经存在的仓位，首次同步时用交易所实际数量建立 baseline，并明确写日志。

risk_multiplier 在执行层的含义改为：

~~~text
目标保留仓位 = entry_initial_amount * risk_multiplier
它只用于已有仓位，不用于初始开仓。
它是目标比例，不是每轮重复减仓比例。
~~~

计算方式：

~~~python
initial_amount = trade_state["entry_initial_amount"]
actual_amount = exchange_position_amount
current_ratio = actual_amount / initial_amount

risk_target_ratio = min(1.0, ai_guard["risk_multiplier"])
requested_target_ratio = risk_target_ratio

if position_is_opposite_to_explicit_signal:
    requested_target_ratio = min(
        requested_target_ratio,
        AI_REVERSE_SIGNAL_RETAINED_POSITION_RATIO,
    )

retained_ratio_floor = max(
    AI_MIN_RETAINED_POSITION_RATIO,
    1.0 - AI_MAX_POSITION_REDUCTION,
)
target_ratio = max(retained_ratio_floor, requested_target_ratio)
target_amount = initial_amount * target_ratio
reduce_amount = max(0.0, actual_amount - target_amount)
~~~

示例：

~~~text
初始仓位 1.00 ETH，risk_multiplier=0.80：
  目标保留 0.80 ETH，减仓 0.20 ETH。

初始仓位 1.00 ETH，出现明确反向信号，但 risk_multiplier=1.00：
  按 AI_REVERSE_SIGNAL_RETAINED_POSITION_RATIO=0.75，目标保留 0.75 ETH，减仓 0.25 ETH。

下一轮仍然是 0.80：
  当前已是 0.80 ETH，不再重复减仓。

下一轮变成 0.90：
  不自动加回仓位，继续保持 0.80 ETH。

下一轮变成 0.70，但 AI_MIN_RETAINED_POSITION_RATIO=0.75：
  最多缩到 0.75 ETH。
~~~

部分减仓有两类触发：

~~~text
触发 A：持仓方向与明确 futures signal 反向
  long 持仓 + futures_bias=short + explicit_short_signal=true。
  short 持仓 + futures_bias=long + explicit_long_signal=true。
  confidence >= AI_REDUCE_MIN_CONFIDENCE。
  目标比例取 risk_multiplier 与 AI_REVERSE_SIGNAL_RETAINED_POSITION_RATIO 中更小者。

触发 B：AI 明确要求缩紧持仓
  risk_multiplier < current_ratio。
  confidence >= AI_REDUCE_MIN_CONFIDENCE。
  不解析 reason 文本，只读取结构化 risk_multiplier。
~~~

共同条件：

~~~text
AI_RESEARCH_MODE = reduce 或 manage。
AI_ALLOW_POSITION_REDUCTION = 1。
AI guard 有效且未过期。
同一个 generated_at 尚未执行过减仓。
交易所仓位和本地仓位同步成功。
~~~

首次启用 AI_MIN_RETAINED_POSITION_RATIO=0.75，即累计最多减掉初始仓位的 25%，不能因为 AI 直接清仓。

当前交易 CSV 是在完整平仓时计算总余额差。加入部分减仓后必须新增字段：

~~~text
partial_reduce_count
partial_reduce_amount
partial_reduce_realized_pnl
partial_reduce_fee
ai_reduce_reasons
~~~

在这些记账字段完成前，不得启用主动部分减仓。

### 15.9 全量平仓方案

AI_ALLOW_FULL_CLOSE 默认必须为 0。

portfolio_stance、普通 futures_bias 或单次新闻判断都不能调用 close_position()。

未来即使考虑 AI 辅助全平，也必须同时满足：

~~~text
AI_RESEARCH_MODE = manage。
AI_ALLOW_FULL_CLOSE = 1。
明确的反向 explicit futures signal。
confidence >= 0.85。
原策略自身也出现出场确认，或保护止损已经需要收紧到接近市价。
同一个 generated_at 未执行过动作。
交易所仓位查询成功。
~~~

AI 平仓后不能在同一轮反向开仓，仍需遵守现有 post-exit cooldown。

任何情况下：

~~~text
AI 不能撤掉止损后不恢复保护。
AI 不能在仓位查询失败时平仓。
AI 不能因文件缺失或过期而平仓。
AI 不能自动反手。
~~~

### 15.10 动作矩阵

| AI 状态 | 新开仓 | pending | 已有仓位 |
|---|---|---|---|
| 文件缺失、过期、解析失败 | 放行，原仓位 | 保留 | 不操作 |
| neutral / mixed | 放行，开仓量不变 | 保留 | 仅当结构化 `risk_multiplier` 低于当前保留比例时才减仓 |
| 同方向 explicit 信号 | 放行，不加仓 | 保留 | 不加仓，不放宽止损 |
| 反方向但未达到严格阈值 | 放行并记录 | 保留 | 只记录 |
| 反方向且满足硬过滤 | 可过滤候选；未过滤时开仓量不变 | 可撤销未成交 pending | `reduce` / `manage` 模式下按目标保留比例减仓 |
| manage 模式反方向强信号 | 不反手；允许开仓时数量不变 | 先处理 pending | 最多部分减仓或收紧止损 |

### 15.11 状态与审计字段

建议在 trade_state 增加：

~~~text
ai_bias_generated_at
ai_bias_expires_at
ai_portfolio_stance
ai_futures_bias
ai_confidence
ai_explicit_long_signal
ai_explicit_short_signal
ai_allow_long
ai_allow_short
ai_risk_multiplier
ai_entry_action
ai_entry_reason
entry_initial_amount
ai_target_position_ratio
ai_target_position_amount
ai_reduce_amount
ai_last_reduce_generated_at
ai_partial_reduce_count
ai_snapshot_json
~~~

pending 状态至少保存：

~~~text
pending_entry_ai_generated_at
pending_entry_ai_futures_bias
pending_entry_ai_confidence
pending_entry_ai_risk_multiplier
pending_entry_ai_action
~~~

每个候选都写一条结构化日志：

~~~text
candidate side
candidate module / strategy_tf
AI mode
AI valid / fail_open
portfolio_stance
futures_bias
explicit signals
confidence
allow_long / allow_short
raw risk_multiplier
entry initial amount
current position amount / current retained ratio
target position ratio / target position amount
reduce amount
最终 action：allow / reduce / filter / log_only
~~~

### 15.12 幂等与并发要求

~~~text
同一个 generated_at 对同一持仓最多执行一次减仓或保护动作。
使用 generated_at + position side + entry_time 作为动作幂等 key。
读取 JSON 时使用一次完整 read + json.loads，不边读边解析。
接入前必须把 ai_market_bias.py 的 write_json() 改为同目录临时文件写入、flush/fsync 后 os.replace 原子替换，避免读取半个 JSON。
AI 读取失败不得改变现有订单和仓位。
pending 撤单、减仓、止损刷新都必须在交易所确认后更新本地状态。
~~~

### 15.13 分阶段上线

阶段 0：单元测试和静态验证

~~~text
测试所有 schema 缺失、过期、未来时间、错误 symbol 和类型错误。
测试 long / short / neutral / mixed 动作矩阵。
测试 risk_multiplier 不改变初始开仓量、不能放大已有仓位，也不能补回已减仓数量。
测试 AI 不能产生候选或反转 side。
~~~

阶段 1：Demo + log

~~~text
BINANCE_DEMO_TRADING=1
AI_RESEARCH_MODE=log
至少观察 20 个真实候选。
确认交易行为与未接入前一致。
~~~

阶段 2：Demo + reduce

~~~text
AI_RESEARCH_MODE=reduce
AI_ALLOW_POSITION_REDUCTION=1
AI_REVERSE_SIGNAL_RETAINED_POSITION_RATIO=0.75
AI_MIN_RETAINED_POSITION_RATIO=0.75
至少验证 20 次持仓态 AI 更新，覆盖反向信号和 risk_multiplier 下调。
确认初始开仓量没有变化。
确认 reduceOnly 数量精度、最小下单量、剩余仓位、止损重挂和部分减仓记账正确。
确认同一个 generated_at 或相同目标比例不会重复减仓。
确认 risk_multiplier 回升时不会自动加仓补回。
~~~

阶段 3：Demo + filter

~~~text
AI_RESEARCH_MODE=filter
AI_FILTER_MIN_CONFIDENCE=0.75
至少验证 20 个满足或接近过滤条件的候选。
统计被过滤候选后续 1h / 4h / 9h 表现。
~~~

阶段 4：真实资金小额试运行

~~~text
先使用 AI_RESEARCH_MODE=log。
真实资金模式和 AI 行为模式不能同日同时切换。
人工确认基础 MARGIN_RATE 和 LEVERAGE。
确认保护止损、外部平仓同步和 pending 管理全部正常。
再考虑启用 reduce，filter 继续关闭。
初始开仓数量始终沿用原策略，不做 AI 缩放。
~~~

阶段 5：manage 与保护动作

~~~text
在已经验证 reduceOnly 部分减仓、止损重挂和完整记账后，再启用 pending 撤单和保护动作。
只在 Demo 完成故障注入、部分成交和幂等测试后启用。
AI_ALLOW_FULL_CLOSE 继续保持 0。
~~~

### 15.14 验收与回滚

必须通过：

~~~text
AI 文件删除后策略继续正常运行。
AI 文件过期后策略继续正常运行。
AI JSON 损坏后策略继续正常运行。
AI 永远不改变原策略的初始开仓数量。
AI 永远不能增加已有仓位，也不能补回已减仓数量。
AI 永远不能放宽止损。
AI 永远不能主动反手。
过滤发生时没有创建任何入场订单。
pending 被过滤时只撤销未成交部分。
减仓后交易所仓位、本地 amount 和保护止损数量一致。
相同 generated_at、相同目标仓位不会重复减仓。
所有 AI 决策都有 generated_at 和 reason 可追溯。
~~~

紧急回滚：

~~~text
AI_RESEARCH_MODE=off
AI_ALLOW_POSITION_REDUCTION=0
AI_ALLOW_FULL_CLOSE=0
~~~

关闭 AI 后不需要改动原策略候选、止损、目标和平仓逻辑。

## 16. 参考来源

- Binance USD-M Futures common definitions/rate limits: https://developers.binance.com/docs/derivatives/usds-margined-futures/common-definition
- Binance USD-M Futures new order / reduceOnly parameters: https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Order
- FRED API key documentation: https://fred.stlouisfed.org/docs/api/api_key.html
- GDELT overview/free open platform: https://www.gdeltproject.org/
- Finnhub API/pricing: https://finnhub.io/pricing
- SEC EDGAR APIs: https://www.sec.gov/search-filings/edgar-application-programming-interfaces
