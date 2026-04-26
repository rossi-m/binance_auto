# ETH 自动交易策略监控网站 - 设计文档

## 1. 概述
为 `bian_auto.py` 币安 ETH 合约自动交易策略提供 Web 监控面板，部署在本地 Ubuntu 服务器上。

## 2. 技术栈
- **后端**: Python 3.12 + Flask
- **前端**: HTML5 + CSS3 + 原生 JavaScript（无构建步骤）
- **图表**: Chart.js v4 (CDN)
- **进程管理**: `subprocess.Popen` + `threading`
- **实时日志**: Server-Sent Events (SSE)
- **数据源**: `trades_log_YYYY-MM.csv` + `trade_stats.db`（SQLite 日收益汇总）

## 3. 功能模块

### 3.1 实时日志流
- 通过子进程管道捕获策略的 stdout/stderr
- 写入 `strategy_output.log`
- 通过 `/api/logs` SSE 端点推送到前端
- 日志面板自动滚动，鼠标悬停可暂停

### 3.2 收益统计
- **当天收益**: 从 SQLite 日收益汇总读取今日 `净利润(USDT)` 聚合值
- **本月折线图**: 按日聚合当月净利润，Chart.js 渲染
- **整体收益**: 跨所有历史日收益数据的净利润累计
- **本月 / 本年收益**: 从 SQLite 日收益汇总按月份、年份聚合
- **年/月汇总**: 按年查看全年收益，按选中年份查看各月收益
- **交易笔数**: 总笔数 / 今日笔数

### 3.3 交易记录表格
- 展示当月 CSV 数据，按平仓时间倒序排列
- 列: 建仓时间、趋势方向、入场原因、平仓时间、平仓原因、点数盈亏、手续费、净利润(USDT)、是否盈利、持仓秒数
- 每2小时自动刷新（前端轮询）

### 3.4 策略控制
- **开始按钮**: 直接启动 `bian_auto.py` 子进程（stdout/stderr 被捕获）
- **暂停按钮**: 需要邮件验证
  1. 点击暂停 -> 后端生成6位验证码
  2. 通过 SMTP 发送邮件到配置邮箱
  3. 用户在弹窗中输入验证码提交
  4. 后端验证通过（5分钟有效期）后终止子进程
- **状态指示**: 绿色（运行中）/ 红色（已停止）

### 3.5 响应式布局
- **PC (>=1024px)**: 双列网格。左侧日志(40%)，右侧统计+图表+表格+控制(60%)
- **平板 (768-1023px)**: 垂直堆叠，日志在上
- **手机 (<768px)**: 单列垂直布局

## 4. API 设计

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 首页 |
| GET | `/api/status` | 策略运行状态 JSON |
| GET | `/api/logs` | SSE 日志流 |
| GET | `/api/trades` | 当月交易数据 JSON |
| GET | `/api/stats` | 统计数据 JSON（今日 / 本月 / 本年 / 整体 / 年月汇总 / 曲线） |
| POST | `/api/start` | 启动策略 |
| POST | `/api/pause-request` | 请求暂停验证码 |
| POST | `/api/pause-verify` | 验证并暂停 |

## 5. 安全设计
- 暂停操作受邮件 OTP 保护（6位数字，5分钟有效期）
- 同时只能存在一个有效验证码会话
- 复用 `.env.local` 中的 SMTP 凭证发邮件

## 6. 文件结构
```
binance/
├── bian_auto.py                    # 原策略脚本（不动）
├── trades_log_2026-04.csv          # 交易记录 CSV（按月）
├── trade_stats.db                  # SQLite 日收益汇总
├── .env.local                      # 环境变量（不动）
└── trading_web/                    # Web 监控台目录
    ├── trader_web.py               # Flask 后端
    ├── design.md                   # 本设计文档
    ├── strategy_output.log         # 策略日志输出（运行时生成）
    ├── templates/
    │   └── index.html              # 前端页面
    └── static/
        ├── css/
        │   └── style.css           # 样式
        └── js/
            └── app.js              # 前端逻辑
```

## 7. 进程流程
```
用户点击开始 -> trader_web.py 通过 Popen 启动 bian_auto.py
             -> 读取 stdout/stderr 写入 strategy_output.log
             -> SSE 端点读取日志文件尾部推送给客户端

用户点击暂停 -> POST /api/pause-request -> 生成 OTP -> 发送邮件
             -> 用户输入 OTP -> POST /api/pause-verify
             -> 验证 OTP -> Popen.terminate() -> 进程停止
```

## 8. 数据流
```
trades_log_2026-04.csv  ->  bian_auto.py 平仓时写入
trade_stats.db          ->  bian_auto.py 同步更新日收益
CSV 历史文件            ->  trader_web.py 启动/查询时补齐同步到 SQLite
trade_stats.db          ->  /api/stats 返回今日、本月、本年、整体、年/月汇总和曲线
当月 CSV                ->  /api/trades 返回原始行
前端                    ->  渲染表格、汇总列表和 Chart.js 折线图
```
