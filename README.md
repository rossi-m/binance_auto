# Binance_auto

这是一个 Binance ETH 合约自动交易脚本仓库。

## 使用前准备

1. 复制 `.env.local.example` 为 `.env.local`
2. 在 `.env.local` 中填写你自己的 Binance API 和邮箱配置
3. 安装脚本依赖后运行 `bian_auto.py`

## 说明

- 仓库中的 `bian_auto.py` 已改为从环境变量读取敏感配置
- `.env.local` 已被 `.gitignore` 忽略，不会被提交到仓库
