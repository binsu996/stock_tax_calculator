# 股票税务计算器

为了应对CRS监管，合理报税，本项目旨在利用量化API自动获取各个平台的股票交易记录并计算不同方式下的盈利，以方便作为报税依据。
现在已经支持**富途牛牛**和**长桥**，使用前请自行开通相关平台的API并准备好密钥和网关程序。

## 功能简介
- 自动获取股票交易流水、资金流水
- 支持多种配对方式（如FIFO、加权等）计算年度盈利
- 按年度、平台、币种、方式等多维度汇总利润，辅助报税

## 支持平台
- 富途牛牛（Futu）
- 长桥（Longbridge）

## 主要文件说明
- `get_tax1.py`(加权平均法)、`get_tax2.py`(移动加权平均法)：不同配对方式的税务计算脚本
- `data/`：存放各平台流水、利润明细、年度汇总等csv文件

## 各平台数据下载流程

### 富途牛牛（Futu）
1. **API准备**：
   - 安装富途OpenD网关并启动，确保本地11111端口可用。
   - 参考[富途OpenAPI文档](https://openapi.futunn.com/)获取API密钥。
2. **下载交易流水**：
   - 运行 `futu/download.py`，自动批量下载所有账户的历史订单，生成 `data/futu_history_raw.csv`。
3. **格式转换**：
   - 运行 `futu/export.py`，将原始数据转换为标准格式，生成 `data/futu_history.csv`。
4. **生成年度利润明细**：
   - 运行 `get_tax1.py`、`get_tax2.py`，自动生成 `data/futu_method1_profit_年份.csv`、`data/futu_method2_profit_年份.csv` 等文件。

### 长桥（Longbridge）
1. **API准备**：
   - 注册并开通长桥OpenAPI，获取API密钥。
   - 配置环境变量或在脚本中填写API密钥。
2. **下载交易流水**：
   - 运行 `longbridge/download_trade_flow.py`，自动下载历史订单，生成 `data/longbridge_history.csv`。
3. **下载资金流水**：
   - 运行 `longbridge/download_cash_flow.py`，生成 `data/longbridge_cash.csv`。
4. **生成年度利润明细**：
   - 运行 `get_tax1.py`、`get_tax2.py`，自动生成 `data/longbridge_method1_profit_年份.csv`、`data/longbridge_method2_profit_年份.csv` 等文件。

## report脚本说明

`report` 脚本用于自动汇总和展示各平台、各方式、各币种、各年度的税务利润数据，便于用户直观查看和后续报税。

### 主要功能
- 自动遍历 `data/` 目录下所有符合 `$platform_$method_profit_$year.csv` 格式的文件
- 筛选“配对原因为年度汇总”的条目
- 按方式（method）分别打印年度税款表
- 每个表按平台、年份、币种、股票代码分组汇总利润
- 支持自定义筛选、导出等扩展

### 使用方法
1. 确保已按前述流程准备好 `data/` 目录下的年度汇总csv文件
2. 运行：
   ```bash
   python report.py
   ```
3. 程序会自动输出每种方式下的年度税款表

### 输入说明
- 需要 `data/` 目录下有如 `futu_method1_profit_2023.csv`、`longbridge_method2_profit_2024.csv` 等文件
- 文件需包含“配对原因”、“结算币种”、“股票代码”、“利润”等字段

### 输出说明
- 控制台分别输出每种方式（如method1、method2）下，按平台、年份、币种、股票代码分组的年度利润表
- 可根据需要修改脚本，筛选特定平台、币种、年份或导出为Excel

---

安装依赖：
```bash
pip install pandas
```