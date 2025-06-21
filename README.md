[中文说明见此](README_zh.md)

# Stock Tax Calculator

To comply with CRS regulations and facilitate tax reporting, this project leverages quantitative APIs to automatically fetch stock trading records from multiple platforms and calculate profits under different matching methods, providing a reliable basis for tax declaration.
Currently, **Futu** and **Longbridge** are supported. Please make sure to enable the relevant platform APIs and prepare the required keys and gateway programs before use.

## Features
- Automatically fetch stock trading and cash flow records
- Support multiple matching methods (e.g., FIFO, weighted average, moving weighted average) for annual profit calculation
- Summarize profits by year, platform, currency, method, etc., to assist with tax reporting

## Supported Platforms
- Futu
- Longbridge

## Main Files
- `get_tax1.py` (Weighted Average Method), `get_tax2.py` (Moving Weighted Average Method): Tax calculation scripts for different matching methods
- `data/`: Stores transaction records, profit details, annual summary CSVs, etc.

## Data Download Process for Each Platform

### Futu
1. **API Preparation:**
   - Install and start the Futu OpenD gateway, ensuring local port 11111 is available.
   - Refer to the [Futu OpenAPI documentation](https://openapi.futunn.com/) to obtain your API key.
2. **Download Trading Records:**
   - Run `futu/download.py` to batch download all account historical orders, generating `data/futu_history_raw.csv`.
3. **Format Conversion:**
   - Run `futu/export.py` to convert the raw data to standard format, generating `data/futu_history.csv`.
4. **Generate Annual Profit Details:**
   - Run `get_tax1.py` and `get_tax2.py` to automatically generate files like `data/futu_method1_profit_YEAR.csv`, `data/futu_method2_profit_YEAR.csv`, etc.

### Longbridge
1. **API Preparation:**
   - Register and enable Longbridge OpenAPI, and obtain your API key.
   - Configure environment variables or fill in the API key in the script.
2. **Download Trading Records:**
   - Run `longbridge/download_trade_flow.py` to download historical orders, generating `data/longbridge_history.csv`.
3. **Download Cash Flow:**
   - Run `longbridge/download_cash_flow.py` to generate `data/longbridge_cash.csv`.
4. **Generate Annual Profit Details:**
   - Run `get_tax1.py` and `get_tax2.py` to automatically generate files like `data/longbridge_method1_profit_YEAR.csv`, `data/longbridge_method2_profit_YEAR.csv`, etc.

## report Script

---

### Description

The `report` script is used to automatically aggregate and display tax profit data by platform, method, currency, and year, making it easy for users to view and prepare for tax reporting.

#### Main Features
- Automatically traverse all files in the `data/` directory matching the `$platform_$method_profit_$year.csv` pattern
- Filter entries where the matching reason is "年度汇总" (annual summary)
- Print annual tax tables by method
- Each table summarizes profits by platform, year, currency, and stock code
- Supports custom filtering and export

#### Usage
1. Make sure the annual summary CSV files are prepared in the `data/` directory as described above
2. Run:
   ```bash
   python report.py
   ```
3. The script will automatically output annual tax tables for each method

#### Input
- Requires files like `futu_method1_profit_2023.csv`, `longbridge_method2_profit_2024.csv`, etc. in the `data/` directory
- Files must contain fields such as "配对原因" (matching reason), "结算币种" (settlement currency), "股票代码" (stock code), "利润" (profit), etc.

#### Output
- The console will output annual profit tables for each method (e.g., method1, method2), grouped by platform, year, currency, and stock code
- You can modify the script as needed to filter by specific platform, currency, year, or export to Excel

---

## Dependencies
- Python 3.7+
- pandas

Install dependencies:
```bash
pip install pandas
```

## Issues
If you have further requirements or questions, feel free to open an issue!
