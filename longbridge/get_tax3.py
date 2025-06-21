import pandas as pd

df = pd.read_csv('data/longbridge_cash.csv')
df['时间'] = pd.to_datetime(df['时间'])
df['年份'] = df['时间'].dt.strftime('%Y')
df = df.sort_values('时间', kind='stable')

stock_terms_translation = {
    'Deposit Cash': '存入现金',
    'Buy Contract-Stocks': '买入合约股票',
    'Currency Conversion (Credit)': '货币兑换（贷记）',
    'Currency Conversion (Debit)': '货币兑换（借记）',
    'Stock Trade Fee': '股票交易费',
    'Promotion Adjustment Credit': '促销调整贷记',
    'Sell Contract-Stocks': '卖出合约股票',
    'Stock Sell Commission': '股票卖出佣金',
    'Option Purchase Transaction': '期权购买交易',
    'Option Purchase Fee': '期权购买费用',
    'Option Sell Transaction': '期权卖出交易',
    'Option Sell Fee': '期权卖出费用',
    'Debit Interest': '借方利息',
    'ADR Fee': '美国存托凭证费用',
    'Cash Dividend': '现金股息',
    'CO Other FEE': '结算其他费用',
    'Exercise Stock Option (Sell Stock)': '行使股票期权（卖出股票）',
    'Others': '其他',
    'Stock Short Sale': '股票卖空',
    'Short Selling Interest': '卖空利息',
    'Placement': '配售',
    'Redemption': '赎回',
    'Credit Corporate Action Funds': '公司行为资金贷记',
    'Debit Corporate Action Funds': '公司行为资金借记'
}

df['事项'] = df['事项'].replace(stock_terms_translation)
grouped_detail = df.groupby(['年份', '币种', '事项'])['金额'].sum().reset_index()
grouped_detail.to_csv('data/longbridge_cash_summary.csv', index=False, encoding='utf-8-sig')
