import pandas as pd
import os

def main():
    # 路径
    raw_path = os.path.join('data', 'futu_history_raw.csv')
    out_path = os.path.join('data', 'futu_history.csv')
    
    # 目标表头
    target_columns = [
        '股票代码', '数量', '成交金额', '买卖方向', '结算币种', '合计手续费', '交易时间'
    ]

    # 读取原始数据
    df = pd.read_csv(raw_path)

    # 生成目标DataFrame
    out_df = pd.DataFrame()
    out_df['股票代码'] = df['code']
    out_df['数量'] = df['dealt_qty']
    out_df['成交价格'] = df['dealt_avg_price']
    out_df['买卖方向'] = df['trd_side'].replace({'BUY': 'OrderSide.Buy', 'SELL': 'OrderSide.Sell'})
    out_df['结算币种'] = df['currency']
    out_df['合计手续费'] = df["合计手续费"]  # futu原始数据无手续费字段
    out_df['交易时间'] = df['create_time'].str[:19]  # 去除毫秒

    # 保存为目标格式
    out_df.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f'已导出到 {out_path}')

if __name__ == '__main__':
    main() 