import pandas as pd
from collections import defaultdict
import numpy as np
import re
from collections import defaultdict
import pandas as pd
import re
from collections import defaultdict


def process_item(holdings,trade):
    option_pattern = re.compile(r'([A-Z]+)(\d{6})([CP])(\d+).US')
    symbol = str(trade['股票代码'])
    side = trade['买卖方向']
    qty = trade['数量'] if side=="OrderSide.Buy" else -trade['数量']
    price = trade['成交金额']
    fee = trade['合计手续费']
    currency = trade['结算币种']
    trade_time = trade['交易时间']
    is_option = bool(option_pattern.match(symbol))
    records=[]
    if is_option:
        if np.isnan(price):
            price=0
        else:
            price*=100
    hold=holdings[symbol]
    cur_qty=hold["quantity"]

    if qty>0:
        if cur_qty<0:
            qty1=min(qty,abs(cur_qty))
            total_buy_cost=hold["total_fee"]+abs(qty1/qty)*fee+qty1*price
            earn=qty1*hold["avg_cost"]-total_buy_cost
            if qty1==abs(cur_qty):
                records.append({
                    "配对原因":"做空了结",
                    "股票代码":symbol,
                    "卖出价格":hold["avg_cost"],
                    "成本价":total_buy_cost/qty1,
                    "数量":qty1,
                    "利润":earn,
                    "时间":trade_time,
                    "结算币种":currency
                })
            delta_qty=qty-abs(cur_qty)
            hold["quantity"]=delta_qty
            if delta_qty>0:
                hold["total_fee"]=delta_qty/qty*fee
                hold["price"]=price
            else:
                cur_qty-=abs(qty)
                hold["avg_cost"]=0 if cur_qty==0 else (abs(qty)*hold["avg_cost"]-total_buy_cost)/abs(cur_qty)
                hold["total_fee"]=0
        else:
            total_buy_cost=hold["avg_cost"]*hold["quantity"]+qty*price
            hold["avg_cost"]=total_buy_cost/(qty+hold["quantity"])
            hold["quantity"]+=qty
            hold["total_fee"]+=fee

    if qty<0:
        if cur_qty>0:
            qty1=min(abs(qty),cur_qty)
            total_buy_cost=hold["total_fee"]+abs(qty1/qty)*fee+cur_qty*hold["avg_cost"]
            earn=abs(qty1)*price-total_buy_cost
            if qty1==cur_qty:
                records.append({
                    "配对原因":"做多了结",
                    "股票代码":symbol,
                    "卖出价格":price,
                    "成本价":total_buy_cost/qty1,
                    "数量":abs(qty1),
                    "利润":earn,
                    "时间":trade_time,
                    "结算币种":currency
                })
            delta_qty=cur_qty-abs(qty)
            hold["quantity"]=delta_qty
            if delta_qty<0:
                hold["total_fee"]=(abs(delta_qty)/abs(qty))*fee
                hold["avg_cost"]=price
            else:
                cur_qty-=abs(qty)
                hold["avg_cost"]=0 if cur_qty==0 else (total_buy_cost-abs(qty)*price)/cur_qty
                hold["total_fee"]=0
        else:
            total_sell_cost=hold["avg_cost"]*abs(hold["quantity"])+abs(qty)*price
            hold["avg_cost"]=total_sell_cost/(abs(qty)+abs(hold["quantity"]))
            hold["quantity"]+=qty
            hold["total_fee"]+=fee

    return records

def summary_year(all_profits,save_path):
    df_result = pd.DataFrame(all_profits)
    
    for currency,sub_df in df_result.groupby("结算币种"):
        total_profits=sub_df["利润"].sum()
        all_profits.append({
            "配对原因":"年度汇总",
            "股票代码":"按年度计算",
            "卖出价格":0,
            "成本价":0,
            "数量":0,
            "利润":total_profits,
            "时间":sub_df.iloc[-1]["时间"],
            "结算币种":currency
        })

        total_profits=sub_df[sub_df["利润"] > 0]["利润"].sum()
        all_profits.append({
            "配对原因":"年度汇总",
            "股票代码":"按单次计算",
            "卖出价格":0,
            "成本价":0,
            "数量":0,
            "利润":total_profits,
            "时间":sub_df.iloc[-1]["时间"],
            "结算币种":currency
        })
    
    df_result = pd.DataFrame(all_profits)
    df_result.to_csv(save_path, index=False, encoding='utf-8-sig')

    
def main():
    df = pd.read_csv('data/longbridge_history.csv')
    # 按照秒级时间排序
    df['交易时间'] = pd.to_datetime(df['交易时间'])
    df['年份'] = df['交易时间'].dt.strftime('%Y')
    df = df.sort_values('交易时间', kind='stable')
    all_profits = []
    cur_year=None
    holdings = defaultdict(lambda: {'quantity': 0.0, 'avg_cost': 0.0,'total_fee':0})
    for _, trade in df.iterrows():
        if cur_year is not None and trade["年份"]!=cur_year:
            summary_year(all_profits,f"data/longbridge_method2_profit_{cur_year}.csv")
            all_profits = []
        cur_year=trade["年份"]
        profit=process_item(holdings,trade)
        all_profits.extend(profit)
    
    summary_year(all_profits,f"data/longbridge_method2_profit_{cur_year}.csv")

if __name__ == '__main__':
    main() 