from datetime import datetime
from longport.openapi import TradeContext, Config, OrderStatus, OrderSide, Market
import time
import logging
import pathlib

config = Config.from_env()
ctx = TradeContext(config)

resp = ctx.history_orders(
    status = [OrderStatus.Filled],
    start_at = datetime(2022, 1, 1),
    end_at = datetime.today()
)

pathlib.Path("data").mkdir(exist_ok=True)
with open("data/longbridge_history.csv",'w') as f:
    print("股票代码,数量,成交价格,买卖方向,结算币种,合计手续费,交易时间", file=f)
    for x in resp:
        try:
            resp = ctx.order_detail(
                order_id = x.order_id,
            )
            print(resp.symbol,resp.executed_quantity,resp.executed_price,resp.side,resp.currency,resp.charge_detail.total_amount,x.submitted_at,sep=",",file=f)
        except:
            logging.info(x)
        time.sleep(1.5)
