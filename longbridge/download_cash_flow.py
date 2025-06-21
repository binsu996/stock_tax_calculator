from datetime import datetime
from longport.openapi import TradeContext, Config
config = Config.from_env()
ctx = TradeContext(config)
resp = ctx.cash_flow(
    start_at = datetime(2022, 1, 1),
    end_at = datetime.today()
)
with open("data/longbridge_cash.csv",'w') as f:
    print("事项,金额,币种,时间", file=f)
    for x in resp:
        print(
            x.transaction_flow_name,
            x.balance,
            x.currency,
            x.business_time,
            sep=",",
            file=f)