from datetime import datetime
from pickletools import floatnl
from token import COLONEQUAL
from longport.openapi import TradeContext, Config, OrderStatus, OpenApiException, OrderChargeDetail
import pandas as pd
import time
from pathlib import Path
from collections import defaultdict
import re
from .trade_type import Stock
from .utils import parse_option_expiry_from_symbol, safe_read_csv


def get_public_attributes(obj):
    return [name for name in dir(obj)
            if not name.startswith('_')]


def flatten_attributes(cols, row):
    new_cols, new_row = [], []
    for col, x in zip(cols, row):
        if isinstance(x, list):
            continue
        if not isinstance(x, OrderChargeDetail):
            new_cols.append(col)
            new_row.append(x)
        else:
            sub_cols = get_public_attributes(x)
            sub_row = [getattr(x, sub_col) for sub_col in sub_cols]
            sub_cols = ["_".join([col, sub_col]) for sub_col in sub_cols]
            new_cols.extend(sub_cols)
            new_row.extend(sub_row)
    return new_cols, new_row


def get_ctx():
    config = Config.from_env()
    return TradeContext(config)


def load_longport_adr_events(cash_path):
    adr = safe_read_csv(cash_path)

    # 只保留 ADR 费用
    adr = adr[adr["transaction_flow_name"] == "ADR Fee"].copy()

    # 费用为正值（balance 是负数）
    adr["fee"] = adr["balance"].abs()

    # 解析 symbol：优先字段，否则从 description 里抽取
    def parse_symbol(row):
        if isinstance(row["symbol"], str) and row["symbol"]:
            return row["symbol"]
        m = re.search(r"([A-Z0-9]+\.[A-Z]+)\s+ADR", str(row["description"]))
        return m.group(1) if m else None

    adr["symbol"] = adr.apply(parse_symbol, axis=1)

    # 只保留能识别 symbol 的记录
    adr = adr[adr["symbol"].notna()]

    # 标准化时间 & 统一结构
    adr = adr.rename(columns={"business_time": "updated_at"})
    adr = adr[["symbol", "fee", "updated_at"]]
    adr["event_type"] = "adr"

    return adr


def get_cash_flow(cache_file_path, ctx, start_time=datetime(2022, 1, 1), end_time=datetime.today()):
    Path(cache_file_path).parent.mkdir(exist_ok=True, parents=True)
    resp = ctx.cash_flow(
        start_at=start_time,
        end_at=end_time
    )
    with open(cache_file_path, 'w') as f:
        columns = ['balance', 'business_time', 'business_type', 'currency',
                   'description', 'direction', 'symbol', 'transaction_flow_name']
        data = []
        for x in resp:
            row = [getattr(x, col) for col in columns]
            data.append(row)
        df = pd.DataFrame(data, columns=columns)
        df.to_csv(f, index=False, encoding='utf-8-sig')


def get_trade_flow(cache_file_path, ctx, start_time, end_time):
    Path(cache_file_path).parent.mkdir(exist_ok=True, parents=True)
    resp = ctx.history_orders(
        status=[OrderStatus.Filled],
        start_at=start_time,
        end_at=end_time
    )
    with open(cache_file_path, 'w') as f:
        columns = None
        data = []
        for x in resp:
            while True:
                try:
                    detail = ctx.order_detail(
                        order_id=x.order_id,
                    )
                    columns = get_public_attributes(detail)
                    row = [getattr(detail, col) for col in columns]
                    columns, row = flatten_attributes(cols=columns, row=row)
                    data.append(row)
                    break
                except OpenApiException as e:
                    if e.code == 429002:
                        time.sleep(1)
                    else:
                        print(e)
                        break
                except Exception as e:
                    print(e)
                    break

        df = pd.DataFrame(data, columns=columns)
        df.to_csv(f, index=False, encoding='utf-8-sig')


def get_profile(csv_file_path):
    profit = defaultdict(float)

    reader = safe_read_csv(csv_file_path).query("symbol.notnull()")

    for _, row in reader.iterrows():
        symbol = row["symbol"].strip()
        if symbol:  # 忽略 symbol 为空的部分
            balance = float(row["balance"])
            profit[symbol] += balance

    return dict(profit)


def format_longport_trade(data_path, cash_path=None):
    data = safe_read_csv(data_path).sort_values(
        by='updated_at', ascending=True)
    data = data[["charge_detail_currency", "charge_detail_total_amount", "executed_price",
                 "executed_quantity", "symbol", "price", "side", "quantity", "updated_at"]]
    pool = {}

    contract_multiplier = {
        "HKD": 500,  # 港股期权：500股/张
        "USD": 100,   # 美股期权：100股/张
    }

    for _, row in data.iterrows():
        symbol = row["symbol"]
        expiry_date, is_option = parse_option_expiry_from_symbol(symbol)
        shares = 1 if not is_option else contract_multiplier[row["charge_detail_currency"]]

        if symbol not in pool:
            pool[symbol] = Stock(symbol, row["charge_detail_currency"])
        if row["side"] == "OrderSide.Sell":
            pool[symbol].sell(row["price"], row["quantity"],
                              row["charge_detail_total_amount"], row["updated_at"], shares)
        else:
            pool[symbol].buy(row["price"], row["quantity"],
                             row["charge_detail_total_amount"], row["updated_at"], shares)

    if cash_path is not None:
        from api.user_longport import load_longport_adr_events
        adr_data = load_longport_adr_events(cash_path)
        print(adr_data)
        for _, row in adr_data.iterrows():
            symbol = row["symbol"]
            pool[symbol].add_fee(row["fee"], row["updated_at"])
    return pool
