from .utils import RateLimiter, parse_option_expiry_from_symbol
from futu import *
import pandas as pd
import os
from datetime import datetime, timedelta
import time
from futu import *
from .trade_type import Stock
from .utils import safe_read_csv
# 频率限制参数
MAX_REQUESTS = 20
TIME_WINDOW = 35  # 秒


def remove_repeated_fee(df):
    # 组合策略里 订单 id 都是同一个，导致合计手续费被重复计算了，只统计第一个，其他改为 0
    # 1. 同样，先排序
    df.sort_values(by=['order_id', 'create_time'],
                   inplace=True, ignore_index=True)
    # 2. 对每个 order_id 组生成一个累积计数
    # 只有计数器 > 0 的行需要被修改
    mask = df.groupby('order_id').cumcount() > 0
    # 3. 使用 mask 更新费用
    df.loc[mask, 'fee_amount'] = 0
    return df


def get_cash_flow(output_path, start_date, end_date):
    # 创建交易上下文
    trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.NONE, host='127.0.0.1',
                                  port=11111, security_firm=SecurityFirm.FUTUSECURITIES)

    all_cash_flow = []
    rate_limiter = RateLimiter(MAX_REQUESTS, TIME_WINDOW)
    try:
        # 获取账户列表
        ret, acc_list_df = trd_ctx.get_acc_list()
        if ret != RET_OK or not isinstance(acc_list_df, pd.DataFrame):
            print(f'获取账户列表失败: {acc_list_df}')
            exit(1)

        # 日期范围
        date_list = []
        d = start_date
        while d <= end_date:
            date_list.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)

        for _, acc_row in acc_list_df.iterrows():
            acc_id = acc_row.get('acc_id')
            if acc_row.get('trd_env') == TrdEnv.SIMULATE:
                continue
            if acc_id is None:
                continue
            try:
                acc_id = int(acc_id)
            except Exception:
                print(f"无效的账户ID: {acc_id}")
                continue
            print(f"处理账户: {acc_id}")
            for clearing_date in date_list:
                print(f"查询日期: {clearing_date}")
                rate_limiter.wait_if_needed()
                ret, data = trd_ctx.get_acc_cash_flow(
                    clearing_date=clearing_date,
                    trd_env=TrdEnv.REAL,
                    acc_id=acc_id,
                    cashflow_direction=CashFlowDirection.NONE
                )
                if ret == RET_OK:
                    data['acc_id'] = acc_id
                    all_cash_flow.append(data)
                else:
                    print(f"获取现金流水失败: {data}")

        if not all_cash_flow:
            print("所有账户都未获取到现金流水")
        else:
            final_df = pd.concat(all_cash_flow, ignore_index=True)
            if len(final_df) > 0:
                final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
                print(f"已导出到 {output_path}")
    finally:
        trd_ctx.close()


def extract_other_fees(path):
    df = safe_read_csv(path)

    # 统一字符串
    df["cashflow_remark"] = df["cashflow_remark"].fillna("").str.upper()

    fee_keywords = [
        "FEE", "ADR", "INTEREST", "LOAN", "STAMP", "WITHHOLDING TAX", "TAX",
        "IRO", "REGISTRATION", "DIVIDENDS"
    ]

    mask_fee = df["cashflow_remark"].str.contains(
        "|".join(fee_keywords), na=False)

    fees = df[mask_fee]

    return fees


def get_trade_flow(output_path, start_date, end_date):
    # 创建OpenD连接
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    # 不指定市场，获取所有市场的交易权限
    trade_ctx = OpenSecTradeContext(
        host='127.0.0.1', port=11111, filter_trdmarket=TrdMarket.NONE)

    # 创建请求限制器（30秒内最多10次请求）
    rate_limiter = RateLimiter(max_requests=10, time_window=50)

    # 存储所有账户的所有订单
    all_accounts_orders = []

    # 定义要查询的市场列表
    # markets_to_query = [TrdMarket.US, TrdMarket.HK]
    markets_to_query = [TrdMarket.NONE]

    try:
        # 获取账户列表
        ret, acc_list_df = trade_ctx.get_acc_list()
        if ret != RET_OK or not isinstance(acc_list_df, pd.DataFrame):
            print(f'获取账户列表失败: {acc_list_df}')
            return

        # 遍历所有账户
        for _, acc_row in acc_list_df.iterrows():
            acc_id = acc_row.get('acc_id')
            if acc_row.get("trd_env") == TrdEnv.SIMULATE:
                continue
            if acc_id is None:
                continue

            try:
                acc_id = int(acc_id)
            except (ValueError, TypeError):
                print(f"无效的账户ID: {acc_id}")
                continue

            print(f"\n开始处理账户: {acc_id}")

            # 遍历所有市场
            for market in markets_to_query:

                # 每3个月为一个批次
                current_start = start_date

                while current_start < end_date:
                    # 计算当前批次的结束时间
                    current_end = min(
                        current_start + timedelta(days=90), end_date)

                    print(
                        f"正在获取 {current_start.strftime('%Y-%m-%d')} 到 {current_end.strftime('%Y-%m-%d')} 的订单数据...")

                    # 等待请求限制
                    rate_limiter.wait_if_needed()

                    # 查询历史订单, 明确指定市场
                    ret, data = trade_ctx.history_deal_list_query(
                        acc_id=acc_id,
                        deal_market=market,
                        start=current_start.strftime('%Y-%m-%d %H:%M:%S'),
                        end=current_end.strftime('%Y-%m-%d %H:%M:%S'),
                    )

                    if ret != RET_OK:
                        print(f'获取历史订单失败: {data}')

                    if isinstance(data, pd.DataFrame) and not data.empty:
                        data['acc_id'] = acc_id  # 新增：为每个订单加上acc_id
                        all_accounts_orders.append(data)
                        print(f"成功获取 {len(data)} 条订单记录")
                    elif data is not None:
                        # 如果不是DataFrame但有内容，尝试转为DataFrame并追加acc_id
                        try:
                            data_df = pd.DataFrame(data)
                            if not data_df.empty:
                                data_df['acc_id'] = acc_id
                                all_accounts_orders.append(data_df)
                                print(
                                    f"成功获取 {len(data_df)} 条订单记录 (非DataFrame原始类型)")
                        except Exception as e:
                            print(f"数据无法转为DataFrame: {e}")

                    # 更新下一批次的开始时间
                    current_start = current_end

        if not all_accounts_orders:
            print("所有账户和市场都未找到任何订单记录")
            return

        # 合并所有账户和市场的数据到一个DataFrame
        final_df = pd.concat(all_accounts_orders, ignore_index=True)

        # 按时间排序
        if 'create_time' in final_df.columns:
            final_df = final_df.sort_values(
                by='create_time', ascending=False, kind='stable')

        # ====== 新增：批量获取订单费用 ======
        if 'order_id' in final_df.columns and 'acc_id' in final_df.columns:
            fee_list = []
            batch_size = 400
            # 按账户分组批量查费用
            for acc_id_val, group in final_df.groupby('acc_id'):
                # 只处理int或str类型的acc_id
                if not isinstance(acc_id_val, (int, str)):
                    print(f'不支持的acc_id类型: {type(acc_id_val)}, 跳过该分组')
                    continue
                try:
                    acc_id_int = int(str(acc_id_val))
                except Exception:
                    print(f'无法转换acc_id: {acc_id_val}，跳过该分组')
                    continue
                order_ids = group['order_id'].tolist()
                for i in range(0, len(order_ids), batch_size):
                    batch_ids = order_ids[i:i+batch_size]
                    ret, fee_df = trade_ctx.order_fee_query(
                        order_id_list=batch_ids, acc_id=acc_id_int, trd_env=TrdEnv.REAL)
                    if ret == RET_OK and isinstance(fee_df, pd.DataFrame):
                        fee_list.append(fee_df[['order_id', 'fee_amount']])
                    else:
                        print(f'acc_id={acc_id_int} 获取订单费用失败:', fee_df)
            if fee_list:
                all_fee_df = pd.concat(fee_list, ignore_index=True)
            else:
                all_fee_df = pd.DataFrame(columns=['order_id', 'fee_amount'])
            # 合并费用到订单表
            final_df = final_df.merge(all_fee_df, on='order_id', how='left')
        else:
            final_df['fee_amount'] = 0
        # ====== 新增结束 ======

        # 打印最终结果的汇总信息
        print(final_df)
        final_df = remove_repeated_fee(final_df)

        # 保存结果到统一的CSV文件
        if len(final_df) > 0:
            final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"\n所有账户数据已合并保存到 {output_path}")

    finally:
        # 关闭连接
        quote_ctx.close()
        trade_ctx.close()


def format_trade(data_path, cash_path=None, check_expiry=True, check_date=None):
    data = (
        safe_read_csv(data_path)
        .assign(updated_time=lambda df: pd.to_datetime(df["create_time"], errors="coerce"))
        .sort_values(by="updated_time", ascending=True)
    )
    data = data[data["qty"] > 0]

    pool = {}

    market2currency = {
        "HK": "HKD",
        "US": "USD",
    }

    contract_multiplier = {
        "HK": 500,  # 港股期权：500股/张
        "US": 100,   # 美股期权：100股/张
    }

    for _, row in data.iterrows():
        symbol = row["code"]
        expiry_date, is_option = parse_option_expiry_from_symbol(symbol)
        shares = 1 if not is_option else contract_multiplier[row["deal_market"]]
        if symbol not in pool:
            pool[symbol] = Stock(symbol, market2currency[row["deal_market"]])
        if "SELL" in row["trd_side"]:
            pool[symbol].sell(row["price"], row["qty"],
                              row["fee_amount"], row["updated_time"], shares)
        else:
            pool[symbol].buy(row["price"], row["qty"],
                             row["fee_amount"], row["updated_time"], shares)

        # 2. 检查并处理过期期权（新增逻辑）
    if check_expiry:
        if check_date is None:
            check_date = datetime.now()

        expired_count = 0
        for symbol, stock_obj in pool.items():
            # 解析期权到期日
            expiry_date, is_option = parse_option_expiry_from_symbol(symbol)
            print(expiry_date, check_date)

            if is_option and expiry_date and stock_obj.qty != 0:
                # 如果期权已过期
                if check_date >= expiry_date:
                    # 使用到期日当天时间作为记录时间
                    stock_obj.expire_option(expiry_date, expiry_date)
                    expired_count += 1

        if expired_count > 0:
            print(f"已处理 {expired_count} 个过期期权")

    if cash_path is not None:
        fees = extract_other_fees(cash_path)

        fees = fees.assign(
            ts=lambda df: pd.to_datetime(df["clearing_date"], errors="coerce")
        ).sort_values("ts")

        for _, r in fees.iterrows():
            currency = r.get("currency", "").upper()
            if not currency:
                continue

            # —— 为每个币种创建“费用账户” —— #
            fee_symbol = f"FEE-{currency}"

            if fee_symbol not in pool:
                pool[fee_symbol] = Stock(fee_symbol, currency)

            amount = float(r["cashflow_amount"])   # 负数=扣费；正数=退款

            # futu的扣费是负数
            pool[fee_symbol].add_fee(-amount, r["ts"])

    return pool
