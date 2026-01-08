from datetime import datetime
import pandas as pd

class Stock:
    def __init__(self, symbol, currency) -> None:
        self.qty = 0                  # 做空 < 0, 做多 > 0
        self.cost = 0.0               # cost = qty * avg_price（允许为负）
        self.symbol = symbol
        self.currency = currency

        self.bonus = 0.0              # 累计已实现收益
        self.bonus_by_year = {}       # {year: realized_pnl}

    @property
    def average_price(self):
        if self.qty == 0:
            return 0.0
        return self.cost / self.qty

    def _add_bonus(self, realized, updated_at):
        """把已实现收益同时记录到总额与年度"""
        if realized == 0:
            return

        self.bonus += realized



        # ---- 情况 1：已经是 datetime / Timestamp ----
        if isinstance(updated_at, (datetime, pd.Timestamp)):
            dt = updated_at

        # ---- 情况 2：字符串（可能有/没有毫秒）----
        else:
            try:
                dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S")

        year = dt.year
        self.bonus_by_year[year] = self.bonus_by_year.get(year, 0.0) + realized

    def buy(self, price, qty, free, updated_at):
        true_price = price + free / qty

        # ---- 先平空 ----
        if self.qty < 0:
            cover = min(qty, abs(self.qty))

            realized = cover * (self.average_price - true_price)
            self._add_bonus(realized, updated_at)

            self.cost += self.average_price * cover
            self.qty += cover
            qty -= cover

        # ---- 剩余是开多 ----
        if qty > 0:
            self.cost += true_price * qty
            self.qty += qty

    def sell(self, price, qty, free, updated_at):
        true_price = price - free / qty

        # ---- 先平多 ----
        if self.qty > 0:
            close = min(qty, self.qty)

            realized = close * (true_price - self.average_price)
            self._add_bonus(realized, updated_at)

            self.cost -= self.average_price * close
            self.qty -= close
            qty -= close

        # ---- 剩余是开空 ----
        if qty > 0:
            self.cost -= true_price * qty
            self.qty -= qty

    def add_fee(self, fee, updated_at):
        if fee <= 0:
            return

        # 费用视作负收益
        self._add_bonus(-fee, updated_at)


