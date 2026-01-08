import time
import io
import contextlib
import streamlit as st
from datetime import datetime
import re

class RateLimiter:
    def __init__(self, max_requests, time_window):
        self.max_requests = max_requests
        self.time_window = time_window
        self.request_times = []

    def wait_if_needed(self):
        now = time.time()
        # 移除过期的请求
        self.request_times = [t for t in self.request_times if now - t < self.time_window]
        if len(self.request_times) >= self.max_requests:
            sleep_time = self.time_window - (now - self.request_times[0])
            if sleep_time > 0:
                print(f"请求过多，等待中")
                time.sleep(sleep_time)
        self.request_times.append(time.time())

def parse_option_expiry_from_symbol(symbol):
    """
    从期权代码解析到期日
    格式示例: HK.TCH230530P320000 或 TCH230530P320000
    返回: (到期日datetime, 是否成功解析)
    """

    # 匹配期权格式: 标的代码 + 到期日(yymmdd) + C/P + 行权价
    pattern = r'[A-Z\.]+([0-9]{6})[CP][0-9]+'
    match = re.match(pattern, symbol)

    if not match:
        return None, False

    expiry_str = match.group(1)

    try:
        # 解析日期: 假设00-49为2000-2049, 50-99为1950-1999
        year_str = expiry_str[:2]
        month = int(expiry_str[2:4])
        day = int(expiry_str[4:6])

        year_int = int(year_str)
        if year_int >= 50:
            year = 1900 + year_int
        else:
            year = 2000 + year_int

        expiry_date = datetime(year, month, day)
        return expiry_date, True

    except (ValueError, IndexError):
        return None, False

class StreamlitLogger(io.StringIO):
    def __init__(self, placeholder, height=300):
        super().__init__()
        self.placeholder = placeholder
        self.height = height

    def write(self, s):
        super().write(s)
        # 每次打印以后刷新
        self.placeholder.text_area(
            "执行日志",
            self.getvalue(),
            height=self.height,
            disabled=True,
        )

def run_with_output(fn, *args, **kwargs):
    placeholder = st.empty()
    logger = StreamlitLogger(placeholder)

    with contextlib.redirect_stdout(logger):
        return fn(*args, **kwargs)

