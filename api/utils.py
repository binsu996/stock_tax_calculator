import time
import io
import contextlib
import streamlit as st

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
            if sleep_time > 1:
                print(f"请求过多，等待 {sleep_time:.1f} 秒...")
                time.sleep(sleep_time)
        self.request_times.append(time.time())


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

