#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
from functools import wraps

logger = logging.getLogger(__name__)


def retry_on_http_error(max_retries=3, delay=1):
    """HTTP请求重试装饰器
    :param max_retries: 最大重试次数
    :param delay: 初始重试间隔时间（秒）

    延迟策略：
    - 前三次重试：使用固定的参数时间
    - 第4-5次重试：使用设定参数时间的2倍
    - 第6-8次重试：使用设定参数时间的3倍
    - 8次以上重试：使用设定参数时间的5倍
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        raise
                    # 只有当重试次数超过5次时才显示警告日志
                    if retries > 5:
                        logger.warning(f'请求失败，正在进行第 {retries} 次重试: {str(e)}')

                    # 根据重试次数确定延迟倍数
                    if retries <= 3:
                        current_delay = delay
                    elif 4 <= retries <= 5:
                        current_delay = delay * 2
                    elif 6 <= retries <= 8:
                        current_delay = delay * 3
                    else:
                        current_delay = delay * 5

                    time.sleep(current_delay)
            return func(*args, **kwargs)
        return wrapper
    return decorator
