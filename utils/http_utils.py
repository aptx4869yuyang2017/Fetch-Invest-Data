#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
from functools import wraps

logger = logging.getLogger(__name__)


def retry_on_http_error(max_retries=3, delay=1):
    """HTTP请求重试装饰器
    :param max_retries: 最大重试次数
    :param delay: 重试间隔时间（秒）
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
                    logger.warning(f'请求失败，正在进行第 {retries} 次重试: {str(e)}')
                    time.sleep(delay)
            return func(*args, **kwargs)
        return wrapper
    return decorator