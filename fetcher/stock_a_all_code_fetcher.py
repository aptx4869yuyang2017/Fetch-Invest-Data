#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import akshare as ak
from typing import List
import os
from utils.http_utils import retry_on_http_error
from utils.cache_utils import FileCache


class StockAAllCodeFetcher:
    """A股股票代码获取器"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # 初始化文件缓存
        cache_dir = os.path.join(os.path.dirname(
            os.path.dirname(__file__)), 'cache', 'akshare')
        self.cache = FileCache(cache_dir)

    @retry_on_http_error(max_retries=3, delay=1)
    def get_all_stock_codes(self) -> List[str]:
        """获取所有A股股票代码
        :return: 股票代码列表
        """
        # 尝试从缓存获取数据
        cache_key = 'stock_a_all_code'
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            self.logger.debug('使用缓存的股票代码列表')
            return cached_data

        try:
            # 使用 akshare 获取所有A股列表
            stock_info_df = ak.stock_info_a_code_name()
            # 提取股票代码列表
            stock_codes = stock_info_df['code'].tolist()

            # 更新缓存（设置24小时过期）
            self.cache.set(cache_key, stock_codes, ttl=86400)

            self.logger.info(f'成功获取所有A股股票代码，共 {len(stock_codes)} 个')
            return stock_codes

        except Exception as e:
            self.logger.error(f'获取股票代码列表时发生错误: {str(e)}')
            raise
