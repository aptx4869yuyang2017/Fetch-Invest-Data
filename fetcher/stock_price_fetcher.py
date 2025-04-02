#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
from .stock_price_provider import StockDataProvider
from .stock_price_provider_akshare import StockPriceProviderAkshare
from .stock_price_provider_tushare import StockPriceProviderTushare
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# 可以添加其他数据提供者实现，例如：
# class TushareProvider(StockDataProvider):
#     """Tushare数据提供者实现"""
#     ...


# 加载环境变量
load_dotenv()


class StockPriceFetcher:
    """股票数据获取器，用于批量获取股票数据"""

    def __init__(self, provider: Optional[Union[StockDataProvider, str]] = None):
        """初始化股票数据获取器
        :param provider: 数据提供者，可以是 StockDataProvider 实例或提供者名称字符串，默认使用 'akshare'
        """
        self.logger = logging.getLogger(__name__)
        self.available_providers = {
            'akshare': StockPriceProviderAkshare
        }

        self.set_provider(provider or 'akshare')

    def set_provider(self, provider: Union[StockDataProvider, str]):
        """设置数据提供者
        :param provider: 数据提供者实例或提供者名称字符串
        :raises ValueError: 当提供的名称不存在时抛出异常
        """
        if isinstance(provider, str):
            provider_name = provider.lower()
            if provider_name not in self.available_providers:
                available_names = ', '.join(self.available_providers.keys())
                raise ValueError(
                    f"未知的数据提供者: '{provider_name}'。可用的提供者: {available_names}")

            provider_class = self.available_providers[provider_name]
            self.provider = provider_class()
            self.logger.info(f'已切换数据提供者为: {provider_name}')
        elif isinstance(provider, StockDataProvider):
            self.provider = provider
            self.logger.info(f'已切换数据提供者为: {provider.__class__.__name__}')
        else:
            raise ValueError("provider 必须是 StockDataProvider 实例或者字符串名称")

    def fetch_stock_price(self, symbol: str, start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> Dict[str, Any]:
        """获取单个股票的价格数据
        :param symbol: 股票代码
        :param start_date: 开始日期，格式为 'YYYY-MM-DD'
        :param end_date: 结束日期，格式为 'YYYY-MM-DD'
        :return: 股票价格数据
        """
        return self.provider.get_price_data(symbol, start_date, end_date)

    def fetch_company_info(self, symbol: str) -> Dict[str, Any]:
        """获取单个股票的公司信息
        :param symbol: 股票代码
        :return: 公司信息
        """
        return self.provider.get_company_info(symbol)

    def fetch_multiple_stock_price(self, symbols: List[str],
                                   start_date: Optional[str] = None,
                                   end_date: Optional[str] = None,
                                   max_workers: int = 10) -> pd.DataFrame:
        """批量获取多个股票的数据
        :param symbols: 股票代码列表
        :param start_date: 开始日期，格式为 'YYYY-MM-DD'
        :param end_date: 结束日期，格式为 'YYYY-MM-DD'
        :param max_workers: 最大线程数，默认为 10
        :return: 合并后的所有股票数据 DataFrame
        """

        def fetch_single_stock(symbol):
            try:
                return self.fetch_stock_price(symbol, start_date, end_date)
            except Exception as e:
                self.logger.error(f'获取股票 {symbol} 数据失败: {str(e)}')
                return None

        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_symbol = {executor.submit(fetch_single_stock, symbol): symbol
                                for symbol in symbols}

            # 使用tqdm创建进度条
            with tqdm(total=len(symbols), desc="获取股票数据") as pbar:
                # 获取完成的任务结果
                for future in as_completed(future_to_symbol):
                    result = future.result()
                    if result is not None:
                        results.append(result)
                    pbar.update(1)

        # 合并所有股票数据
        if results:
            return pd.concat(results, ignore_index=True)
        return pd.DataFrame()  # 如果没有成功获取任何数据，返回空DataFrame

    def get_all_stock_codes(self) -> List[str]:
        """获取所有股票代码
        :return: 股票代码列表
        """
        return self.provider.get_all_stock_codes()
