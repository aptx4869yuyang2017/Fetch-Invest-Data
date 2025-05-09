#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import tushare as ts
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
from utils.http_utils import retry_on_http_error
from .stock_a_price_provider import StockDataProvider
import os
from utils.cache_utils import FileCache


class StockPriceProviderTushare(StockDataProvider):
    """Tushare数据提供者实现"""

    def __init__(self, token: str):
        """初始化 Tushare 数据提供者
        :param token: Tushare API token
        """
        self.logger = logging.getLogger(__name__)
        ts.set_token(token)
        self.pro = ts.pro_api()

        # 初始化文件缓存
        cache_dir = os.path.join(os.path.dirname(
            os.path.dirname(__file__)), 'cache', 'tushare')
        self.cache = FileCache(cache_dir)

    def _format_symbol(self, symbol: str) -> str:
        """格式化股票代码
        :param symbol: 原始股票代码
        :return: 格式化后的股票代码，符合 Tushare 接口要求
        """
        symbol = symbol.replace('sh', '').replace('sz', '').strip()
        # Tushare 要求股票代码带市场后缀
        if symbol.startswith('6'):
            return f"{symbol}.SH"
        return f"{symbol}.SZ"

    @retry_on_http_error(max_retries=3, delay=1)
    def _fetch_stock_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取股票数据的内部方法，带有重试机制
        :param symbol: 股票代码
        :param start_date: 开始日期
        :param end_date: 结束日期
        :return: 股票数据DataFrame
        """
        df = self.pro.daily(
            ts_code=symbol, start_date=start_date, end_date=end_date)
        return df

    def get_price_data(self, symbol: str, start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> Dict[str, Any]:
        """获取A股股票的价格数据
        :param symbol: 股票代码
        :param start_date: 开始日期，格式为 'yyyy-mm-dd'
        :param end_date: 结束日期，格式为 'yyyy-mm-dd'
        :return: 股票价格数据DataFrame
        """
        try:
            # 格式化股票代码
            formatted_symbol = self._format_symbol(symbol)

            # 设置默认日期范围
            if not start_date:
                start_date_obj = datetime.now() - timedelta(days=30)
                start_date = start_date_obj.strftime('%Y%m%d')
            else:
                start_date = start_date.replace('-', '')

            if not end_date:
                end_date_obj = datetime.now()
                end_date = end_date_obj.strftime('%Y%m%d')
            else:
                end_date = end_date.replace('-', '')

            # 获取数据
            df = self._fetch_stock_data(formatted_symbol, start_date, end_date)

            # 重命名列以保持与其他提供者一致
            column_mapping = {
                'trade_date': 'date',
                'ts_code': 'symbol',
                'open': 'open',
                'close': 'close',
                'high': 'high',
                'low': 'low',
                'vol': 'volume',
                'amount': 'amount',
                'pct_chg': 'change_percent',
                'turnover_rate': 'turnover_rate'
            }

            df.rename(columns=column_mapping, inplace=True)
            df['adjust_type'] = 'none'  # Tushare 的基础数据是不复权数据

            # 格式化日期列
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

            self.logger.info(f'成功获取股票 {symbol} 的价格数据')
            return df

        except Exception as e:
            self.logger.error(f'获取股票 {symbol} 价格数据时发生错误: {str(e)}')
            raise

    @retry_on_http_error(max_retries=3, delay=1)
    def get_company_info(self, symbol: str) -> Dict[str, Any]:
        """获取A股公司基本信息
        :param symbol: 股票代码
        :return: 公司基本信息DataFrame
        """
        try:
            formatted_symbol = self._format_symbol(symbol)

            # 获取公司基本信息
            info = self.pro.stock_company(ts_code=formatted_symbol)

            if info.empty:
                raise ValueError(f"未找到股票 {symbol} 的公司信息")

            # 重命名列以保持与其他提供者一致
            column_mapping = {
                'ts_code': 'stock_code',
                'name': 'stock_name',
                'total_share': 'total_shares',
                'float_share': 'circulating_shares',
                'industry': 'industry',
                'list_date': 'listing_date'
            }

            info.rename(columns=column_mapping, inplace=True)

            # 添加symbol字段
            info['symbol'] = symbol

            # 添加更新时间
            info['update_date'] = datetime.now().strftime('%Y-%m-%d')

            self.logger.info(f'成功获取股票 {symbol} 的公司信息')
            return info.iloc[0].to_dict()

        except Exception as e:
            self.logger.error(f'获取股票 {symbol} 公司信息时发生错误: {str(e)}')
            raise
