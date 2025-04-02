#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import akshare as ak
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import os
from utils.http_utils import retry_on_http_error
from utils.cache_utils import FileCache
from .stock_price_provider import StockDataProvider


class StockPriceProviderAkshare(StockDataProvider):
    """Akshare数据提供者实现"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # 初始化文件缓存
        cache_dir = os.path.join(os.path.dirname(
            os.path.dirname(__file__)), 'cache', 'akshare')
        self.cache = FileCache(cache_dir)

    def _format_symbol(self, symbol: str) -> str:
        """格式化股票代码
        :param symbol: 原始股票代码
        :return: 格式化后的股票代码
        """
        # 移除可能的交易所前缀
        symbol = symbol.replace('sh', '').replace('sz', '').strip()
        return symbol

    @retry_on_http_error(max_retries=3, delay=1)
    def _fetch_stock_data(self, symbol: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
        """获取股票数据的内部方法，带有重试机制
        :param symbol: 股票代码
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param adjust: 复权类型
        :return: 股票数据DataFrame
        """
        return ak.stock_zh_a_hist(symbol=symbol,
                                  start_date=start_date,
                                  end_date=end_date,
                                  adjust=adjust)

    def get_price_data(self, symbol: str, start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> pd.DataFrame:
        """获取A股股票的价格数据
        :param symbol: 股票代码
        :param start_date: 开始日期，格式为 'yyyy-mm-dd'
        :param end_date: 结束日期，格式为 'yyyy-mm-dd'
        :return: 包含2种复权类型数据的DataFrame，列名为英文
        """
        try:
            # 格式化股票代码
            formatted_symbol = self._format_symbol(symbol)

            # 设置默认日期范围
            if not start_date:
                start_date_obj = datetime.now() - timedelta(days=30)
                start_date = start_date_obj.strftime('%Y-%m-%d')
            else:
                start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')

            if not end_date:
                end_date_obj = datetime.now()
                end_date = end_date_obj.strftime('%Y-%m-%d')
            else:
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')

            # 列名映射字典
            column_mapping = {
                '日期': 'dt',
                '股票代码': 'symbol',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'change_percent',
                '涨跌额': 'change_amount',
                '换手率': 'turnover_rate'
            }

            # 获取三种复权类型的数据
            start_date_fmt = start_date_obj.strftime('%Y%m%d')
            end_date_fmt = end_date_obj.strftime('%Y%m%d')

            # 前复权数据
            # df_qfq = self._fetch_stock_data(
            #     formatted_symbol, start_date_fmt, end_date_fmt, 'qfq')
            # df_qfq.rename(columns=column_mapping, inplace=True)
            # df_qfq['adjust_type'] = 'qfq'  # 添加复权类型标识

            # 后复权数据
            df_hfq = self._fetch_stock_data(
                formatted_symbol, start_date_fmt, end_date_fmt, 'hfq')
            df_hfq.rename(columns=column_mapping, inplace=True)
            df_hfq['adjust_type'] = 'hfq'  # 添加复权类型标识

            # 不复权数据
            df_none = self._fetch_stock_data(
                formatted_symbol, start_date_fmt, end_date_fmt, '')
            df_none.rename(columns=column_mapping, inplace=True)
            df_none['adjust_type'] = 'none'  # 添加复权类型标识

            # 合并所有数据
            result_df = pd.concat([df_hfq, df_none], ignore_index=True)

            return result_df

        except Exception as e:
            self.logger.error(f'获取股票 {symbol} 价格数据时发生错误: {str(e)}')
            raise

    @retry_on_http_error(max_retries=3, delay=1)
    def get_company_info(self, symbol: str) -> pd.DataFrame:
        """获取A股公司基本信息
        :param symbol: 股票代码
        :return: 公司基本信息，以DataFrame形式返回，列名为英文
        """
        try:
            # 格式化股票代码
            formatted_symbol = self._format_symbol(symbol)

            # 获取公司基本信息
            info = ak.stock_individual_info_em(symbol=formatted_symbol)

            # 中文列名到英文列名的映射
            column_mapping = {
                '股票代码': 'stock_code',
                '股票简称': 'stock_name',
                '总股本': 'total_shares',
                '流通股': 'circulating_shares',
                '总市值': 'total_market_value',
                '流通市值': 'circulating_market_value',
                '行业': 'industry',
                '上市时间': 'listing_date'
            }

            # 将原始信息转换为字典
            info_dict = {}
            if not info.empty:
                for _, row in info.iterrows():
                    key = row[0]
                    value = row[1]
                    # 使用映射转换列名
                    if key in column_mapping:
                        english_key = column_mapping[key]
                    else:
                        # 对于未映射的键，使用原始键名
                        english_key = key
                    info_dict[english_key] = value

            # 添加symbol字段
            info_dict['symbol'] = formatted_symbol

            # 添加数据更新日期字段
            info_dict['update_date'] = datetime.now().strftime('%Y-%m-%d')

            # 创建单行DataFrame
            result_df = pd.DataFrame([info_dict])

            self.logger.info(f'成功获取股票 {formatted_symbol} 的公司信息')
            return result_df

        except Exception as e:
            self.logger.error(f'获取股票 {symbol} 公司信息时发生错误: {str(e)}')
            raise

    @retry_on_http_error(max_retries=3, delay=1)
    def get_all_stock_codes(self) -> List[str]:
        """获取所有A股股票代码
        :return: 股票代码列表
        """
        # 尝试从缓存获取数据
        cache_key = 'all_stock_codes'
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
