#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import akshare as ak
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta
# from .base import Stock
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class StockDataProvider(ABC):
    """股票数据提供者抽象基类，定义统一接口"""

    @abstractmethod
    def get_price_data(self, symbol: str, start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> Dict[str, Any]:
        """获取股票价格数据的抽象方法"""
        pass

    @abstractmethod
    def get_company_info(self, symbol: str) -> Dict[str, Any]:
        """获取公司信息的抽象方法"""
        pass


class AkshareProvider(StockDataProvider):
    """Akshare数据提供者实现"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def _format_symbol(self, symbol: str) -> str:
        """格式化股票代码
        :param symbol: 原始股票代码
        :return: 格式化后的股票代码
        """
        # 移除可能的交易所前缀
        symbol = symbol.replace('sh', '').replace('sz', '').strip()
        return symbol

    def get_price_data(self, symbol: str, start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> Dict[str, Any]:
        """获取A股股票的价格数据
        :param symbol: 股票代码
        :param start_date: 开始日期，格式为 'yyyy-mm-dd'
        :param end_date: 结束日期，格式为 'yyyy-mm-dd'
        :return: 包含三种复权类型数据的DataFrame，列名为英文
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
                '日期': 'date',
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
            df_qfq = ak.stock_zh_a_hist(symbol=formatted_symbol,
                                        start_date=start_date_fmt,
                                        end_date=end_date_fmt,
                                        adjust='qfq')
            df_qfq.rename(columns=column_mapping, inplace=True)
            df_qfq['adjust_type'] = 'qfq'  # 添加复权类型标识

            # 后复权数据
            df_hfq = ak.stock_zh_a_hist(symbol=formatted_symbol,
                                        start_date=start_date_fmt,
                                        end_date=end_date_fmt,
                                        adjust='hfq')
            df_hfq.rename(columns=column_mapping, inplace=True)
            df_hfq['adjust_type'] = 'hfq'  # 添加复权类型标识

            # 不复权数据
            df_none = ak.stock_zh_a_hist(symbol=formatted_symbol,
                                         start_date=start_date_fmt,
                                         end_date=end_date_fmt,
                                         adjust='')
            df_none.rename(columns=column_mapping, inplace=True)
            df_none['adjust_type'] = 'none'  # 添加复权类型标识

            # 合并所有数据
            import pandas as pd
            result_df = pd.concat([df_qfq, df_hfq, df_none], ignore_index=True)

            self.logger.info(f'成功获取股票 {formatted_symbol} 的三种复权类型价格数据')
            return result_df

        except Exception as e:
            self.logger.error(f'获取股票 {symbol} 价格数据时发生错误: {str(e)}')
            raise

    def get_company_info(self, symbol: str) -> Dict[str, Any]:
        """获取A股公司基本信息
        :param symbol: 股票代码
        :return: 公司基本信息，以DataFrame形式返回，列名为英文
        """
        try:
            # 格式化股票代码
            formatted_symbol = self._format_symbol(symbol)

            # 获取公司基本信息
            info = ak.stock_individual_info_em(symbol=formatted_symbol)

            # 创建一个新的DataFrame，将信息转换为标准格式
            import pandas as pd

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


# 可以添加其他数据提供者实现，例如：
# class TushareProvider(StockDataProvider):
#     """Tushare数据提供者实现"""
#     ...


class StockPriceFetcher:
    """股票数据获取器，用于批量获取股票数据"""

    def __init__(self, provider: Optional[Union[StockDataProvider, str]] = None):
        """初始化股票数据获取器
        :param provider: 数据提供者，可以是 StockDataProvider 实例或提供者名称字符串，默认使用 'akshare'
        """
        self.logger = logging.getLogger(__name__)
        self.available_providers = {
            'akshare': AkshareProvider,
            # 可以在这里添加更多的提供者
            # 'tushare': TushareProvider,
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

    def fetch_stock_price(self, symbol: str, start_date: Optional[datetime] = None,
                          end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """获取单个股票的价格数据
        :param symbol: 股票代码
        :param start_date: 开始日期
        :param end_date: 结束日期
        :return: 股票价格数据
        """
        return self.provider.get_price_data(symbol, start_date, end_date)

    def fetch_company_info(self, symbol: str) -> Dict[str, Any]:
        """获取单个股票的公司信息
        :param symbol: 股票代码
        :return: 公司信息
        """
        return self.provider.get_company_info(symbol)

    def fetch_multiple_stocks(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """批量获取多个股票的数据
        :param symbols: 股票代码列表
        :return: 多个股票的数据列表
        """
        results = []
        for symbol in symbols:
            try:
                data = self.fetch_stock_price(symbol)
                results.append(data)
            except Exception as e:
                self.logger.error(f'获取股票 {symbol} 数据失败: {str(e)}')
                continue
        return results
