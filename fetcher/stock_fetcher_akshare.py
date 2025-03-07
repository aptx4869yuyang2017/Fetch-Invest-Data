#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import akshare as ak
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from .base import Stock
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class StockDataProvider(ABC):
    """股票数据提供者抽象基类，定义统一接口"""

    @abstractmethod
    def get_price_data(self, symbol: str, start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None) -> Dict[str, Any]:
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

    def get_price_data(self, symbol: str, start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """获取A股股票的价格数据
        :param symbol: 股票代码
        :param start_date: 开始日期
        :param end_date: 结束日期
        :return: 股票价格数据
        """
        try:
            # 格式化股票代码
            formatted_symbol = self._format_symbol(symbol)

            # 设置默认日期范围
            if not start_date:
                start_date = datetime.now() - timedelta(days=30)
            if not end_date:
                end_date = datetime.now()

            # 使用akshare获取股票数据
            df = ak.stock_zh_a_hist(symbol=formatted_symbol,
                                    start_date=start_date.strftime('%Y%m%d'),
                                    end_date=end_date.strftime('%Y%m%d'),
                                    adjust='qfq')

            # 转换数据格式
            price_data = {
                'symbol': formatted_symbol,
                'prices': df.to_dict('records'),
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d')
            }

            self.logger.info(f'成功获取股票 {formatted_symbol} 的价格数据')
            return price_data

        except Exception as e:
            self.logger.error(f'获取股票 {symbol} 价格数据时发生错误: {str(e)}')
            raise

    def get_company_info(self, symbol: str) -> Dict[str, Any]:
        """获取A股公司基本信息
        :param symbol: 股票代码
        :return: 公司基本信息
        """
        try:
            # 格式化股票代码
            formatted_symbol = self._format_symbol(symbol)

            # 获取公司基本信息
            info = ak.stock_individual_info_em(symbol=formatted_symbol)

            # 转换为字典格式
            company_info = {
                'symbol': formatted_symbol,
                'info': info.to_dict('records')[0] if not info.empty else {}
            }

            self.logger.info(f'成功获取股票 {formatted_symbol} 的公司信息')
            return company_info

        except Exception as e:
            self.logger.error(f'获取股票 {symbol} 公司信息时发生错误: {str(e)}')
            raise


# 可以添加其他数据提供者实现，例如：
# class TushareProvider(StockDataProvider):
#     """Tushare数据提供者实现"""
#     ...


class StockFetcher:
    """股票数据获取器，用于批量获取股票数据"""

    def __init__(self, provider: Optional[StockDataProvider] = None):
        """初始化股票数据获取器
        :param provider: 数据提供者，默认使用AkshareProvider
        """
        self.logger = logging.getLogger(__name__)
        self.provider = provider or AkshareProvider()

    def set_provider(self, provider: StockDataProvider):
        """设置数据提供者
        :param provider: 数据提供者实例
        """
        self.provider = provider
        self.logger.info(f'已切换数据提供者为: {provider.__class__.__name__}')

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
