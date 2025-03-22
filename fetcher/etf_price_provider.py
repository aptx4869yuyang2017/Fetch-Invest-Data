#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime


class ETFDataProvider(ABC):
    """ETF数据提供者基类"""

    @abstractmethod
    def get_price_data(self, symbol: str, start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """获取ETF的价格数据
        :param symbol: ETF代码
        :param start_date: 开始日期
        :param end_date: 结束日期
        :return: ETF价格数据
        """
        pass

    @abstractmethod
    def get_etf_info(self, symbol: str) -> Dict[str, Any]:
        """获取ETF基本信息
        :param symbol: ETF代码
        :return: ETF基本信息
        """
        pass

    @abstractmethod
    def get_all_etf_codes(self) -> List[str]:
        """获取所有ETF代码
        :return: ETF代码列表
        """
        pass