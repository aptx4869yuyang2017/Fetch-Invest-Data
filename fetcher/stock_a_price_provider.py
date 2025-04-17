#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


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
