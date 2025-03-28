#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd


class FinancialReportProvider(ABC):
    """财务报表数据提供者抽象基类，定义统一接口"""

    @abstractmethod
    def get_balance_sheet(self, symbol: str) -> pd.DataFrame:
        """获取资产负债表数据的抽象方法

        Args:
            symbol: 股票代码

        Returns:
            资产负债表数据，DataFrame格式
        """
        pass

    @abstractmethod
    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        """获取利润表数据的抽象方法

        Args:
            symbol: 股票代码

        Returns:
            利润表数据，DataFrame格式
        """
        pass

    @abstractmethod
    def get_cash_flow_statement(self, symbol: str) -> pd.DataFrame:
        """获取现金流量表数据的抽象方法

        Args:
            symbol: 股票代码

        Returns:
            现金流量表数据，DataFrame格式
        """
        pass
