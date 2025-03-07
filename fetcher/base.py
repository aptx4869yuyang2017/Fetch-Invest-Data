#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class Stock(ABC):
    """股票基类，定义股票数据获取的基本接口"""
    
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.logger = logging.getLogger(__name__)
    
    @abstractmethod
    def get_price_data(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """获取股票价格数据的抽象方法"""
        pass
    
    @abstractmethod
    def get_company_info(self) -> Dict[str, Any]:
        """获取公司基本信息的抽象方法"""
        pass