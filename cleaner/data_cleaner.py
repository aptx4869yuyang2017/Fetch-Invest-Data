#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Union, List

logger = logging.getLogger(__name__)

class DataCleaner:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def clean_stock_price_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        清洗股票价格数据
        :param data: 原始股票价格数据
        :return: 清洗后的数据
        """
        try:
            prices = pd.DataFrame(data['prices'])
            
            # 处理缺失值
            prices = prices.fillna(method='ffill')
            
            # 处理异常值（例如：将超过3个标准差的值视为异常）
            for column in ['Open', 'High', 'Low', 'Close']:
                if column in prices.columns:
                    mean = prices[column].mean()
                    std = prices[column].std()
                    prices[column] = prices[column].clip(mean - 3*std, mean + 3*std)
            
            # 添加技术指标
            prices['Daily_Return'] = prices['Close'].pct_change()
            prices['MA5'] = prices['Close'].rolling(window=5).mean()
            prices['MA20'] = prices['Close'].rolling(window=20).mean()
            
            # 更新数据
            data['prices'] = prices.to_dict('records')
            self.logger.info(f'成功清洗股票 {data["symbol"]} 的价格数据')
            return data
            
        except Exception as e:
            self.logger.error(f'清洗股票价格数据时发生错误: {str(e)}')
            raise

    def clean_financial_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        清洗财务数据
        :param data: 原始财务数据
        :return: 清洗后的数据
        """
        try:
            # 处理财务报表数据
            for statement in ['balance_sheet', 'income_statement', 'cash_flow']:
                if statement in data:
                    df = pd.DataFrame(data[statement])
                    # 处理缺失值
                    df = df.fillna(0)
                    # 将所有数值转换为浮点数
                    df = df.astype(float)
                    data[statement] = df.to_dict()
            
            # 处理关键指标
            metrics = {k: float(v) if isinstance(v, (int, float)) else v 
                      for k, v in data.items() 
                      if k not in ['symbol', 'balance_sheet', 'income_statement', 'cash_flow']}
            
            data.update(metrics)
            self.logger.info(f'成功清洗股票 {data["symbol"]} 的财务数据')
            return data
            
        except Exception as e:
            self.logger.error(f'清洗财务数据时发生错误: {str(e)}')
            raise

    def validate_data(self, data: Dict[str, Any]) -> bool:
        """
        验证数据的完整性和有效性
        :param data: 待验证的数据
        :return: 验证结果
        """
        try:
            required_fields = ['symbol']
            
            # 检查必要字段
            if not all(field in data for field in required_fields):
                self.logger.error('数据缺少必要字段')
                return False
            
            # 检查数值字段的有效性
            if 'prices' in data:
                prices = pd.DataFrame(data['prices'])
                if prices.empty or prices.isnull().all().all():
                    self.logger.error('价格数据无效')
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f'数据验证时发生错误: {str(e)}')
            return False