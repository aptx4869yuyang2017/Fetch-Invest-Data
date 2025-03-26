#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import akshare as ak
import pandas as pd
import logging
from typing import Dict, Any, Optional

from fetcher.financial_report_provider import FinancialReportProvider


class FinancialReportProviderAkshare(FinancialReportProvider):
    """基于AkShare的财务报表数据提供者实现"""

    def __init__(self):
        """初始化AkShare财务报表数据提供者"""
        self.logger = logging.getLogger(__name__)

    def get_balance_sheet(self, symbol: str) -> pd.DataFrame:
        """获取资产负债表数据

        Args:
            symbol: 股票代码，格式为 "600000"（不带市场前缀）

        Returns:
            资产负债表数据DataFrame，包含报告期和各项资产负债表指标
        """
        self.logger.debug(f"开始获取股票 {symbol} 的资产负债表数据")
        try:
            # 添加市场前缀
            market_prefix = "sh" if symbol.startswith("6") else "sz"
            full_symbol = f"{market_prefix}{symbol}"

            df = ak.stock_balance_sheet_by_report_em(symbol=full_symbol)

            # 排除所有包含 YOY 的列（同比增长率指标）
            df = df.loc[:, ~df.columns.str.contains('YOY', case=False)]

            # 列名映射
            column_mapping = {
                'security_code': 'symbol',
                'secucode': 'symbol_full',
                'security_name_abbr': 'symbol_name_abbr'
            }

            # 重命名列

            # 将列名转换为小写
            df.columns = df.columns.str.lower()
            df = df.rename(columns=column_mapping)

            # 将日期列转换为 datetime64 类型
            if 'report_date' in df.columns:
                df['report_date'] = pd.to_datetime(
                    df['report_date'], errors='coerce').dt.date

            if 'notice_date' in df.columns:
                df['notice_date'] = pd.to_datetime(
                    df['notice_date'], errors='coerce').dt.date

            if 'update_date' in df.columns:
                df['update_date'] = pd.to_datetime(
                    df['update_date'], errors='coerce').dt.date

            # 处理可能的NaN值
            df = df.fillna("")
            self.logger.debug(f"成功处理股票 {symbol} 的资产负债表数据，最终数据包含 {len(df)} 行")
            return df

        except Exception as e:
            # 异常处理
            self.logger.error(
                f"获取股票 {symbol} 的资产负债表数据失败: {str(e)}", exc_info=True)
            # 返回空DataFrame
            return pd.DataFrame()
