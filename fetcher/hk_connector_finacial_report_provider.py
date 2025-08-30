#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import akshare as ak
import pandas as pd
import logging
from typing import Dict, Any, Optional

from fetcher.base_financial_report_provider import FinancialReportProvider


class HKConnectorFinancialReportProvider(FinancialReportProvider):
    """基于AkShare的港股通财务报表数据提供者实现"""

    def __init__(self):
        """初始化港股通财务报表数据提供者"""
        self.logger = logging.getLogger(__name__)

    def get_balance_sheet(self, symbol: str) -> pd.DataFrame:
        """获取港股通资产负债表数据

        Args:
            symbol: 港股股票代码，格式为 "00700"（不带市场前缀）

        Returns:
            资产负债表数据DataFrame，包含报告期和各项资产负债表指标
        """
        self.logger.debug(f"开始获取港股 {symbol} 的资产负债表数据")
        try:

            # 使用AkShare的港股资产负债表接口
            df = ak.stock_financial_hk_report_em(
                stock=symbol, symbol="资产负债表", indicator="报告期")
            # 先将列名转换为字符串类型，然后再转换为小写
            df.columns = df.columns.astype(str).str.lower()

            # 将日期列转换为 datetime64 类型
            if 'report_date' in df.columns:
                df['report_date'] = pd.to_datetime(
                    df['report_date'], errors='coerce').dt.date

            # 处理可能的NaN值
            df = df.fillna("")
            self.logger.debug(f"成功处理港股 {symbol} 的资产负债表数据，最终数据包含 {len(df)} 行")
            return df

        except Exception as e:
            # 异常处理
            self.logger.error(
                f"获取港股 {symbol} 的资产负债表数据失败: {str(e)}", exc_info=True)
            # 返回空DataFrame
            return pd.DataFrame()

    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        """获取港股通利润表数据

        Args:
            symbol: 港股股票代码，格式为 "00700"（不带市场前缀）

        Returns:
            利润表数据DataFrame，包含报告期和各项利润表指标
        """
        self.logger.debug(f"开始获取港股 {symbol} 的利润表数据")
        try:

            # 使用AkShare的港股利润表接口
            df = ak.stock_financial_hk_report_em(
                stock=symbol, symbol="利润表", indicator="报告期")

            # 先将列名转换为字符串类型，然后再转换为小写
            df.columns = df.columns.astype(str).str.lower()

            # 将日期列转换为 datetime64 类型
            if 'report_date' in df.columns:
                df['report_date'] = pd.to_datetime(
                    df['report_date'], errors='coerce').dt.date

            # 处理可能的NaN值
            df = df.fillna("")
            self.logger.debug(f"成功处理港股 {symbol} 的利润表数据，最终数据包含 {len(df)} 行")
            return df

        except Exception as e:
            # 异常处理
            self.logger.error(
                f"获取港股 {symbol} 的利润表数据失败: {str(e)}", exc_info=True)
            # 返回空DataFrame
            return pd.DataFrame()

    def get_cash_flow_statement(self, symbol: str) -> pd.DataFrame:
        """获取港股通现金流量表数据

        Args:
            symbol: 港股股票代码，格式为 "00700"（不带市场前缀）

        Returns:
            现金流量表数据DataFrame，包含报告期和各项现金流量表指标
        """
        self.logger.debug(f"开始获取港股 {symbol} 的现金流量表数据")
        try:

            # 使用AkShare的港股现金流量表接口
            df = ak.stock_financial_hk_report_em(
                stock=symbol, symbol="现金流量表", indicator="报告期")

            # 先将列名转换为字符串类型，然后再转换为小写
            df.columns = df.columns.astype(str).str.lower()

            # 将日期列转换为 datetime64 类型
            if 'report_date' in df.columns:
                df['report_date'] = pd.to_datetime(
                    df['report_date'], errors='coerce').dt.date

            # 处理可能的NaN值
            df = df.fillna("")
            self.logger.debug(f"成功处理港股 {symbol} 的现金流量表数据，最终数据包含 {len(df)} 行")
            return df

        except Exception as e:
            # 异常处理
            self.logger.error(
                f"获取港股 {symbol} 的现金流量表数据失败: {str(e)}", exc_info=True)
            # 返回空DataFrame
            return pd.DataFrame()
