#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from typing import List, Dict, Any, Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
import time
from dotenv import load_dotenv

from .base_financial_report_provider import FinancialReportProvider
from .a_financial_report_provider_akshare import AFinancialReportProviderAkshare

# 加载环境变量
load_dotenv()

logger = logging.getLogger(__name__)


class AFinancialReportFetcher:
    """财务报表数据获取器，用于批量获取财务报表数据"""

    def __init__(self, provider: Optional[Union[FinancialReportProvider, str]] = None):
        """初始化财务报表数据获取器

        Args:
            provider: 数据提供者，可以是 FinancialReportProvider 实例或提供者名称字符串，默认使用 'akshare'
        """
        self.logger = logging.getLogger(__name__)
        self.available_providers = {
            'akshare': AFinancialReportProviderAkshare,
            # 可以在这里添加其他数据提供者
        }

        self.set_provider(provider or 'akshare')

    def set_provider(self, provider: Union[FinancialReportProvider, str]):
        """设置数据提供者

        Args:
            provider: 数据提供者实例或提供者名称字符串

        Raises:
            ValueError: 当提供的名称不存在时抛出异常
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
        elif isinstance(provider, FinancialReportProvider):
            self.provider = provider
            self.logger.info(f'已切换数据提供者为: {provider.__class__.__name__}')
        else:
            raise ValueError("provider 必须是 FinancialReportProvider 实例或者字符串名称")

    def fetch_balance_sheet(self, symbol: str) -> pd.DataFrame:
        """获取单个股票的资产负债表数据

        Args:
            symbol: 股票代码，格式为 "600000"（不带市场前缀）

        Returns:
            资产负债表数据DataFrame
        """
        try:
            df = self.provider.get_balance_sheet(symbol)

            if df.empty:
                self.logger.warning(f"未获取到股票 {symbol} 的资产负债表数据")
            else:
                self.logger.debug(f"成功获取股票 {symbol} 的资产负债表数据，共 {len(df)} 条记录")

            return df
        except Exception as e:
            self.logger.error(
                f"获取股票 {symbol} 的资产负债表数据失败: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def fetch_income_statement(self, symbol: str) -> pd.DataFrame:
        """获取单个股票的利润表数据

        Args:
            symbol: 股票代码，格式为 "600000"（不带市场前缀）

        Returns:
            利润表数据DataFrame
        """
        try:
            df = self.provider.get_income_statement(symbol)

            if df.empty:
                self.logger.warning(f"未获取到股票 {symbol} 的利润表数据")
            else:
                self.logger.debug(f"成功获取股票 {symbol} 的利润表数据，共 {len(df)} 条记录")

            return df
        except Exception as e:
            self.logger.error(
                f"获取股票 {symbol} 的利润表数据失败: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def fetch_cash_flow_statement(self, symbol: str) -> pd.DataFrame:
        """获取单个股票的现金流量表数据

        Args:
            symbol: 股票代码，格式为 "600000"（不带市场前缀）

        Returns:
            现金流量表数据DataFrame
        """
        try:
            df = self.provider.get_cash_flow_statement(symbol)

            if df.empty:
                self.logger.warning(f"未获取到股票 {symbol} 的现金流量表数据")
            else:
                self.logger.debug(f"成功获取股票 {symbol} 的现金流量表数据，共 {len(df)} 条记录")

            return df
        except Exception as e:
            self.logger.error(
                f"获取股票 {symbol} 的现金流量表数据失败: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def fetch_multiple_cash_flow_statements(self, symbols: List[str], max_workers: int = 5,
                                            delay: float = 0.5, merge_results: bool = True) -> Union[Dict[str, pd.DataFrame], pd.DataFrame]:
        """批量获取多个股票的现金流量表数据

        Args:
            symbols: 股票代码列表，格式为 ["600000", "000001"]
            max_workers: 最大线程数，默认为 5
            delay: 每个请求之间的延迟时间(秒)，避免频繁请求被限制，默认0.5秒
            merge_results: 是否合并结果为一个DataFrame，默认为True

        Returns:
            如果merge_results为True，返回合并后的DataFrame，包含所有股票的现金流量表数据，
            并添加'symbol'列标识股票代码；
            否则返回包含多个股票现金流量表的字典，键为股票代码，值为对应的DataFrame
        """
        self.logger.info(f"开始批量获取 {len(symbols)} 只股票的现金流量表数据")

        results = {}

        def fetch_single_cash_flow_statement(symbol):
            try:
                time.sleep(delay)  # 添加延迟，避免请求过于频繁
                return symbol, self.fetch_cash_flow_statement(symbol)
            except Exception as e:
                self.logger.error(
                    f"获取股票 {symbol} 的现金流量表数据失败: {str(e)}", exc_info=True)
                return symbol, pd.DataFrame()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_symbol = {executor.submit(fetch_single_cash_flow_statement, symbol): symbol
                                for symbol in symbols}

            # 使用tqdm创建进度条
            with tqdm(total=len(symbols), desc="获取现金流量表数据") as pbar:
                # 获取完成的任务结果
                for future in as_completed(future_to_symbol):
                    symbol, df = future.result()
                    results[symbol] = df
                    pbar.update(1)

        # 统计成功获取的数量
        success_count = sum(1 for df in results.values() if not df.empty)
        self.logger.info(f"批量获取完成，成功率: {success_count}/{len(symbols)}")

        # 如果需要合并结果
        if merge_results:
            merged_df = pd.DataFrame()
            for symbol, df in results.items():
                if not df.empty:
                    # 添加股票代码列
                    df = df.copy()
                    merged_df = pd.concat([merged_df, df], ignore_index=True)

            self.logger.info(f"已将 {success_count} 只股票的现金流量表数据合并为一个DataFrame")
            return merged_df

        return results

    def fetch_multiple_balance_sheets(self, symbols: List[str], max_workers: int = 5,
                                      delay: float = 0.5, merge_results: bool = True) -> Union[Dict[str, pd.DataFrame], pd.DataFrame]:
        """批量获取多个股票的资产负债表数据

        Args:
            symbols: 股票代码列表，格式为 ["600000", "000001"]
            max_workers: 最大线程数，默认为 5
            delay: 每个请求之间的延迟时间(秒)，避免频繁请求被限制，默认0.5秒
            merge_results: 是否合并结果为一个DataFrame，默认为True

        Returns:
            如果merge_results为True，返回合并后的DataFrame，包含所有股票的资产负债表数据，
            并添加'symbol'列标识股票代码；
            否则返回包含多个股票资产负债表的字典，键为股票代码，值为对应的DataFrame
        """
        self.logger.info(f"开始批量获取 {len(symbols)} 只股票的资产负债表数据")

        results = {}

        def fetch_single_balance_sheet(symbol):
            try:
                time.sleep(delay)  # 添加延迟，避免请求过于频繁
                return symbol, self.fetch_balance_sheet(symbol)
            except Exception as e:
                self.logger.error(
                    f"获取股票 {symbol} 的资产负债表数据失败: {str(e)}", exc_info=True)
                return symbol, pd.DataFrame()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_symbol = {executor.submit(fetch_single_balance_sheet, symbol): symbol
                                for symbol in symbols}

            # 使用tqdm创建进度条
            with tqdm(total=len(symbols), desc="获取资产负债表数据") as pbar:
                # 获取完成的任务结果
                for future in as_completed(future_to_symbol):
                    symbol, df = future.result()
                    results[symbol] = df
                    pbar.update(1)

        # 统计成功获取的数量
        success_count = sum(1 for df in results.values() if not df.empty)
        self.logger.info(f"批量获取完成，成功率: {success_count}/{len(symbols)}")

        # 如果需要合并结果
        if merge_results:
            merged_df = pd.DataFrame()
            for symbol, df in results.items():
                if not df.empty:
                    # 添加股票代码列
                    df = df.copy()
                    merged_df = pd.concat([merged_df, df], ignore_index=True)

            self.logger.info(f"已将 {success_count} 只股票的资产负债表数据合并为一个DataFrame")
            return merged_df

        return results

    def fetch_multiple_income_statements(self, symbols: List[str], max_workers: int = 5,
                                         delay: float = 0.5, merge_results: bool = True) -> Union[Dict[str, pd.DataFrame], pd.DataFrame]:
        """批量获取多个股票的利润表数据

        Args:
            symbols: 股票代码列表，格式为 ["600000", "000001"]
            max_workers: 最大线程数，默认为 5
            delay: 每个请求之间的延迟时间(秒)，避免频繁请求被限制，默认0.5秒
            merge_results: 是否合并结果为一个DataFrame，默认为True

        Returns:
            如果merge_results为True，返回合并后的DataFrame，包含所有股票的利润表数据，
            并添加'symbol'列标识股票代码；
            否则返回包含多个股票利润表的字典，键为股票代码，值为对应的DataFrame
        """
        self.logger.info(f"开始批量获取 {len(symbols)} 只股票的利润表数据")

        results = {}

        def fetch_single_income_statement(symbol):
            try:
                time.sleep(delay)  # 添加延迟，避免请求过于频繁
                return symbol, self.fetch_income_statement(symbol)
            except Exception as e:
                self.logger.error(
                    f"获取股票 {symbol} 的利润表数据失败: {str(e)}", exc_info=True)
                return symbol, pd.DataFrame()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_symbol = {executor.submit(fetch_single_income_statement, symbol): symbol
                                for symbol in symbols}

            # 使用tqdm创建进度条
            with tqdm(total=len(symbols), desc="获取利润表数据") as pbar:
                # 获取完成的任务结果
                for future in as_completed(future_to_symbol):
                    symbol, df = future.result()
                    results[symbol] = df
                    pbar.update(1)

        # 统计成功获取的数量
        success_count = sum(1 for df in results.values() if not df.empty)
        self.logger.info(f"批量获取完成，成功率: {success_count}/{len(symbols)}")

        # 如果需要合并结果
        if merge_results:
            merged_df = pd.DataFrame()
            for symbol, df in results.items():
                if not df.empty:
                    # 添加股票代码列
                    df = df.copy()
                    merged_df = pd.concat([merged_df, df], ignore_index=True)

            self.logger.info(f"已将 {success_count} 只股票的利润表数据合并为一个DataFrame")
            return merged_df

        return results
