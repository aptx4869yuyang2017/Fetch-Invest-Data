#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import pandas as pd
import akshare as ak
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from utils.stock_utils import get_full_symbol
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import os
from utils.cache_utils import FileCache
from fetcher.stock_a_all_code_fetcher import StockAAllCodeFetcher


class StockIndicatorFetcher:
    """
    A股个股指标获取器
    用于获取A股个股财务指标信息
    """

    def __init__(self, max_retries=3):
        self.logger = logging.getLogger(__name__)
        self.max_retries = max_retries
        # 初始化缓存，用于存储失败的股票代码
        cache_dir = os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), 'cache', 'failed_symbols')
        self.cache = FileCache(cache_dir)

    def get_stock_indicator(self, symbol):
        """
        获取指定股票的财务指标信息

        Args:
            symbol (str): 股票代码，如 "000001"

        Returns:
            pandas.DataFrame: 包含股票财务指标信息的DataFrame
        """
        retry_count = 0
        max_attempts = self.max_retries + 1

        while retry_count < max_attempts:
            try:
                self.logger.debug(
                    f"正在获取股票 {symbol} 的财务指标信息... (尝试 {retry_count + 1}/{max_attempts})")
                # 使用akshare获取财务指标信息
                stock_indicator = ak.stock_a_indicator_lg(symbol=symbol)

                if not stock_indicator.empty:
                    # 添加symbol列
                    stock_indicator['symbol'] = symbol
                    self.logger.debug(f"成功获取股票 {symbol} 的财务指标信息")
                    return stock_indicator
                else:
                    self.logger.warning(f"获取股票 {symbol} 的财务指标信息返回空结果")
                    return pd.DataFrame()

            except Exception as e:
                retry_count += 1
                if retry_count >= max_attempts:
                    self.logger.error(
                        f"获取股票 {symbol} 的财务指标信息失败 (已重试 {retry_count} 次): {str(e)}")
                    return pd.DataFrame()
                else:
                    self.logger.warning(
                        f"获取股票 {symbol} 的财务指标信息失败，将重试 ({retry_count}/{self.max_retries}): {str(e)}")
                    # 指数退避，每次重试等待时间增加
                    time.sleep(3 * retry_count)

    def get_stock_indicator_batch(self, symbols, max_workers=2, delay=0.5):
        """
        批量获取多只股票的财务指标信息

        Args:
            symbols (list): 股票代码列表
            max_workers (int, optional): 最大线程数。默认为2。
            delay (float, optional): 请求间隔时间(秒)。默认为0.5。

        Returns:
            pandas.DataFrame: 包含所有股票财务指标信息的DataFrame
        """
        try:
            self.logger.info(f"开始批量获取 {len(symbols)} 只股票的财务指标信息...")

            # 创建一个空的DataFrame用于存储结果
            all_stock_indicators = pd.DataFrame()
            failed_symbols = []

            # 使用线程池并行获取股票信息
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_symbol = {}
                for symbol in symbols:
                    def get_stock_indicator_with_delay(code=symbol):
                        time.sleep(delay)
                        return self.get_stock_indicator(code)

                    future_to_symbol[executor.submit(
                        get_stock_indicator_with_delay)] = symbol

                # 使用tqdm创建进度条
                with tqdm(total=len(symbols), desc="获取股票财务指标信息") as pbar:
                    # 处理结果
                    for future in as_completed(future_to_symbol):
                        symbol = future_to_symbol[future]
                        try:
                            stock_indicator = future.result()
                            if not stock_indicator.empty:
                                all_stock_indicators = pd.concat(
                                    [all_stock_indicators, stock_indicator], ignore_index=True)
                                self.logger.debug(
                                    f"成功获取股票 {symbol} 的财务指标信息")
                            else:
                                self.logger.warning(
                                    f"股票 {symbol} 获取财务指标信息失败或返回空结果")
                                failed_symbols.append(symbol)
                        except Exception as e:
                            self.logger.error(
                                f"处理股票 {symbol} 的财务指标信息时出错: {str(e)}")
                            failed_symbols.append(symbol)
                        pbar.update(1)

            # 统计成功获取的数量
            success_count = len(all_stock_indicators['symbol'].unique())
            self.logger.info(f"批量获取完成，成功率: {success_count}/{len(symbols)}")

            # 记录失败的股票代码
            if failed_symbols:
                self.logger.info(f"获取失败的股票数量: {len(failed_symbols)}")
                self.logger.debug(f"获取失败的股票代码: {failed_symbols}")
                # 将失败的股票代码缓存到文件中
                self.cache_failed_symbols(failed_symbols)

            return all_stock_indicators

        except Exception as e:
            self.logger.error(f"批量获取股票财务指标信息失败: {str(e)}")
            raise

    def get_stock_indicator_all(self, max_workers=5, delay=0.5, symbol_range=None):
        """
        获取所有A股股票的财务指标信息

        Args:
            max_workers (int, optional): 最大线程数。默认为5。
            delay (float, optional): 请求间隔时间(秒)。默认为0.5。
            symbol_range (tuple, optional): 股票代码范围，格式为(start_index, end_index)。
                                         例如：(0, 100)表示只获取前100只股票。默认为None，表示获取所有股票。

        Returns:
            pandas.DataFrame: 包含所有A股股票财务指标信息的DataFrame
        """
        try:
            # 使用 StockAAllCodeFetcher 获取所有股票代码
            self.logger.info("正在获取所有A股股票代码...")
            code_fetcher = StockAAllCodeFetcher()
            stock_codes = code_fetcher.get_all_stock_codes()

            # 如果指定了范围，则只获取指定范围内的股票代码
            if symbol_range and isinstance(symbol_range, tuple) and len(symbol_range) == 2:
                start_idx, end_idx = symbol_range
                stock_codes = stock_codes[start_idx:end_idx]
                self.logger.info(
                    f"根据指定范围获取 {len(stock_codes)} 只A股股票代码 (范围: {start_idx}-{end_idx})")
            else:
                self.logger.info(f"成功获取 {len(stock_codes)} 只A股股票代码")

            # 批量获取所有股票财务指标信息
            all_stock_indicators = self.get_stock_indicator_batch(
                stock_codes, max_workers=max_workers, delay=delay)

            return all_stock_indicators

        except Exception as e:
            self.logger.error(f"获取所有A股股票财务指标信息失败: {str(e)}")
            raise

    def cache_failed_symbols(self, failed_symbols, task_name="stock_indicator"):
        """
        缓存获取失败的股票代码，只保存最近一次失败的记录

        Args:
            failed_symbols (list): 获取失败的股票代码列表
            task_name (str, optional): 任务名称，用于区分不同的任务。默认为"stock_indicator"。
        """
        try:
            if not failed_symbols:
                return

            # 使用当前日期作为缓存键的一部分，以便区分不同日期的失败记录
            date_str = time.strftime("%Y%m%d")
            cache_key = f"{task_name}_failed_symbols_{date_str}"

            # 直接设置新的失败股票代码，覆盖之前的记录
            self.logger.info(f"缓存最近一次失败的股票代码，共 {len(failed_symbols)} 只")
            self.cache.set(cache_key, failed_symbols, ttl=86400*7)  # 缓存7天

        except Exception as e:
            self.logger.error(f"缓存失败股票代码时出错: {str(e)}")

    def get_failed_symbols(self, task_name="stock_indicator"):
        """
        获取最近一次失败的股票代码

        Args:
            task_name (str, optional): 任务名称。默认为"stock_indicator"。
            days: 已废弃参数，保留是为了向后兼容

        Returns:
            list: 最近一次获取失败的股票代码列表
        """
        try:
            # 只获取当天的失败记录
            date_str = time.strftime("%Y%m%d")
            cache_key = f"{task_name}_failed_symbols_{date_str}"

            failed_symbols = self.cache.get(cache_key)
            if failed_symbols:
                self.logger.info(f"获取到最近一次失败的股票代码，共 {len(failed_symbols)} 只")
                return failed_symbols

            return []

        except Exception as e:
            self.logger.error(f"获取失败股票代码时出错: {str(e)}")
            return []

    def retry_failed_symbols(self, max_workers=2, delay=1.0, task_name="stock_indicator"):
        """
        重试获取失败的股票财务指标信息

        Args:
            max_workers (int, optional): 最大线程数。默认为2。
            delay (float, optional): 请求间隔时间(秒)。默认为1.0。
            task_name (str, optional): 任务名称。默认为"stock_indicator"。

        Returns:
            pandas.DataFrame: 包含重试成功的股票财务指标信息的DataFrame
        """
        try:
            # 获取失败的股票代码
            failed_symbols = self.get_failed_symbols(task_name)

            if not failed_symbols:
                self.logger.info("没有需要重试的股票代码")
                return pd.DataFrame()

            self.logger.info(f"开始重试获取 {len(failed_symbols)} 只股票的财务指标信息...")

            # 使用更长的延迟时间和更少的线程数，以提高成功率
            return self.get_stock_indicator_batch(failed_symbols, max_workers=max_workers, delay=delay)

        except Exception as e:
            self.logger.error(f"重试获取失败股票财务指标信息时出错: {str(e)}")
            return pd.DataFrame()
