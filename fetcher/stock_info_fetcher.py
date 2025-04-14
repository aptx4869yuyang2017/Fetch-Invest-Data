#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import pandas as pd
import akshare as ak
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from tqdm import tqdm
from utils.cache_utils import FileCache


class StockInfoFetcher:
    """
    A股个股信息获取器
    用于获取A股个股基本信息
    """

    def __init__(self, cache_dir=None):
        self.logger = logging.getLogger(__name__)
        # 设置默认缓存目录
        if cache_dir is None:
            cache_dir = os.path.join(os.path.dirname(os.path.dirname(
                os.path.abspath(__file__))), 'cache', 'stock_info')
        os.makedirs(cache_dir, exist_ok=True)
        self.cache = FileCache(cache_dir)
        self.default_ttl = 86400  # 默认缓存时间为1天

    def get_stock_list(self, use_cache=True):
        """
        获取所有A股股票代码列表

        Args:
            use_cache (bool, optional): 是否使用缓存。默认为True。

        Returns:
            list: 包含所有A股股票代码的列表
        """
        cache_key = "stock_list"

        # 如果使用缓存且缓存存在，则直接返回缓存数据
        if use_cache:
            cached_data = self.cache.get(cache_key)
            if cached_data is not None:
                self.logger.info("使用缓存的A股股票代码列表")
                return cached_data

        try:
            self.logger.info("正在获取A股股票代码列表...")
            # 使用akshare获取A股股票列表
            stock_info_df = ak.stock_info_a_code_name()
            stock_codes = stock_info_df['code'].tolist()
            self.logger.info(f"成功获取A股股票代码列表，共 {len(stock_codes)} 只股票")

            # 将数据存入缓存
            if use_cache:
                self.cache.set(cache_key, stock_codes, ttl=self.default_ttl)

            return stock_codes
        except Exception as e:
            self.logger.error(f"获取A股股票代码列表失败: {str(e)}")
            raise

    def get_stock_info(self, symbol):
        """
        获取指定股票的基本信息

        Args:
            symbol (str): 股票代码，如 "600519"

        Returns:
            pandas.DataFrame: 包含股票基本信息的DataFrame
        """
        try:
            self.logger.debug(f"正在获取股票 {symbol} 的基本信息...")
            stock_info = ak.stock_individual_info_em(symbol=symbol)

            # 将结果转换为更易于使用的格式
            if not stock_info.empty:
                # 将item和value列转换为字典格式
                info_dict = dict(zip(stock_info['item'], stock_info['value']))

                # 创建新的DataFrame，每个字段作为一列
                result = pd.DataFrame([info_dict])

                # 添加symbol列
                result['symbol'] = symbol

                # 重命名列
                column_mapping = {
                    '股票代码': 'stock_code',
                    '股票简称': 'stock_name',
                    '总股本': 'total_shares',
                    '流通股': 'circulating_shares',
                    '总市值': 'total_market_value',
                    '流通市值': 'circulating_market_value',
                    '行业': 'industry',
                    '上市时间': 'listing_date'
                }

                # 只重命名存在的列
                for old_name, new_name in column_mapping.items():
                    if old_name in result.columns:
                        result = result.rename(columns={old_name: new_name})

                # 数值类型转换
                numeric_columns = ['total_shares', 'circulating_shares',
                                   'total_market_value', 'circulating_market_value']
                for col in numeric_columns:
                    if col in result.columns:
                        result[col] = pd.to_numeric(
                            result[col], errors='coerce')

                # 日期类型转换
                if 'listing_date' in result.columns:
                    result['listing_date'] = pd.to_datetime(
                        result['listing_date'], format='%Y%m%d', errors='coerce')

                return result
            else:
                self.logger.warning(f"获取股票 {symbol} 的基本信息返回空结果")
                return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"获取股票 {symbol} 的基本信息失败: {str(e)}")
            return pd.DataFrame()

    def get_stock_info_batch(self, symbols, max_workers=2, delay=0.5, use_cache=True):
        """
        批量获取多只股票的基本信息

        Args:
            symbols (list): 股票代码列表
            max_workers (int, optional): 最大线程数。默认为2。
            delay (float, optional): 请求间隔时间(秒)。默认为0.5。
            use_cache (bool, optional): 是否使用缓存。默认为True。

        Returns:
            pandas.DataFrame: 包含所有股票基本信息的DataFrame
        """
        # 移除缓存相关代码，不再使用缓存
        try:
            self.logger.info(f"开始批量获取 {len(symbols)} 只股票的基本信息...")

            # 创建一个空的DataFrame用于存储结果
            all_stock_info = pd.DataFrame()

            # 使用线程池并行获取股票信息
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_symbol = {}
                for symbol in symbols:
                    def get_stock_info_with_delay(code=symbol):
                        time.sleep(delay)
                        return self.get_stock_info(code)

                    future_to_symbol[executor.submit(
                        get_stock_info_with_delay)] = symbol

                # 使用tqdm创建进度条
                with tqdm(total=len(symbols), desc="获取股票基本信息") as pbar:
                    # 处理结果
                    for future in as_completed(future_to_symbol):
                        symbol = future_to_symbol[future]
                        try:
                            stock_info = future.result()
                            if not stock_info.empty:
                                all_stock_info = pd.concat(
                                    [all_stock_info, stock_info], ignore_index=True)
                                self.logger.debug(
                                    f"成功获取股票 {symbol} 的基本信息")
                            else:
                                self.logger.warning(
                                    f"股票 {symbol} 获取基本信息失败或返回空结果")
                        except Exception as e:
                            self.logger.error(
                                f"处理股票 {symbol} 的基本信息时出错: {str(e)}")
                        pbar.update(1)

            # 统计成功获取的数量
            success_count = len(all_stock_info)
            self.logger.info(f"批量获取完成，成功率: {success_count}/{len(symbols)}")

            return all_stock_info

        except Exception as e:
            self.logger.error(f"批量获取股票基本信息失败: {str(e)}")
            raise

    def get_stock_info_all(self, max_workers=2, delay=0.5, use_cache=True):
        """
        获取所有A股股票的基本信息

        Args:
            max_workers (int, optional): 最大线程数。默认为2。
            delay (float, optional): 请求间隔时间(秒)。默认为0.5。
            use_cache (bool, optional): 是否使用缓存。默认为True。

        Returns:
            pandas.DataFrame: 包含所有A股股票基本信息的DataFrame
        """
        # 只保留股票列表的缓存，不缓存股票详细信息
        try:
            # 获取所有股票代码（这里仍然使用缓存）
            stock_codes = self.get_stock_list(use_cache=use_cache)

            # 批量获取所有股票信息（不使用缓存）
            all_stock_info = self.get_stock_info_batch(
                stock_codes, max_workers=max_workers, delay=delay, use_cache=False)

            return all_stock_info

        except Exception as e:
            self.logger.error(f"获取所有A股股票基本信息失败: {str(e)}")
            raise
