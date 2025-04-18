#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import pandas as pd
import akshare as ak
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from utils.stock_utils import get_full_symbol
from .stock_a_all_code_fetcher import StockAAllCodeFetcher


class StockValueFetcher:
    """
    A股个股价值指标获取器
    用于获取A股个股价值指标信息（使用东方财富网数据）
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.code_fetcher = StockAAllCodeFetcher()

    def get_stock_value(self, symbol, start_date='2018-01-01', end_date=None):
        """
        获取指定股票的价值指标信息

        Args:
            symbol (str): 股票代码，如 "000001"
            start_date (str, optional): 开始日期，格式：YYYY-MM-DD，如 "2023-01-01"
            end_date (str, optional): 结束日期，格式：YYYY-MM-DD，如 "2023-12-31"

        Returns:
            pandas.DataFrame: 包含股票价值指标信息的DataFrame
        """
        try:
            self.logger.debug(f"正在获取股票 {symbol} 的价值指标信息...")
            # 使用akshare获取价值指标信息
            stock_value = ak.stock_value_em(symbol=symbol)

            if not stock_value.empty:
                # 重命名列
                column_map = {
                    '数据日期': 'date',
                    '当日收盘价': 'close_price',
                    '当日涨跌幅': 'price_change_pct',
                    '总市值': 'total_market_value',
                    '流通市值': 'circulating_market_value',
                    '总股本': 'total_shares',
                    '流通股本': 'circulating_shares',
                    'PE(TTM)': 'pe_ttm',
                    'PE(静)': 'pe_static',
                    '市净率': 'pb_ratio',
                    'PEG值': 'peg_ratio',
                    '市现率': 'pcf_ratio',
                    '市销率': 'ps_ratio'
                }
                stock_value = stock_value.rename(columns=column_map)

                # 添加日期过滤
                if start_date or end_date:
                    # 确保date列的格式为 YYYY-MM-DD
                    stock_value['date'] = pd.to_datetime(
                        stock_value['date']).dt.strftime('%Y-%m-%d')
                    if start_date:
                        stock_value = stock_value[stock_value['date']
                                                  >= start_date]
                    if end_date:
                        stock_value = stock_value[stock_value['date']
                                                  <= end_date]

                # 添加symbol列
                stock_value['symbol'] = symbol
                self.logger.debug(f"成功获取股票 {symbol} 的价值指标信息")
                return stock_value
            else:
                self.logger.warning(f"获取股票 {symbol} 的价值指标信息返回空结果")
                return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"获取股票 {symbol} 的价值指标信息失败: {str(e)}")
            return pd.DataFrame()

    def get_stock_value_batch(self, symbols, max_workers=2, delay=0.5, start_date='2018-01-01', end_date=None):
        """
        批量获取多只股票的价值指标信息

        Args:
            symbols (list): 股票代码列表
            max_workers (int, optional): 最大线程数。默认为2。
            delay (float, optional): 请求间隔时间(秒)。默认为0.5。
            start_date (str, optional): 开始日期，格式：YYYY-MM-DD，如 "2023-01-01"
            end_date (str, optional): 结束日期，格式：YYYY-MM-DD，如 "2023-12-31"

        Returns:
            pandas.DataFrame: 包含所有股票价值指标信息的DataFrame
        """
        try:
            self.logger.info(f"开始批量获取 {len(symbols)} 只股票的价值指标信息...")

            # 创建一个空的DataFrame用于存储结果
            all_stock_values = pd.DataFrame()

            # 使用线程池并行获取股票信息
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_symbol = {}
                for symbol in symbols:
                    def get_stock_value_with_delay(code=symbol):
                        time.sleep(delay)
                        return self.get_stock_value(code, start_date, end_date)

                    future_to_symbol[executor.submit(
                        get_stock_value_with_delay)] = symbol

                # 使用tqdm创建进度条
                with tqdm(total=len(symbols), desc="获取股票价值指标信息") as pbar:
                    # 处理结果
                    for future in as_completed(future_to_symbol):
                        symbol = future_to_symbol[future]
                        try:
                            stock_value = future.result()
                            if not stock_value.empty:
                                all_stock_values = pd.concat(
                                    [all_stock_values, stock_value], ignore_index=True)
                                self.logger.debug(
                                    f"成功获取股票 {symbol} 的价值指标信息")
                            else:
                                self.logger.warning(
                                    f"股票 {symbol} 获取价值指标信息失败或返回空结果")
                        except Exception as e:
                            self.logger.error(
                                f"处理股票 {symbol} 的价值指标信息时出错: {str(e)}")
                        pbar.update(1)

            # 统计成功获取的数量
            success_count = len(all_stock_values['symbol'].unique())
            self.logger.info(f"批量获取完成，成功率: {success_count}/{len(symbols)}")

            return all_stock_values

        except Exception as e:
            self.logger.error(f"批量获取股票价值指标信息失败: {str(e)}")
            raise

    def get_stock_value_all(self, max_workers=5, delay=0.5, start_date='2018-01-01', end_date=None):
        """
        获取所有A股股票的价值指标信息

        Args:
            max_workers (int, optional): 最大线程数。默认为5。
            delay (float, optional): 请求间隔时间(秒)。默认为0.5。
            start_date (str, optional): 开始日期，格式：YYYY-MM-DD，如 "2023-01-01"
            end_date (str, optional): 结束日期，格式：YYYY-MM-DD，如 "2023-12-31"

        Returns:
            pandas.DataFrame: 包含所有A股股票价值指标信息的DataFrame
        """
        try:
            # 获取所有A股股票代码
            self.logger.info("正在获取所有A股股票代码...")
            stock_codes = self.code_fetcher.get_all_stock_codes()
            self.logger.info(f"成功获取 {len(stock_codes)} 只A股股票代码")

            # 批量获取所有股票价值指标信息
            all_stock_values = self.get_stock_value_batch(
                stock_codes, max_workers=max_workers, delay=delay,
                start_date=start_date, end_date=end_date)

            return all_stock_values

        except Exception as e:
            self.logger.error(f"获取所有A股股票价值指标信息失败: {str(e)}")
            raise
