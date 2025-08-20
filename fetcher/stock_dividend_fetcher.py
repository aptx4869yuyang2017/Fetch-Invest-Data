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


class StockDividendFetcher:
    """
    A股个股分红数据获取器
    用于获取A股个股历史分红数据（使用东方财富网数据）
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.code_fetcher = StockAAllCodeFetcher()

    def get_stock_dividend(self, symbol):
        """
        获取指定股票的历史分红数据

        Args:
            symbol (str): 股票代码，如 "000001"

        Returns:
            pandas.DataFrame: 包含股票历史分红数据的DataFrame
        """
        try:
            self.logger.debug(f"正在获取股票 {symbol} 的历史分红数据...")
            # 使用akshare获取分红数据
            stock_dividend = ak.stock_history_dividend_detail(
                symbol=symbol, indicator="分红")

            if not stock_dividend.empty:
                # 重命名列
                column_map = {
                    '派息': 'dividend',
                    '进度': 'progress',
                    '公告日期': 'announcement_date',
                    '除权除息日': 'ex_date',
                    '股权登记日': 'record_date',
                    '红股上市日': 'dividend_share_listing_date'
                }
                stock_dividend = stock_dividend.rename(columns=column_map)

                # 只保留进度为"实施"的数据
                stock_dividend = stock_dividend[stock_dividend['progress'] == '实施']

                # 只保留指定的列，不包含 progress
                columns_to_keep = ['dividend', 'ex_date',
                                   'record_date', 'dividend_share_listing_date', 'announcement_date']
                stock_dividend = stock_dividend[columns_to_keep]

                # 添加date列，使用股权登记日数据
                stock_dividend['date'] = stock_dividend['record_date']

                # 添加symbol列
                stock_dividend['symbol'] = symbol
                self.logger.debug(f"成功获取股票 {symbol} 的历史分红数据")
                return stock_dividend
            else:
                return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"获取股票 {symbol} 的历史分红数据失败: {str(e)}")
            return pd.DataFrame()

    def get_stock_dividend_batch(self, symbols, max_workers=2, delay=0.5):
        """
        批量获取多只股票的历史分红数据

        Args:
            symbols (list): 股票代码列表
            max_workers (int, optional): 最大线程数。默认为2。
            delay (float, optional): 请求间隔时间(秒)。默认为0.5。

        Returns:
            pandas.DataFrame: 包含所有股票历史分红数据的DataFrame
        """
        try:
            self.logger.info(f"开始批量获取 {len(symbols)} 只股票的历史分红数据...")

            # 创建一个空的DataFrame用于存储结果
            all_stock_dividends = pd.DataFrame()

            # 使用线程池并行获取股票信息
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_symbol = {}
                for symbol in symbols:
                    def get_stock_dividend_with_delay(code=symbol):
                        time.sleep(delay)
                        return self.get_stock_dividend(code)

                    future_to_symbol[executor.submit(
                        get_stock_dividend_with_delay)] = symbol

                # 使用tqdm创建进度条
                with tqdm(total=len(symbols), desc="获取股票历史分红数据") as pbar:
                    # 处理结果
                    for future in as_completed(future_to_symbol):
                        symbol = future_to_symbol[future]
                        try:
                            stock_dividend = future.result()
                            if not stock_dividend.empty:
                                all_stock_dividends = pd.concat(
                                    [all_stock_dividends, stock_dividend], ignore_index=True)
                                self.logger.debug(
                                    f"成功获取股票 {symbol} 的历史分红数据")
                            else:
                                self.logger.warning(
                                    f"股票 {symbol} 获取历史分红数据返回空结果")
                        except Exception as e:
                            self.logger.error(
                                f"处理股票 {symbol} 的历史分红数据时出错: {str(e)}")
                        pbar.update(1)

            # 统计成功获取的数量
            success_count = len(all_stock_dividends['symbol'].unique())
            self.logger.info(f"批量获取完成，成功率: {success_count}/{len(symbols)}")

            return all_stock_dividends

        except Exception as e:
            self.logger.error(f"批量获取股票历史分红数据失败: {str(e)}")
            raise

    def get_stock_dividend_all(self, max_workers=5, delay=2):
        """
        获取所有A股股票的历史分红数据

        Args:
            max_workers (int, optional): 最大线程数。默认为5。
            delay (float, optional): 请求间隔时间(秒)。默认为0.5。

        Returns:
            pandas.DataFrame: 包含所有A股股票历史分红数据的DataFrame
        """
        try:
            # 获取所有A股股票代码
            self.logger.info("正在获取所有A股股票代码...")
            stock_codes = self.code_fetcher.get_all_stock_codes()
            self.logger.info(f"成功获取 {len(stock_codes)} 只A股股票代码")

            # 批量获取所有股票历史分红数据
            all_stock_dividends = self.get_stock_dividend_batch(
                stock_codes, max_workers=max_workers, delay=delay)

            return all_stock_dividends

        except Exception as e:
            self.logger.error(f"获取所有A股股票历史分红数据失败: {str(e)}")
            raise
