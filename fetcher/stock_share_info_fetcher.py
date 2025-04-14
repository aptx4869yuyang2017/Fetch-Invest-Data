#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import pandas as pd
import akshare as ak
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from tqdm import tqdm
from utils.stock_utils import get_full_symbol


class StockShareInfoFetcher:
    """
    A股个股股本结构获取器
    用于获取A股个股股本结构信息
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def get_stock_share_info(self, symbol):
        """
        获取指定股票的股本结构信息

        Args:
            symbol (str): 股票代码，如 "000568"

        Returns:
            pandas.DataFrame: 包含股票股本结构信息的DataFrame
        """
        try:
            self.logger.debug(f"正在获取股票 {symbol} 的股本结构信息...")
            # 使用akshare获取股本结构信息
            full_symbol = get_full_symbol(symbol, type='suffix')
            stock_capital = ak.stock_zh_a_gbjg_em(symbol=full_symbol)

            if not stock_capital.empty:
                # 添加symbol列
                stock_capital['symbol'] = symbol

                # 将列名从中文转换为英文
                column_mapping = {
                    '变更日期': 'change_date',
                    '总股本': 'total_share',
                    '流通受限股份': 'share_restricted',
                    '其他内资持股(受限)': 'other_domestic_restricted',
                    '境内法人持股(受限)': 'domestic_legal_person_restricted',
                    '境内自然人持股(受限)': 'domestic_natural_person_restricted',
                    '已流通股份': 'share_circulating',
                    '已上市流通A股': 'share_circulating_a',
                    '变动原因': 'change_reason'
                }

                # 重命名列
                stock_capital = stock_capital.rename(columns=column_mapping)

                self.logger.debug(f"成功获取股票 {symbol} 的股本结构信息")

                return stock_capital
            else:
                self.logger.warning(f"获取股票 {symbol} 的股本结构信息返回空结果")
                return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"获取股票 {symbol} 的股本结构信息失败: {str(e)}")
            return pd.DataFrame()

    def get_stock_share_info_batch(self, symbols, max_workers=2, delay=0.5):
        """
        批量获取多只股票的股本结构信息

        Args:
            symbols (list): 股票代码列表
            max_workers (int, optional): 最大线程数。默认为2。
            delay (float, optional): 请求间隔时间(秒)。默认为0.5。

        Returns:
            pandas.DataFrame: 包含所有股票股本结构信息的DataFrame
        """
        try:
            self.logger.info(f"开始批量获取 {len(symbols)} 只股票的股本结构信息...")

            # 创建一个空的DataFrame用于存储结果
            all_stock_capital = pd.DataFrame()

            # 使用线程池并行获取股票信息
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_symbol = {}
                for symbol in symbols:
                    def get_stock_capital_with_delay(code=symbol):
                        time.sleep(delay)
                        return self.get_stock_share_info(code)

                    future_to_symbol[executor.submit(
                        get_stock_capital_with_delay)] = symbol

                # 使用tqdm创建进度条
                with tqdm(total=len(symbols), desc="获取股票股本结构信息") as pbar:
                    # 处理结果
                    for future in as_completed(future_to_symbol):
                        symbol = future_to_symbol[future]
                        try:
                            stock_capital = future.result()
                            if not stock_capital.empty:
                                all_stock_capital = pd.concat(
                                    [all_stock_capital, stock_capital], ignore_index=True)
                                self.logger.debug(
                                    f"成功获取股票 {symbol} 的股本结构信息")
                            else:
                                self.logger.warning(
                                    f"股票 {symbol} 获取股本结构信息失败或返回空结果")
                        except Exception as e:
                            self.logger.error(
                                f"处理股票 {symbol} 的股本结构信息时出错: {str(e)}")
                        pbar.update(1)

            # 统计成功获取的数量
            success_count = len(all_stock_capital['symbol'].unique())
            self.logger.info(f"批量获取完成，成功率: {success_count}/{len(symbols)}")

            return all_stock_capital

        except Exception as e:
            self.logger.error(f"批量获取股票股本结构信息失败: {str(e)}")
            raise

    def get_stock_share_info_all(self, max_workers=2, delay=0.5):
        """
        获取所有A股股票的股本结构信息

        Args:
            max_workers (int, optional): 最大线程数。默认为2。
            delay (float, optional): 请求间隔时间(秒)。默认为0.5。

        Returns:
            pandas.DataFrame: 包含所有A股股票股本结构信息的DataFrame
        """
        try:
            # 获取所有股票代码
            from fetcher.stock_info_fetcher import StockInfoFetcher
            stock_info_fetcher = StockInfoFetcher()
            stock_codes = stock_info_fetcher.get_stock_list(use_cache=True)

            # 批量获取所有股票股本结构信息
            all_stock_capital = self.get_stock_share_info_batch(
                stock_codes, max_workers=max_workers, delay=delay)

            return all_stock_capital

        except Exception as e:
            self.logger.error(f"获取所有A股股票股本结构信息失败: {str(e)}")
            raise
