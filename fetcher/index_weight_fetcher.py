#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import pandas as pd
import akshare as ak
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from tqdm import tqdm
# 移除 FileCache 导入


class IndexWeightFetcher:
    """
    指数权重获取器
    用于获取各类指数的成分股权重信息
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # 移除缓存相关初始化代码

    def get_index_weight(self, symbol):
        """
        获取中证指数成分股权重数据

        Args:
            symbol (str): 指数代码，如 "000300" 表示沪深300

        Returns:
            pandas.DataFrame: 包含指数成分股权重的DataFrame
        """
        # 移除缓存相关代码

        try:
            self.logger.info(f"正在获取 {symbol} 指数权重数据...")
            weight_data = ak.index_stock_cons_weight_csindex(symbol=symbol)

            # 定义中英文列名映射
            column_mapping = {
                '成分券代码': 'stock_code',
                '成分券名称': 'stock_name',
                '权重(%)': 'weight',
                '纳入日期': 'inclusion_date',
                '纳入市值': 'inclusion_market_value',
                '交易所': 'exchange'
            }

            # 重命名列名
            if not weight_data.empty:
                existing_columns = set(weight_data.columns)
                mapping = {k: v for k, v in column_mapping.items()
                           if k in existing_columns}
                weight_data = weight_data.rename(columns=mapping)

                # 添加指数代码列
                weight_data['index_code'] = symbol

            self.logger.info(f"成功获取 {symbol} 指数权重数据，共 {len(weight_data)} 条记录")

            # 移除缓存存储代码

            return weight_data
        except Exception as e:
            self.logger.error(f"获取 {symbol} 指数权重数据失败: {str(e)}")
            return pd.DataFrame()

    def get_multiple_index_weights(self, index_list, max_workers=2, delay=0.5):
        """
        批量获取多个中证指数的成分股权重数据

        Args:
            index_list (list): 指数代码列表
            max_workers (int, optional): 最大线程数。默认为2。
            delay (float, optional): 请求间隔时间(秒)。默认为0.5。

        Returns:
            pandas.DataFrame: 包含所有指数成分股权重的合并DataFrame
        """
        try:
            self.logger.info(f"开始批量获取 {len(index_list)} 个指数的权重数据...")

            # 创建一个空的DataFrame用于存储结果
            all_weights = pd.DataFrame()

            # 使用线程池并行获取权重数据
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_index = {}
                for index_code in index_list:
                    def get_weights_with_delay(code=index_code):
                        time.sleep(delay)
                        return self.get_index_weight(code)

                    future_to_index[executor.submit(
                        get_weights_with_delay)] = index_code

                # 使用tqdm创建进度条
                with tqdm(total=len(index_list), desc="获取中证指数权重") as pbar:
                    # 处理结果
                    for future in as_completed(future_to_index):
                        index_code = future_to_index[future]
                        try:
                            weights = future.result()
                            if not weights.empty:
                                all_weights = pd.concat(
                                    [all_weights, weights], ignore_index=True)
                                self.logger.debug(
                                    f"成功获取指数 {index_code} 的权重数据，共 {len(weights)} 条记录")
                            else:
                                self.logger.warning(
                                    f"指数 {index_code} 没有权重数据或获取失败")
                        except Exception as e:
                            self.logger.error(
                                f"处理指数 {index_code} 的权重数据时出错: {str(e)}")
                        pbar.update(1)

            # 统计成功获取的数量
            success_count = len(
                set(all_weights['index_code'])) if not all_weights.empty else 0
            self.logger.info(f"批量获取完成，成功率: {success_count}/{len(index_list)}")
            self.logger.info(f"成功获取所有指数权重数据，共 {len(all_weights)} 条记录")
            return all_weights

        except Exception as e:
            self.logger.error(f"批量获取指数权重数据失败: {str(e)}")
            raise
