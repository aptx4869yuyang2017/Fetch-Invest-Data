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


class SWIndexFetcher:
    """
    申万行业指数获取器
    用于获取申万行业指数信息和成分股
    """

    def __init__(self, cache_dir=None):
        self.logger = logging.getLogger(__name__)
        # 设置默认缓存目录
        if cache_dir is None:
            cache_dir = os.path.join(os.path.dirname(os.path.dirname(
                os.path.abspath(__file__))), 'cache', 'sw_index')
        os.makedirs(cache_dir, exist_ok=True)
        self.cache = FileCache(cache_dir)
        self.default_ttl = 86400  # 默认缓存时间为1天

    def get_sw_level3_codes(self, use_cache=True):
        """
        获取所有申万三级行业代码

        Args:
            use_cache (bool, optional): 是否使用缓存。默认为True。

        Returns:
            list: 包含所有申万三级行业代码的列表
        """
        cache_key = "sw_index_third_codes"

        # 如果使用缓存且缓存存在，则直接返回缓存数据
        if use_cache:
            cached_data = self.cache.get(cache_key)
            if cached_data is not None:
                self.logger.info("使用缓存的申万三级行业代码")
                return cached_data

        try:
            self.logger.info("正在获取申万三级行业代码...")
            sw_index_info = ak.sw_index_third_info()
            index_codes = sw_index_info['行业代码'].tolist()
            self.logger.info(f"成功获取申万三级行业代码，共 {len(index_codes)} 个行业")

            # 将数据存入缓存
            if use_cache:
                self.cache.set(cache_key, index_codes, ttl=self.default_ttl)

            return index_codes
        except Exception as e:
            self.logger.error(f"获取申万三级行业代码失败: {str(e)}")
            raise

    def get_sw_level3_info(self):
        """
        获取所有申万三级行业信息

        Returns:
            pandas.DataFrame: 包含所有申万三级行业信息的DataFrame，包括行业代码、名称、成分股数量、估值指标等
        """
        try:
            self.logger.info("正在获取申万三级行业信息...")
            sw_index_info = ak.sw_index_third_info()

            # 定义中英文列名映射
            column_mapping = {
                '行业代码': 'level_3_code',           # 行业代码 (object)
                '行业名称': 'level_3_name',           # 行业名称 (object)
                '上级行业': 'level_2_name',
                '成份个数': 'stock_count',    # 成份个数 (int64)
                '静态市盈率': 'pe_ratio',           # 静态市盈率 (float64)
                'TTM(滚动)市盈率': 'pe_ratio_ttm',  # TTM(滚动)市盈率 (float64)
                '市净率': 'pb_ratio',              # 市净率 (float64)
                '静态股息率': 'dividend_yield',     # 静态股息率 (float64)
            }

            # 重命名列名
            if not sw_index_info.empty:
                existing_columns = set(sw_index_info.columns)
                mapping = {k: v for k, v in column_mapping.items()
                           if k in existing_columns}
                sw_index_info = sw_index_info.rename(columns=mapping)

            self.logger.info(f"成功获取申万三级行业信息，共 {len(sw_index_info)} 个行业")
            return sw_index_info
        except Exception as e:
            self.logger.error(f"获取申万三级行业信息失败: {str(e)}")
            raise

    def get_sw_level1_info(self, use_cache=True):
        """
        获取所有申万一级行业信息

        Args:
            use_cache (bool, optional): 是否使用缓存。默认为True。

        Returns:
            pandas.DataFrame: 包含所有申万一级行业信息的DataFrame，包括行业代码、名称、成分股数量、估值指标等
        """
        cache_key = "sw_index_first_info"

        # 如果使用缓存且缓存存在，则直接返回缓存数据
        if use_cache:
            cached_data = self.cache.get(cache_key)
            if cached_data is not None:
                self.logger.info("使用缓存的申万一级行业信息")
                # 将缓存的字典数据转换回DataFrame
                return pd.DataFrame(cached_data)

        try:
            self.logger.info("正在获取申万一级行业信息...")
            sw_index_info = ak.sw_index_first_info()

            # 定义中英文列名映射
            column_mapping = {
                '行业代码': 'level_1_code',           # 行业代码 (object)
                '行业名称': 'level_1_name',           # 行业名称 (object)
                '成份个数': 'stock_count',           # 成份个数 (int64)
                '静态市盈率': 'pe_ratio',            # 静态市盈率 (float64)
                'TTM(滚动)市盈率': 'pe_ratio_ttm',   # TTM(滚动)市盈率 (float64)
                '市净率': 'pb_ratio',               # 市净率 (float64)
                '静态股息率': 'dividend_yield',      # 静态股息率 (float64)
            }

            # 重命名列名
            if not sw_index_info.empty:
                existing_columns = set(sw_index_info.columns)
                mapping = {k: v for k, v in column_mapping.items()
                           if k in existing_columns}
                sw_index_info = sw_index_info.rename(columns=mapping)

            self.logger.info(f"成功获取申万一级行业信息，共 {len(sw_index_info)} 个行业")

            # 将数据存入缓存，转换为可序列化的字典格式
            if use_cache:
                self.cache.set(cache_key, sw_index_info.to_dict(
                    'records'), ttl=self.default_ttl)

            return sw_index_info
        except Exception as e:
            self.logger.error(f"获取申万一级行业信息失败: {str(e)}")
            raise

    def get_sw_level2_info(self, use_cache=True):
        """
        获取所有申万二级行业信息

        Args:
            use_cache (bool, optional): 是否使用缓存。默认为True。

        Returns:
            pandas.DataFrame: 包含所有申万二级行业信息的DataFrame，包括行业代码、名称、成分股数量、估值指标等
        """
        cache_key = "sw_index_second_info"

        # 如果使用缓存且缓存存在，则直接返回缓存数据
        if use_cache:
            cached_data = self.cache.get(cache_key)
            if cached_data is not None:
                self.logger.info("使用缓存的申万二级行业信息")
                # 将缓存的字典数据转换回DataFrame
                return pd.DataFrame(cached_data)

        try:
            self.logger.info("正在获取申万二级行业信息...")
            sw_index_info = ak.sw_index_second_info()

            # 定义中英文列名映射
            column_mapping = {
                '行业代码': 'level_2_code',           # 行业代码 (object)
                '行业名称': 'level_2_name',           # 行业名称 (object)
                '上级行业': 'level_1_name',           # 上级行业 (object)
                '成份个数': 'stock_count',           # 成份个数 (int64)
                '静态市盈率': 'pe_ratio',            # 静态市盈率 (float64)
                'TTM(滚动)市盈率': 'pe_ratio_ttm',   # TTM(滚动)市盈率 (float64)
                '市净率': 'pb_ratio',               # 市净率 (float64)
                '静态股息率': 'dividend_yield',      # 静态股息率 (float64)
            }

            # 重命名列名
            if not sw_index_info.empty:
                existing_columns = set(sw_index_info.columns)
                mapping = {k: v for k, v in column_mapping.items()
                           if k in existing_columns}
                sw_index_info = sw_index_info.rename(columns=mapping)

            self.logger.info(f"成功获取申万二级行业信息，共 {len(sw_index_info)} 个行业")

            # 将数据存入缓存，转换为可序列化的字典格式
            if use_cache:
                self.cache.set(cache_key, sw_index_info.to_dict(
                    'records'), ttl=self.default_ttl)

            return sw_index_info
        except Exception as e:
            self.logger.error(f"获取申万二级行业信息失败: {str(e)}")
            raise

    def get_sw_stock_info(self, symbol):
        """
        获取指定申万三级行业的成分股

        Args:
            index_code (str): 申万三级行业代码

        Returns:
            pandas.DataFrame: 包含行业成分股的DataFrame，列名为英文
        """
        try:
            self.logger.debug(f"正在获取申万三级行业 {symbol} 的成分股...")
            constituents = ak.sw_index_third_cons(symbol=symbol)

            # 定义中英文列名映射
            column_mapping = {
                '序号': 'id',
                '股票代码': 'stock_code',
                '股票简称': 'stock_name',
                '纳入时间': 'inclusion_date',
                '申万3级': 'level_3_name',
                '申万3级行业代码': 'level_3_code',
                '价格': 'price',
                '市盈率': 'pe_ratio',
                '市盈率ttm': 'pe_ratio_ttm',
                '市净率': 'pb_ratio',
                '股息率': 'dividend_yield',  # 单位: %
                '市值': 'market_value',  # 单位: 亿元
                '归母净利润同比增长(09-30)': 'net_profit_yoy_growth_q3',  # 单位: %
                '归母净利润同比增长(06-30)': 'net_profit_yoy_growth_q2',  # 单位: %
                '营业收入同比增长(09-30)': 'revenue_yoy_growth_q3',  # 单位: %
                '营业收入同比增长(06-30)': 'revenue_yoy_growth_q2',  # 单位: %
            }

            # 重命名列名
            if not constituents.empty:
                # 只重命名存在的列
                existing_columns = set(constituents.columns)
                mapping = {k: v for k, v in column_mapping.items()
                           if k in existing_columns}
                constituents = constituents.rename(columns=mapping)

                # 如果数据中没有三级行业代码列，则添加该列并填充当前symbol值
                if 'level_3_code' not in constituents.columns:
                    constituents['level_3_code'] = symbol

                # 显式移除申万1级和申万2级列
                if '申万1级' in constituents.columns:
                    constituents = constituents.drop(columns=['申万1级'])
                if '申万2级' in constituents.columns:
                    constituents = constituents.drop(columns=['申万2级'])

                # 将level_3_code放到第一列
                cols = constituents.columns.tolist()
                if 'level_3_code' in cols:
                    cols.remove('level_3_code')
                    cols = ['level_3_code'] + cols
                    constituents = constituents[cols]
                
                # 添加symbol列，提取stock_code的数值部分
                if 'stock_code' in constituents.columns:
                    constituents['symbol'] = constituents['stock_code'].str.extract(r'(\d+)')

            return constituents
        except Exception as e:
            self.logger.error(f"获取申万三级行业 {symbol} 的成分股失败: {str(e)}")
            return pd.DataFrame()

    def get_all_sw_stock_info(self, index_codes, max_workers=2, delay=0.5):
        try:
            self.logger.info(f"开始获取 {len(index_codes)} 个申万三级行业的成分股...")

            # 创建一个空的DataFrame用于存储结果
            all_constituents = pd.DataFrame()

            # 使用线程池并行获取成分股
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_index = {}
                for index_code in index_codes:
                    def get_constituents_with_delay(code=index_code):

                        time.sleep(delay)
                        return self.get_sw_stock_info(code)

                    future_to_index[executor.submit(
                        get_constituents_with_delay)] = index_code

                # 使用tqdm创建进度条
                with tqdm(total=len(index_codes), desc="获取申万行业成分股") as pbar:
                    # 处理结果
                    for future in as_completed(future_to_index):
                        index_code = future_to_index[future]
                        try:
                            constituents = future.result()
                            if not constituents.empty:
                                all_constituents = pd.concat(
                                    [all_constituents, constituents], ignore_index=True)
                                self.logger.debug(
                                    f"成功获取行业 {index_code} 的成分股，共 {len(constituents)} 个")
                            else:
                                self.logger.warning(
                                    f"行业 {index_code} 没有成分股或获取失败")
                        except Exception as e:
                            self.logger.error(
                                f"处理行业 {index_code} 的成分股时出错: {str(e)}")
                        pbar.update(1)

            # 统计成功获取的数量
            success_count = len(set(all_constituents['level_3_code']))
            self.logger.info(f"批量获取完成，成功率: {success_count}/{len(index_codes)}")
            self.logger.info(f"成功获取所有申万三级行业成分股，共 {len(all_constituents)} 条记录")
            return all_constituents

        except Exception as e:
            self.logger.error(f"获取所有申万三级行业成分股失败: {str(e)}")
            raise
