#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import akshare as ak
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import os
from utils.http_utils import retry_on_http_error
from utils.cache_utils import FileCache
from .etf_price_provider import ETFDataProvider


class ETFPriceProviderAkshare(ETFDataProvider):
    """Akshare ETF数据提供者实现"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # 初始化文件缓存
        cache_dir = os.path.join(os.path.dirname(
            os.path.dirname(__file__)), 'cache', 'akshare_etf')
        self.cache = FileCache(cache_dir)

    def _format_symbol(self, symbol: str) -> str:
        """格式化ETF代码
        :param symbol: 原始ETF代码
        :return: 格式化后的ETF代码
        """
        # 移除可能的交易所前缀
        symbol = symbol.replace('sh', '').replace('sz', '').strip()
        return symbol

    @retry_on_http_error(max_retries=3, delay=1)
    def _fetch_etf_data(self, symbol: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
        """获取ETF数据的内部方法，带有重试机制
        :param symbol: ETF代码
        :param start_date: 开始日期
        :param end_date: 结束日期
        :param adjust: 复权类型
        :return: ETF数据DataFrame
        """
        try:
            return ak.fund_etf_hist_em(symbol=symbol,
                                       start_date=start_date,
                                       end_date=end_date,
                                       adjust=adjust)
        except requests.exceptions.SSLError as e:
            self.logger.error(f"SSL证书验证失败: {str(e)}")
            raise
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"连接失败 (可能是443端口问题): {str(e)}")
            raise

    def get_price_data(self, symbol: str, start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> Dict[str, Any]:
        try:
            # 格式化ETF代码
            formatted_symbol = self._format_symbol(symbol)

            # 设置默认日期范围
            if not start_date:
                start_date_obj = datetime.now() - timedelta(days=30)
                start_date = start_date_obj.strftime('%Y-%m-%d')
            else:
                start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')

            if not end_date:
                end_date_obj = datetime.now()
                end_date = end_date_obj.strftime('%Y-%m-%d')
            else:
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')

            # 转换日期格式为YYYYMMDD
            start_date_fmt = start_date_obj.strftime('%Y%m%d')
            end_date_fmt = end_date_obj.strftime('%Y%m%d')

            # 列名映射字典
            column_mapping = {
                '日期': 'dt',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'change_percent',
                '涨跌额': 'change_amount',
                '换手率': 'turnover_rate'
            }

            # 获取无复权数据
            none_qfq = self._fetch_etf_data(
                formatted_symbol, start_date_fmt, end_date_fmt, '')
            none_qfq.rename(columns=column_mapping, inplace=True)
            none_qfq['adjust_type'] = 'none'

            # 获取后复权数据
            df_hfq = self._fetch_etf_data(
                formatted_symbol, start_date_fmt, end_date_fmt, 'hfq')
            df_hfq.rename(columns=column_mapping, inplace=True)
            df_hfq['adjust_type'] = 'hfq'

            # 合并数据
            result_df = pd.concat([none_qfq, df_hfq], ignore_index=True)

            # 添加ETF代码列
            result_df['symbol'] = formatted_symbol

            return result_df

        except Exception as e:
            self.logger.error(f'获取ETF {symbol} 价格数据时发生错误: {str(e)}')
            raise

    @retry_on_http_error(max_retries=3, delay=1)
    def get_etf_info(self, symbol: str) -> Dict[str, Any]:
        """获取ETF基本信息
        :param symbol: ETF代码
        :return: ETF基本信息DataFrame
        """
        try:
            # 格式化ETF代码
            formatted_symbol = self._format_symbol(symbol)

            # 获取ETF基本信息
            # 注意：这里使用fund_etf_spot_em获取ETF实时信息作为基本信息
            info = ak.fund_etf_spot_em()
            etf_info = info[info['代码'] == formatted_symbol]

            # 中文列名到英文列名的映射
            column_mapping = {
                '代码': 'symbol',
                '名称': 'name',
                '最新价': 'current_price',
                '涨跌幅': 'change_percent',
                '成交量': 'volume',
                '成交额': 'amount',
                '开盘价': 'open',
                '最高价': 'high',
                '最低价': 'low',
                '昨收': 'pre_close'
            }

            # 重命名列
            etf_info = etf_info.rename(columns=column_mapping)

            # 添加更新时间
            etf_info['update_date'] = datetime.now().strftime('%Y-%m-%d')

            return etf_info

        except Exception as e:
            self.logger.error(f'获取ETF {symbol} 基本信息时发生错误: {str(e)}')
            raise

    @retry_on_http_error(max_retries=3, delay=1)
    def get_all_etf_codes(self) -> List[str]:
        """获取所有ETF代码
        :return: ETF代码列表
        """
        # 尝试从缓存获取数据
        cache_key = 'all_etf_codes'
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            self.logger.debug('使用缓存的ETF代码列表')
            return cached_data

        try:
            # 使用akshare获取所有ETF列表
            etf_info = ak.fund_etf_spot_em()
            # 提取ETF代码列表
            etf_codes = etf_info['代码'].tolist()

            # 更新缓存（设置24小时过期）
            self.cache.set(cache_key, etf_codes, ttl=86400)

            self.logger.info(f'成功获取所有ETF代码，共 {len(etf_codes)} 个')
            return etf_codes

        except Exception as e:
            self.logger.error(f'获取ETF代码列表时发生错误: {str(e)}')
            raise
