from utils.cache_utils import FileCache
from utils.http_utils import retry_on_http_error
import pandas as pd
from typing import List, Dict
import akshare as ak
import logging
import sys
import os


class StockGGTAllCodeFetcher:
    """港股通股票代码获取器"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # 初始化文件缓存
        cache_dir = os.path.join(os.path.dirname(
            os.path.dirname(__file__)), 'cache', 'akshare')
        self.cache = FileCache(cache_dir)

    @retry_on_http_error(max_retries=3, delay=1)
    def get_all_hk_ggt_info(self) -> List[Dict]:
        """获取所有港股通股票代码和信息
        :return: 港股通股票信息列表，每个元素为包含股票代码、名称等信息的字典
        """
        # 尝试从缓存获取数据
        cache_key = 'stock_hk_ggt_all_code_components'
        cached_data = self.cache.get(cache_key)
        if cached_data is not None:
            self.logger.debug('使用缓存的港股通股票列表')
            return cached_data

        try:
            # 使用 akshare 获取所有港股通成分股列表
            stock_hk_ggt_df = ak.stock_hk_ggt_components_em()

            # 将DataFrame转换为字典列表
            stock_info_list = stock_hk_ggt_df.to_dict('records')

            # 更新缓存（设置24小时过期）
            self.cache.set(cache_key, stock_info_list, ttl=86400)

            self.logger.info(f'成功获取所有港股通股票，共 {len(stock_info_list)} 个')
            return stock_info_list

        except Exception as e:
            self.logger.error(f'获取港股通股票列表时发生错误: {str(e)}')
            raise

    def get_all_stock_codes(self) -> List[str]:
        """仅获取所有港股通股票代码
        :return: 港股通股票代码列表
        """
        stock_info_list = self.get_all_hk_ggt_info()
        # 提取股票代码列表
        # 假设股票代码在'代码'列中，根据实际情况可能需要调整
        return [item['代码'] for item in stock_info_list]


if __name__ == "__main__":
    # 设置日志级别
    logging.basicConfig(level=logging.INFO)

    # 测试获取港股通股票代码
    fetcher = StockGGTAllCodeFetcher()

    # 仅获取代码
    codes = fetcher.get_all_stock_codes()
    print(f"获取到 {len(codes)} 个港股通股票代码")
    print(f"前五个代码：{codes[:5]}")
