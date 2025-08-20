#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from fetcher.stock_a_price_provider_akshare import StockPriceProviderAkshare
import unittest
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))


class TestAkshareProvider(unittest.TestCase):
    """AkshareProvider 类的单元测试"""

    def setUp(self):
        """测试前的准备工作"""
        self.provider = StockPriceProviderAkshare()
        # 设置测试使用的股票代码和日期范围
        self.test_symbol = '000001'  # 平安银行
        self.start_date = '2023-01-09'
        self.end_date = '2023-01-10'

    def test_get_price_data(self):
        """测试 get_price_data 方法，使用真实数据"""
        # 直接调用方法获取真实数据
        result = self.provider.get_price_data(
            self.test_symbol, self.start_date, self.end_date)

        # 测试1: 检查返回的列是否符合预期

        expected_columns = {'symbol', 'dt', 'open', 'close', 'high', 'low',
                            'volume', 'amount', 'amplitude', 'change_percent',
                            'change_amount', 'turnover_rate', 'adjust_type'}
        self.assertEqual(set(result.columns.tolist()), expected_columns)

        # 测试2: 检查返回的数据行数是否符合预期
        # 应该有两种复权类型，每种2行数据，总共4行
        self.assertEqual(len(result), 4, "返回的数据行数应该等于4")

        # 检查是否包含后复权和不复权两种类型
        adjust_types = result['adjust_type'].unique()
        self.assertIn('hfq', adjust_types, "数据中应包含后复权(hfq)类型")
        self.assertIn('none', adjust_types, "数据中应包含不复权(none)类型")

        # 测试3: 检查返回的具体某日的数据是否符合预期
        # 根据实际返回的数据进行精确测试，只测试2023-01-09的数据

        # 检查后复权数据
        hfq_data = result[result['adjust_type'] == 'hfq']
        self.assertEqual(len(hfq_data), 2, "后复权数据应该有2行")

        # 检查2023-01-09的后复权数据

        # 使用更灵活的方式匹配日期，处理可能的格式差异
        hfq_20230109_data = hfq_data[hfq_data['dt'].astype(
            str).str.contains('2023-01-09')]
        self.assertFalse(hfq_20230109_data.empty, "应该存在2023-01-09的后复权数据")
        hfq_20230109 = hfq_20230109_data.iloc[0]
        # self.assertAlmostEqual(hfq_20230109['open'], 2603.19, delta=0.01)
        # self.assertAlmostEqual(hfq_20230109['close'], 2611.31, delta=0.01)
        # self.assertAlmostEqual(hfq_20230109['high'], 2624.31, delta=0.01)
        # self.assertAlmostEqual(hfq_20230109['low'], 2565.8, delta=0.01)
        self.assertEqual(hfq_20230109['volume'], 1057659.0)
        self.assertAlmostEqual(
            hfq_20230109['amount'], 1.5613684873E9, delta=1E7)
        self.assertAlmostEqual(hfq_20230109['amplitude'], 2.27, delta=0.01)
        self.assertAlmostEqual(
            hfq_20230109['change_percent'], 1.13, delta=0.01)
        # self.assertAlmostEqual(
        #     hfq_20230109['change_amount'], 29.25, delta=0.01)
        self.assertAlmostEqual(hfq_20230109['turnover_rate'], 0.55, delta=0.01)

        # 检查不复权数据
        none_data = result[result['adjust_type'] == 'none']
        self.assertEqual(len(none_data), 2, "不复权数据应该有2行")

        # 检查2023-01-09的不复权数据
        # 使用相同的灵活匹配方式
        none_20230109_data = none_data[none_data['dt'].astype(
            str).str.contains('2023-01-09')]
        self.assertFalse(none_20230109_data.empty, "应该存在2023-01-09的不复权数据")
        none_20230109 = none_20230109_data.iloc[0]
        self.assertAlmostEqual(none_20230109['open'], 14.75, delta=0.01)
        self.assertAlmostEqual(none_20230109['close'], 14.8, delta=0.01)
        self.assertAlmostEqual(none_20230109['high'], 14.88, delta=0.01)
        self.assertAlmostEqual(none_20230109['low'], 14.52, delta=0.01)
        self.assertEqual(none_20230109['volume'], 1057659.0)
        self.assertAlmostEqual(
            none_20230109['amount'], 1.5613684873E9, delta=1E7)
        self.assertAlmostEqual(none_20230109['amplitude'], 2.46, delta=0.01)
        self.assertAlmostEqual(
            none_20230109['change_percent'], 1.23, delta=0.01)
        self.assertAlmostEqual(
            none_20230109['change_amount'], 0.18, delta=0.01)
        self.assertAlmostEqual(
            none_20230109['turnover_rate'], 0.55, delta=0.01)

        # 检查数据的合理性
        for _, row in result.iterrows():
            self.assertGreaterEqual(row['high'], row['close'], "最高价应该大于等于收盘价")
            self.assertGreaterEqual(row['high'], row['open'], "最高价应该大于等于开盘价")
            self.assertGreaterEqual(row['close'], row['low'], "收盘价应该大于等于最低价")
            self.assertGreaterEqual(row['open'], row['low'], "开盘价应该大于等于最低价")
            self.assertGreater(row['volume'], 0, "成交量应该大于0")
            self.assertGreater(row['amount'], 0, "成交额应该大于0")


if __name__ == '__main__':
    unittest.main()
