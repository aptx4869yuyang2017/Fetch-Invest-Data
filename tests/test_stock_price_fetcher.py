#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
from fetcher.stock_price_fetcher import StockDataProvider, AkshareProvider, StockPriceFetcher
import pandas as pd


class TestStockDataProvider(unittest.TestCase):
    """测试StockDataProvider抽象类的实现"""

    def test_abstract_methods(self):
        """测试抽象方法是否被正确定义"""
        with self.assertRaises(TypeError):
            StockDataProvider()


class TestAkshareProvider(unittest.TestCase):
    """测试AkshareProvider类的功能"""

    def setUp(self):
        self.provider = AkshareProvider()
        self.test_symbol = 'sh600000'

    def test_format_symbol(self):
        """测试股票代码格式化"""
        self.assertEqual(self.provider._format_symbol('sh600000'), '600000')
        self.assertEqual(self.provider._format_symbol('sz000001'), '000001')
        self.assertEqual(self.provider._format_symbol('600000'), '600000')

    @patch('akshare.stock_zh_a_hist')
    def test_get_price_data(self, mock_stock_hist):
        """测试获取股票价格数据"""
        # 模拟akshare返回数据
        mock_df = pd.DataFrame({
            'Date': ['2023-01-01'],
            'Open': [10.0],
            'High': [11.0],
            'Low': [9.0],
            'Close': [10.5],
            'Volume': [1000000]
        })
        mock_stock_hist.return_value = mock_df

        # 测试正常情况
        result = self.provider.get_price_data(self.test_symbol)
        self.assertEqual(result['symbol'], '600000')
        self.assertIn('prices', result)
        self.assertIn('start_date', result)
        self.assertIn('end_date', result)

        # 测试异常情况
        mock_stock_hist.side_effect = Exception('API错误')
        with self.assertRaises(Exception):
            self.provider.get_price_data(self.test_symbol)

    @patch('akshare.stock_individual_info_em')
    def test_get_company_info(self, mock_company_info):
        """测试获取公司信息"""
        # 模拟公司信息数据
        mock_df = pd.DataFrame({
            'item': ['公司名称'],
            'value': ['测试公司']
        })
        mock_company_info.return_value = mock_df

        result = self.provider.get_company_info(self.test_symbol)
        self.assertEqual(result['symbol'], '600000')
        self.assertIn('info', result)


class TestStockFetcher(unittest.TestCase):
    """测试StockFetcher类的功能"""

    def setUp(self):
        self.mock_provider = Mock(spec=StockDataProvider)
        self.fetcher = StockPriceFetcher(provider=self.mock_provider)

    def test_init_default_provider(self):
        """测试默认提供者初始化"""
        fetcher = StockPriceFetcher()
        self.assertIsInstance(fetcher.provider, AkshareProvider)

    def test_set_provider(self):
        """测试设置数据提供者"""
        new_provider = Mock(spec=StockDataProvider)
        self.fetcher.set_provider(new_provider)
        self.assertEqual(self.fetcher.provider, new_provider)

    def test_fetch_stock_price(self):
        """测试获取单个股票价格数据"""
        test_data = {'symbol': '600000', 'prices': []}
        self.mock_provider.get_price_data.return_value = test_data

        result = self.fetcher.fetch_stock_price('600000')
        self.mock_provider.get_price_data.assert_called_once()
        self.assertEqual(result, test_data)

    def test_fetch_company_info(self):
        """测试获取公司信息"""
        test_data = {'symbol': '600000', 'info': {}}
        self.mock_provider.get_company_info.return_value = test_data

        result = self.fetcher.fetch_company_info('600000')
        self.mock_provider.get_company_info.assert_called_once()
        self.assertEqual(result, test_data)

    def test_fetch_multiple_stocks(self):
        """测试批量获取股票数据"""
        test_symbols = ['600000', '600001']
        test_data = {'symbol': '600000', 'prices': []}
        self.mock_provider.get_price_data.return_value = test_data

        results = self.fetcher.fetch_multiple_stocks(test_symbols)
        self.assertEqual(len(results), 2)
        self.assertEqual(self.mock_provider.get_price_data.call_count, 2)

    def test_fetch_multiple_stocks_with_error(self):
        """测试批量获取时的错误处理"""
        test_symbols = ['600000', '600001']
        self.mock_provider.get_price_data.side_effect = [
            Exception('测试错误'), {'symbol': '600001', 'prices': []}]

        results = self.fetcher.fetch_multiple_stocks(test_symbols)
        self.assertEqual(len(results), 1)


if __name__ == '__main__':
    unittest.main()
