#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import pandas as pd
from unittest.mock import patch
from fetcher.macro_data_china_fetcher import MacroDataChinaFetcher


class TestMacroDataChinaFetcher(unittest.TestCase):
    """测试宏观经济数据获取器"""

    def setUp(self):
        """测试前的准备工作"""
        self.fetcher = MacroDataChinaFetcher()

    @patch('akshare.macro_china_gdp')
    def test_fetch_gdp_monthly(self, mock_gdp):
        """测试获取中国GDP月度数据"""
        # 创建模拟的GDP数据，包含2023年和2024年的数据
        mock_data = {
            '季度': [
                '2024年第1-4季度', '2024年第1-3季度', '2024年第1-2季度', '2024年第1季度',
                '2023年第1-4季度', '2023年第1-3季度', '2023年第1-2季度', '2023年第1季度'
            ],
            '国内生产总值-绝对值': [
                1349083.5, 975357.4, 633599.4, 304761.8,
                1294271.7, 937047.0, 608606.3, 292368.8
            ],
            '国内生产总值-同比增长': [
                5.0, 4.8, 5.0, 5.3,
                5.2, 5.2, 5.5, 4.5
            ],
            '第一产业-绝对值': [
                91413.9, 57363.6, 30468.8, 11458.8,
                89169.1, 55955.6, 30195.9, 11500.4
            ],
            '第一产业-同比增长': [
                3.5, 3.4, 3.5, 3.3,
                4.1, 4.0, 3.7, 3.7
            ],
            '第二产业-绝对值': [
                492087.1, 356492.0, 233253.0, 108277.4,
                475936.1, 344190.1, 224508.4, 104579.5
            ],
            '第二产业-同比增长': [
                5.3, 5.4, 5.8, 6.0,
                4.7, 4.4, 4.3, 3.3
            ],
            '第三产业-绝对值': [
                765582.5, 561501.8, 369877.5, 185025.6,
                729166.5, 536901.2, 353902.0, 176288.8
            ],
            '第三产业-同比增长': [
                5.0, 4.7, 4.6, 5.0,
                5.8, 6.0, 6.4, 5.4
            ]
        }
        mock_df = pd.DataFrame(mock_data)
        mock_gdp.return_value = mock_df

        # 调用被测试的方法
        result = self.fetcher.fetch_gdp_monthly(start_year='2023')

        # 只筛选2024年的数据进行验证
        result_2024 = result[result['year'] == '2024']

        # 验证结果
        self.assertIsInstance(result_2024, pd.DataFrame)
        self.assertFalse(result_2024.empty)

        # 验证数据结构
        expected_columns = ['date', 'year', 'month',
                            'monthly_gdp', 'monthly_gdp_yoy']
        for col in expected_columns:
            self.assertIn(col, result_2024.columns)

        # 验证2024年数据
        self.assertEqual(len(result_2024), 12)  # 应该有12个月的数据

        # 验证第一季度数据
        q1_data = result_2024[result_2024['month'].isin([1, 2, 3])]
        self.assertEqual(len(q1_data), 3)
        for _, row in q1_data.iterrows():
            self.assertAlmostEqual(row['monthly_gdp'], 304761.8 / 3, delta=0.1)
            self.assertAlmostEqual(row['monthly_gdp_yoy'], 4.2, delta=0.1)

        # 验证第二季度数据
        q2_data = result_2024[result_2024['month'].isin([4, 5, 6])]
        self.assertEqual(len(q2_data), 3)
        for _, row in q2_data.iterrows():
            self.assertAlmostEqual(
                row['monthly_gdp'], (633599.4 - 304761.8) / 3, delta=0.1)

        # 验证第三季度数据
        q3_data = result_2024[result_2024['month'].isin([7, 8, 9])]
        self.assertEqual(len(q3_data), 3)
        for _, row in q3_data.iterrows():
            self.assertAlmostEqual(
                row['monthly_gdp'], (975357.4 - 633599.4) / 3, delta=0.1)

        # 验证第四季度数据
        q4_data = result_2024[result_2024['month'].isin([10, 11, 12])]
        self.assertEqual(len(q4_data), 3)
        for _, row in q4_data.iterrows():
            self.assertAlmostEqual(
                row['monthly_gdp'], (1349083.5 - 975357.4) / 3, delta=0.1)


if __name__ == '__main__':
    unittest.main()
