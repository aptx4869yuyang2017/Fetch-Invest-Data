#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import pandas as pd
import akshare as ak
from typing import Optional
from datetime import datetime


class MacroDataChinaFetcher:
    """宏观经济数据获取器"""

    def __init__(self):
        """初始化宏观数据获取器"""
        self.logger = logging.getLogger(__name__)

    def fetch_money_supply(self, start_year: Optional[str] = "2000") -> pd.DataFrame:
        """获取中国货币供应量数据

        Args:
            start_year: 起始年份，默认为"2000"年

        Returns:
            DataFrame包含以下字段:
            - date: 统计时间
            - m2: 货币和准货币（广义货币M2）
            - m2_yoy: M2同比增长
            - m1: 货币(狭义货币M1)
            - m1_yoy: M1同比增长
            - m0: 流通中现金(M0)
            - m0_yoy: M0同比增长
            - demand_deposit: 活期存款
            - demand_deposit_yoy: 活期存款同比增长
            - quasi_money: 准货币
            - quasi_money_yoy: 准货币同比增长
            - time_deposit: 定期存款
            - time_deposit_yoy: 定期存款同比增长
            - savings_deposit: 储蓄存款
            - savings_deposit_yoy: 储蓄存款同比增长
            - other_deposit: 其他存款
            - other_deposit_yoy: 其他存款同比增长
        """
        try:
            self.logger.info("开始获取中国货币供应量数据")

            # 获取原始数据
            df = ak.macro_china_supply_of_money()

            # 将统计时间列转换为日期类型
            df['统计时间'] = pd.to_datetime(df['统计时间'].astype(str).apply(
                lambda x: x.replace('.', '-') + '-01'))

            # 筛选指定年份之后的数据
            if start_year:
                df = df[df['统计时间'].dt.year >= int(start_year)]

            # 重命名列
            column_mapping = {
                '统计时间': 'date',
                '货币和准货币（广义货币M2）': 'm2',
                '货币和准货币（广义货币M2）同比增长': 'm2_yoy',
                '货币(狭义货币M1)': 'm1',
                '货币(狭义货币M1)同比增长': 'm1_yoy',
                '流通中现金(M0)': 'm0',
                '流通中现金(M0)同比增长': 'm0_yoy',
                '活期存款': 'demand_deposit',
                '活期存款同比增长': 'demand_deposit_yoy',
                '准货币': 'quasi_money',
                '准货币同比增长': 'quasi_money_yoy',
                '定期存款': 'time_deposit',
                '定期存款同比增长': 'time_deposit_yoy',
                '储蓄存款': 'savings_deposit',
                '储蓄存款同比增长': 'savings_deposit_yoy',
                '其他存款': 'other_deposit',
                '其他存款同比增长': 'other_deposit_yoy'
            }

            df = df.rename(columns=column_mapping)

            # 按日期降序排序
            df = df.sort_values('date', ascending=False)

            self.logger.info(f"成功获取货币供应量数据，共 {len(df)} 条记录")
            return df

        except Exception as e:
            self.logger.error(f"获取货币供应量数据失败: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def fetch_gdp_monthly(self, start_year: Optional[str] = '2007') -> pd.DataFrame:
        """获取中国GDP数据并处理为月度数据

        Args:
            start_year: 起始年份，默认为None表示获取所有可用数据

        Returns:
            DataFrame包含以下字段:
            - date: 日期
            - year: 年份
            - month: 月份
            - monthly_gdp: 月度GDP
            - monthly_gdp_yoy: 月度GDP同比增长
        """
        try:
            self.logger.info("开始获取中国GDP数据")

            # 获取原始数据
            df = ak.macro_china_gdp()

            # 处理数据：将累计数据转换为单季度数据，并拆分为月度数据
            processed_df = self._process_gdp_data(df)

            # 筛选指定年份之后的数据
            if start_year:
                processed_df = processed_df[processed_df['年份'].astype(
                    int) >= int(start_year)]

            # 重命名列
            column_mapping = {
                '日期': 'date',
                '年份': 'year',
                '月份': 'month',
                '月度GDP': 'monthly_gdp',
                '月度GDP同比增长': 'monthly_gdp_yoy'
            }

            processed_df = processed_df.rename(columns=column_mapping)

            self.logger.info(f"成功获取并处理GDP月度数据，共 {len(processed_df)} 条记录")
            return processed_df

        except Exception as e:
            self.logger.error(f"获取GDP数据失败: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def _process_gdp_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理GDP数据：将累计数据转换为单季度数据，并拆分为月度数据

        Args:
            df: 原始GDP数据DataFrame

        Returns:
            处理后的月度GDP数据DataFrame
        """
        # 复制一份数据
        processed_df = df.copy()

        # 确保数据按季度降序排列（最新的在前）
        processed_df = processed_df.sort_values('季度', ascending=False)

        # 创建新列用于存储单季度数据
        processed_df['单季度GDP'] = None

        # 处理单季度GDP数据
        for i in range(len(processed_df)):
            quarter = processed_df.iloc[i]['季度']
            gdp_value = processed_df.iloc[i]['国内生产总值-绝对值']

            # 如果是第一季度，则单季度GDP等于累计GDP
            if '第1季度' in quarter or '第1-1季度' in quarter:
                processed_df.iloc[i, processed_df.columns.get_loc(
                    '单季度GDP')] = gdp_value
            # 如果是其他季度，需要减去前一个累计值
            elif '第1-2季度' in quarter:
                # 查找第一季度的GDP
                q1_gdp = processed_df[processed_df['季度'].str.contains('第1季度') &
                                      (processed_df['季度'].str[:4] == quarter[:4])]['国内生产总值-绝对值'].values
                if len(q1_gdp) > 0:
                    processed_df.iloc[i, processed_df.columns.get_loc(
                        '单季度GDP')] = gdp_value - q1_gdp[0]
            elif '第1-3季度' in quarter:
                # 查找1-2季度的GDP
                q2_gdp = processed_df[processed_df['季度'].str.contains('第1-2季度') &
                                      (processed_df['季度'].str[:4] == quarter[:4])]['国内生产总值-绝对值'].values
                if len(q2_gdp) > 0:
                    processed_df.iloc[i, processed_df.columns.get_loc(
                        '单季度GDP')] = gdp_value - q2_gdp[0]
            elif '第1-4季度' in quarter:
                # 查找1-3季度的GDP
                q3_gdp = processed_df[processed_df['季度'].str.contains('第1-3季度') &
                                      (processed_df['季度'].str[:4] == quarter[:4])]['国内生产总值-绝对值'].values
                if len(q3_gdp) > 0:
                    processed_df.iloc[i, processed_df.columns.get_loc(
                        '单季度GDP')] = gdp_value - q3_gdp[0]

        # 计算单季度GDP同比增长率
        processed_df['单季度GDP同比增长'] = None

        # 创建一个映射，将累计季度转换为单季度标识
        quarter_mapping = {
            '第1季度': 'Q1',
            '第1-1季度': 'Q1',
            '第1-2季度': 'Q2',
            '第1-3季度': 'Q3',
            '第1-4季度': 'Q4'
        }

        # 添加年份和单季度标识列
        processed_df['年份'] = processed_df['季度'].apply(lambda x: x[:4])
        processed_df['季度标识'] = processed_df['季度'].apply(lambda x: next(
            (v for k, v in quarter_mapping.items() if k in x), None))

        # 添加日期列，使用每个季度的最后一天
        def get_quarter_end_date(year, quarter):
            if quarter == 'Q1':
                return f"{year}-03-31"
            elif quarter == 'Q2':
                return f"{year}-06-30"
            elif quarter == 'Q3':
                return f"{year}-09-30"
            elif quarter == 'Q4':
                return f"{year}-12-31"
            return None

        processed_df['日期'] = processed_df.apply(
            lambda row: get_quarter_end_date(row['年份'], row['季度标识']), axis=1)
        processed_df['日期'] = pd.to_datetime(processed_df['日期'])

        # 计算同比增长率
        for i in range(len(processed_df)):
            current_year = processed_df.iloc[i]['年份']
            current_quarter = processed_df.iloc[i]['季度标识']

            # 查找去年同期数据
            last_year = str(int(current_year) - 1)
            last_year_data = processed_df[(processed_df['年份'] == last_year) &
                                          (processed_df['季度标识'] == current_quarter)]

            if not last_year_data.empty and not pd.isna(processed_df.iloc[i]['单季度GDP']):
                current_gdp = processed_df.iloc[i]['单季度GDP']
                last_year_gdp = last_year_data['单季度GDP'].values[0]

                if not pd.isna(last_year_gdp) and last_year_gdp != 0:
                    yoy_growth = (current_gdp / last_year_gdp - 1) * 100
                    processed_df.iloc[i, processed_df.columns.get_loc(
                        '单季度GDP同比增长')] = round(yoy_growth, 1)

        # 选择需要的列
        quarterly_df = processed_df[[
            '日期', '年份', '单季度GDP', '单季度GDP同比增长']].copy()

        # 将季度数据拆分为月度数据
        monthly_data = []

        for _, row in quarterly_df.iterrows():
            quarter_date = row['日期']
            quarter = pd.Timestamp(quarter_date).quarter
            year = row['年份']

            # 计算该季度的三个月
            months = []
            if quarter == 1:
                months = [1, 2, 3]
            elif quarter == 2:
                months = [4, 5, 6]
            elif quarter == 3:
                months = [7, 8, 9]
            elif quarter == 4:
                months = [10, 11, 12]

            # 将季度GDP平均分配到三个月
            monthly_gdp = row['单季度GDP'] / \
                3 if not pd.isna(row['单季度GDP']) else None
            monthly_gdp_yoy = row['单季度GDP同比增长'] if not pd.isna(
                row['单季度GDP同比增长']) else None

            # 为每个月创建数据行
            for month in months:
                # 使用每月1号作为日期
                month_date = pd.Timestamp(f"{year}-{month:02d}-01")
                monthly_data.append({
                    '日期': month_date,
                    '年份': year,
                    '月份': month,
                    '月度GDP': monthly_gdp,
                    '月度GDP同比增长': monthly_gdp_yoy
                })

        # 创建月度数据DataFrame
        monthly_df = pd.DataFrame(monthly_data)

        # 按日期逆序排序
        monthly_df = monthly_df.sort_values('日期', ascending=False)

        return monthly_df
