from datautils import FileStorage
import pandas as pd
import os


def split_financial_report(file_name):
    # 读取合并后的利润表
    fs = FileStorage()
    df = fs.load_from_parquet(file_name)

    # 添加时间过滤条件
    # 修正日期类型比较问题
    df = df[df['end_date'] >= pd.to_datetime('2010-01-01').date()]

    # 按公司类型分类处理
    for comp_type in ['1', '2', '3', '4']:
        # 筛选特定类型公司
        type_df = df[df['comp_type'] == comp_type].copy()

        # 删除全空列
        type_df = type_df.dropna(axis=1, how='all')

        fs.save_to_parquet(type_df, f'income_statement_type_{comp_type}')
        fs.save_to_csv(type_df, f'income_statement_type_{comp_type}')
        print(f'Saved {comp_type} type with {len(type_df)} rows')


if __name__ == '__main__':
    split_financial_report(
        file_name='a_income_statement'
    )
