from datautils import FileStorage
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

fs = FileStorage()

stock_dividends_file_path = "data/stock_dividends.parquet"
sw_dim_file_path = "data/sw_dim.parquet"

sql = """
select 
    t1.symbol,  
    t2.stock_name,
    t1.date, 
    t1.dividend
from stock_dividens t1 
left join sw_dim t2 
on t1.symbol = t2.symbol
"""

table_dict = {'stock_dividens': stock_dividends_file_path,
              'sw_dim': sw_dim_file_path}
df = fs.sql_from_parquet(table_dict, sql)

# 确保日期列是日期类型
df['date'] = pd.to_datetime(df['date'])

# 添加年份列以便于分组
df['year'] = df['date'].dt.year

# 获取每个股票的最早和最晚日期
min_max_dates = df.groupby('symbol')['date'].agg(['min', 'max']).reset_index()

# 创建一个空的结果DataFrame
result_frames = []

# 为每个股票创建完整的时间序列
for _, row in min_max_dates.iterrows():
    symbol = row['symbol']
    min_date = row['min']
    max_date = row['max']
    
    # 创建完整的日期范围
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')
    
    # 为该股票创建完整的时间序列DataFrame
    symbol_df = pd.DataFrame({'date': date_range})
    symbol_df['symbol'] = symbol
    
    # 获取该股票的名称
    stock_name = df[df['symbol'] == symbol]['stock_name'].iloc[0] if not df[df['symbol'] == symbol].empty else None
    symbol_df['stock_name'] = stock_name
    
    # 合并原始数据
    symbol_data = df[df['symbol'] == symbol].copy()
    symbol_df = symbol_df.merge(symbol_data[['symbol', 'date', 'dividend']], 
                               on=['symbol', 'date'], how='left')
    
    # 填充缺失的分红为0
    symbol_df['dividend'] = symbol_df['dividend'].fillna(0)
    
    # 添加年份列
    symbol_df['year'] = symbol_df['date'].dt.year
    
    # 计算分红数据
    # 1. 按年份分组计算每年的总分红
    yearly_dividends = symbol_data.groupby('year')['dividend'].sum()
    
    # 2. 计算每条记录的分红数据
    symbol_df['last_year_total_dividend'] = symbol_df['year'].apply(
        lambda year: yearly_dividends.get(year-1, 0))
    
    # 3. 计算今年累计分红
    symbol_df['current_year_total_dividend'] = 0.0
    for year in symbol_df['year'].unique():
        year_data = symbol_df[symbol_df['year'] == year]
        for i, date_row in year_data.iterrows():
            current_date = date_row['date']
            cumulative_div = symbol_data[(symbol_data['year'] == year) & 
                                        (symbol_data['date'] <= current_date)]['dividend'].sum()
            symbol_df.at[i, 'current_year_total_dividend'] = cumulative_div
    
    # 4. 计算滚动12个月分红
    symbol_df['rolling_12m_dividend'] = 0.0
    for i, row in symbol_df.iterrows():
        current_date = row['date']
        one_year_ago = current_date - pd.DateOffset(months=11)
        rolling_div = symbol_data[(symbol_data['date'] >= one_year_ago) & 
                                 (symbol_data['date'] <= current_date)]['dividend'].sum()
        symbol_df.at[i, 'rolling_12m_dividend'] = rolling_div
    
    # 添加到结果列表
    result_frames.append(symbol_df)

# 合并所有股票的结果
result = pd.concat(result_frames, ignore_index=True)

print(result)

# 保存结果
fs.save_to_parquet(result, "stock_dividends_with_yields")
fs.save_to_csv(result, "stock_dividends_with_yields")
