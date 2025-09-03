#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import pandas as pd
import configparser
from tracemalloc import start
import unittest
import subprocess
from pathlib import Path
from dotenv import load_dotenv

from fetcher import sw_index_fetcher
from fetcher.stock_a_price_fetcher import StockAPriceFetcher
from datautils import FileStorage, DBStorage
from fetcher.a_financial_report_fetcher import AFinancialReportFetcher
from fetcher.etf_price_fetcher import ETFPriceFetcher
from fetcher.sw_index_fetcher import SWIndexFetcher
from fetcher.index_weight_fetcher import IndexWeightFetcher
from fetcher.stock_info_fetcher import StockInfoFetcher
from fetcher.stock_share_info_fetcher import StockShareInfoFetcher
from fetcher.stock_indicator_fetcher import StockIndicatorFetcher
from fetcher.stock_value_fetcher import StockValueFetcher
from fetcher.macro_data_china_fetcher import MacroDataChinaFetcher
from fetcher.stock_dividend_fetcher import StockDividendFetcher
from fetcher.stock_a_all_code_fetcher import StockAAllCodeFetcher
from fetcher.stock_hk_connector_all_code_fetcher import StockHKConnectorAllCodeFetcher
from fetcher.hk_connector_finacial_report_fetcher import HKConnectorFinancialReportFetcher
from utils.stock_utils import get_full_symbol

fs = FileStorage()
db = DBStorage()


def setup_logging(level_str='INFO'):
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)

    # 将字符串日志级别转换为logging模块的级别常量
    level = getattr(logging, level_str.upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/app.log'),
            logging.StreamHandler()
        ]
    )

# 加载配置文件


def load_config():
    config = configparser.ConfigParser()
    config_path = Path('config/config.ini')
    if config_path.exists():
        config.read(config_path)
    return config


# 运行单元测试
def run_tests():
    logger = logging.getLogger(__name__)
    logger.info('开始执行单元测试...')

    # 使用subprocess运行测试，这样可以捕获输出并记录到日志
    test_result = subprocess.run(['python', '-m', 'unittest', 'discover', 'tests'],
                                 capture_output=True, text=True)

    if test_result.returncode == 0:
        logger.info('单元测试全部通过')
        logger.debug(f'测试输出: {test_result.stdout}')
        return True
    else:
        logger.error('单元测试失败')
        logger.error(f'测试错误: {test_result.stderr}')
        logger.error(f'测试输出: {test_result.stdout}')
        return False


def fetch_stock_a_price(end_date):
    logger = logging.getLogger(__name__)

    fetcher = StockAPriceFetcher('tushare')

    res = fetcher.fetch_stock_price_all(
        start_date='2000-01-01', end_date=end_date, max_workers=3)

    # 初始化文件存储并保存数据
    # fs.save_to_csv(res, 'stock_price')
    fs.save_to_parquet(res, 'stock_price')
    logger.info('股票数据已成功保存到文件中')


def fetch_etf_price(end_date):
    logger = logging.getLogger(__name__)

    etf_fetcher = ETFPriceFetcher('akshare')
    etf_list = etf_fetcher.get_all_etf_codes()

    # 获取ETF价格数据
    etf_data = etf_fetcher.fetch_multiple_etf_price(
        etf_list, '2000-01-01', end_date)

    # 保存数据

    # fs.save_to_csv(etf_data, 'etf_price')
    fs.save_to_parquet(etf_data, 'etf_price')
    logger.info('ETF数据已成功保存到文件中')


def fetch_a_financial_report(symbols=None, fetch_all=True, max_workers=10, delay=2, report_types=None, provider='tushare'):
    """获取A股股票的财务报表数据

    Args:
        symbols: 可选，指定A股股票代码列表。如果为None且fetch_all=True，则获取所有A股股票
        fetch_all: 是否获取所有A股股票。如果为True且symbols为None，则获取所有A股股票
        max_workers: 最大线程数，默认为10
        delay: 每个请求之间的延迟时间(秒)，避免频繁请求被限制，默认2秒
        report_types: 可选，指定要获取的财务报表类型列表。如果为None，则获取所有报表
                     可选值: ['balance_sheet', 'income_statement', 'cash_flow_statement']
        provider: 数据源提供者，默认为'akshare'
    """
    logger = logging.getLogger(__name__)
    logger.info(f'开始获取A股股票财务报表数据(使用 {provider} 数据源)...')

    # 默认获取所有报表类型
    if report_types is None:
        report_types = ['balance_sheet',
                        'income_statement', 'cash_flow_statement']

    a_fin_report_fetcher = StockAAllCodeFetcher()

    # 获取A股股票代码列表
    if symbols is None:
        if fetch_all:
            logger.info('获取所有A股股票代码...')
            stock_list = a_fin_report_fetcher.get_all_stock_codes()
            logger.info(f'成功获取 {len(stock_list)} 个A股股票代码')
        else:
            logger.warning('symbols为None且fetch_all为False，无法获取A股股票代码')
            return
    elif isinstance(symbols, (int, float)):
        # 如果symbols是数值，获取指定数量的股票代码
        num_symbols = int(symbols)
        logger.info(f'获取前 {num_symbols} 个A股股票代码...')
        all_stocks = a_fin_report_fetcher.get_all_stock_codes()
        stock_list = all_stocks[:num_symbols]
        logger.info(f'成功获取 {len(stock_list)} 个A股股票代码')
    else:
        stock_list = symbols
        logger.info(f'使用指定的 {len(stock_list)} 个A股股票代码')

    a_fin_report_fetcher = AFinancialReportFetcher(provider)

    # 获取财务数据

    # 保存数据
    storage = FileStorage()

    if 'balance_sheet' in report_types:
        logger.info('开始获取资产负债表数据...')
        a_balance = a_fin_report_fetcher.fetch_multiple_balance_sheets(
            symbols=stock_list, max_workers=max_workers, delay=delay)
        storage.save_to_parquet(a_balance, 'a_balance_sheet_statement')
        storage.save_to_csv(a_balance.head(5000), 'a_balance_sheet_statement')

        logger.info(f'资产负债表数据已成功保存，共 {len(a_balance)} 条记录')

    if 'income_statement' in report_types:
        logger.info('开始获取利润表数据...')
        a_income = a_fin_report_fetcher.fetch_multiple_income_statements(
            symbols=stock_list, max_workers=max_workers, delay=delay)
        storage.save_to_parquet(a_income, 'a_income_statement')
        storage.save_to_csv(a_income.head(5000), 'a_income_statement')

        logger.info(f'利润表数据已成功保存，共 {len(a_income)} 条记录')

    if 'cash_flow_statement' in report_types:
        logger.info('开始获取现金流量表数据...')
        a_cash_flow = a_fin_report_fetcher.fetch_multiple_cash_flow_statements(
            symbols=stock_list, max_workers=max_workers, delay=delay)
        storage.save_to_parquet(a_cash_flow, 'a_cash_flow_statement')
        storage.save_to_csv(a_cash_flow.head(5000), 'a_cash_flow_statement')

        logger.info(f'现金流量表数据已成功保存，共 {len(a_cash_flow)} 条记录')

    logger.info('A股财务报表数据获取完成')


def fetch_hk_connector_financial_report(symbols=None, fetch_all=True, max_workers=5, delay=3, report_types=None):
    """获取港股通股票的财务报表数据

    Args:
        symbols: 可选，指定港股通股票代码列表。如果为None且fetch_all=True，则获取所有港股通股票
        fetch_all: 是否获取所有港股通股票。如果为True且symbols为None，则获取所有港股通股票
        max_workers: 最大线程数，默认为5
        delay: 每个请求之间的延迟时间(秒)，避免频繁请求被限制，默认0.5秒
        report_types: 可选，指定要获取的财务报表类型列表。如果为None，则获取所有报表
                     可选值: ['balance_sheet', 'income_statement', 'cash_flow_statement']
    """
    logger = logging.getLogger(__name__)
    logger.info('开始获取港股通股票财务报表数据...')

    # 默认获取所有报表类型
    if report_types is None:
        report_types = ['balance_sheet',
                        'income_statement', 'cash_flow_statement']

    # 初始化港股通股票代码获取器
    hk_code_fetcher = StockHKConnectorAllCodeFetcher()

    # 初始化港股通财务报表获取器
    hk_fin_report_fetcher = HKConnectorFinancialReportFetcher('akshare')

    # 初始化文件存储
    storage = FileStorage()

    # 获取港股通股票代码列表
    if symbols is None:
        if fetch_all:
            logger.info('获取所有港股通股票代码...')
            symbols = hk_code_fetcher.get_all_stock_codes()
            logger.info(f'成功获取 {len(symbols)} 个港股通股票代码')
        else:
            logger.warning('symbols为None且fetch_all为False，无法获取港股通股票代码')
            return
    else:
        logger.info(f'使用指定的 {len(symbols)} 个港股通股票代码')

    # 获取资产负债表数据
    if 'balance_sheet' in report_types:
        logger.info('开始获取港股通股票资产负债表数据...')
        hk_balance = hk_fin_report_fetcher.fetch_multiple_balance_sheets(
            symbols=symbols, max_workers=max_workers, delay=delay)
        storage.save_to_csv(hk_balance, 'hk_balance_sheet_statement')
        storage.save_to_parquet(hk_balance, 'hk_balance_sheet_statement')
        logger.info(f'港股通股票资产负债表数据已成功保存，共 {len(hk_balance)} 条记录')

    # 获取利润表数据
    if 'income_statement' in report_types:
        logger.info('开始获取港股通股票利润表数据...')
        hk_income = hk_fin_report_fetcher.fetch_multiple_income_statements(
            symbols=symbols, max_workers=max_workers, delay=delay)
        storage.save_to_csv(hk_income, 'hk_income_statement')
        storage.save_to_parquet(hk_income, 'hk_income_statement')
        logger.info(f'港股通股票利润表数据已成功保存，共 {len(hk_income)} 条记录')

    # 获取现金流量表数据
    if 'cash_flow_statement' in report_types:
        logger.info('开始获取港股通股票现金流量表数据...')
        hk_cash_flow = hk_fin_report_fetcher.fetch_multiple_cash_flow_statements(
            symbols=symbols, max_workers=max_workers, delay=delay)
        storage.save_to_csv(hk_cash_flow, 'hk_cash_flow_statement')
        storage.save_to_parquet(hk_cash_flow, 'hk_cash_flow_statement')
        logger.info(f'港股通股票现金流量表数据已成功保存，共 {len(hk_cash_flow)} 条记录')

    logger.info('港股通股票财务报表数据获取完成')


def fetch_sw_index_data(get_data=True):
    logger = logging.getLogger(__name__)
    logger.info('开始获取申万行业指数数据...')

    if get_data:

        # 初始化申万行业指数获取器
        sw_fetcher = SWIndexFetcher()

        # 获取所有申万三级行业代码
        level3 = sw_fetcher.get_sw_level3_info()
        fs.save_to_parquet(level3, 'sw_level3')

        sw_stock = sw_fetcher.get_all_sw_stock_info(
            sw_fetcher.get_sw_level3_codes())
        fs.save_to_parquet(sw_stock, 'sw_stock')

        level2 = sw_fetcher.get_sw_level2_info(use_cache=False)
        fs.save_to_parquet(level2, 'sw_level2')

        level1 = sw_fetcher.get_sw_level1_info(use_cache=False)
        fs.save_to_parquet(level1, 'sw_level1')
        logger.info('申万三级行业成分股数据已成功保存到文件中')

    file_list = ['data/sw_stock.parquet', 'data/sw_level1.parquet',
                 'data/sw_level2.parquet', 'data/sw_level3.parquet']
    sql = """
    select 
        t0.stock_name,t0.stock_code,
        t0.symbol,
        t3.level_3_name,t3.level_3_code,
        t2.level_2_name,t2.level_2_code,
        t1.level_1_name,t1.level_1_code
    from t0
    left join t3 on t0.level_3_code = t3.level_3_code
    left join t2 on t3.level_2_name = t2.level_2_name
    left join t1 on t2.level_1_name = t1.level_1_name
    """

    sw_dim = fs.sql_from_parquet(
        {'t0': file_list[0], 't1': file_list[1], 't2': file_list[2], 't3': file_list[3]}, sql)
    fs.save_to_csv(sw_dim, 'sw_dim')
    fs.save_to_parquet(sw_dim, 'sw_dim')

    # 删除临时文件
    import os
    for file in file_list:
        if os.path.exists(file):
            os.remove(file)
            logger.info(f'SW数据临时文件 {file} 已删除')


def read_parquet():
    logger = logging.getLogger(__name__)
    logger.info('开始读取数据...')
    # 保存数据
    storage = FileStorage()
    res = storage.sql_from_parquet(
        {'t1': 'data/stock_prices_test.parquet'}, 'select * from t1')
    print(res)
    logger.info('数据成功读取')


def fetch_index_weights():
    logger = logging.getLogger(__name__)
    logger.info('开始获取中证指数成分股权重数据...')

    # 初始化中证指数权重获取器
    index_fetcher = IndexWeightFetcher()

    # 获取所有中证指数代码
    # 这里可以根据需要设置具体的指数列表，例如：
    index_list = ['000300', '000905']  # 沪深300、中证500、创业板指

    # 获取指数权重数据
    index_weights = index_fetcher.get_multiple_index_weights(
        index_list=index_list,
        max_workers=3,
        delay=1.0
    )

    # 保存数据
    fs.save_to_csv(index_weights, 'index_weights')
    fs.save_to_parquet(index_weights, 'index_weights')

    logger.info(f'中证指数成分股权重数据已成功保存，共 {len(index_weights)} 条记录')


def fetch_stock_info():
    logger = logging.getLogger(__name__)
    logger.info('开始获取A股股票基本信息...')

    # 初始化股票信息获取器
    stock_info_fetcher = StockInfoFetcher()

    # 获取所有A股股票的基本信息
    stock_info = stock_info_fetcher.get_stock_info_all(
        max_workers=5,
        delay=0.5,
        use_cache=True
    )
    # stock_info = stock_info_fetcher.get_stock_info("600519")

    # 保存数据
    fs.save_to_csv(stock_info, 'stock_info')
    fs.save_to_parquet(stock_info, 'stock_info')

    logger.info(f'A股股票基本信息已成功保存，共 {len(stock_info)} 条记录')


def fetch_stock_share_info():
    logger = logging.getLogger(__name__)
    logger.info('开始获取A股股票股本结构数据...')

    # 初始化股本结构获取器
    stock_capital_fetcher = StockShareInfoFetcher()

    # 获取所有A股股票的股本结构信息
    stock_share_info = stock_capital_fetcher.get_stock_share_info_all(
    )

    # 保存数据
    fs.save_to_csv(stock_share_info, 'stock_share_info')
    fs.save_to_parquet(stock_share_info, 'stock_share_info')

    logger.info(f'A股股票股本结构数据已成功保存，共 {len(stock_share_info)} 条记录')


def fetch_stock_indicators():
    logger = logging.getLogger(__name__)
    logger.info('开始获取A股股票财务指标数据...')

    # 初始化股票财务指标获取器
    stock_indicator_fetcher = StockIndicatorFetcher()

    # 获取所有A股股票的财务指标信息
    stock_indicators = stock_indicator_fetcher.get_stock_indicator_all(
        max_workers=2,
        delay=1,
        symbol_range=(0, 500)
    )

    # 保存数据
    fs.save_to_csv(stock_indicators, 'stock_indicators-500')
    fs.save_to_parquet(stock_indicators, 'stock_indicators-500')

    logger.info(f'A股股票财务指标数据已成功保存，共 {len(stock_indicators)} 条记录')


def fetch_stock_value(start_date, end_date):
    logger = logging.getLogger(__name__)
    logger.info('开始获取A股股票价值指标数据...')

    # 初始化股票价值指标获取器
    stock_value_fetcher = StockValueFetcher()

    # 获取所有A股股票的价值指标信息
    stock_values = stock_value_fetcher.get_stock_value_all(
        max_workers=10,
        delay=0.5,
        start_date=start_date,
        end_date=end_date
    )

    # 保存数据
    fs.save_to_csv(stock_values, 'stock_values-25')
    fs.save_to_parquet(stock_values, 'stock_values-25')

    logger.info(f'A股股票价值指标数据已成功保存，共 {len(stock_values)} 条记录')


def fetch_macro_data_china():
    logger = logging.getLogger(__name__)
    logger.info('开始获取中国宏观经济数据...')

    # 先运行宏观数据相关的单元测试
    test_result = subprocess.run(['python', '-m', 'unittest', 'tests/test_macro_data_china_fetcher.py'],
                                 capture_output=True, text=True)

    if test_result.returncode != 0:
        logger.error('宏观数据单元测试失败')
        logger.error(f'测试错误: {test_result.stderr}')
        logger.error(f'测试输出: {test_result.stdout}')
        return

    logger.info('宏观数据单元测试通过，开始获取数据...')

    # 初始化宏观数据获取器
    macro_fetcher = MacroDataChinaFetcher()

    # 获取货币供应量数据
    money_supply = macro_fetcher.fetch_money_supply(start_year='2000')

    # 保存数据
    fs.save_to_csv(money_supply, 'money_supply')
    fs.save_to_parquet(money_supply, 'money_supply')

    logger.info(f'中国货币供应量数据已成功保存，共 {len(money_supply)} 条记录')

    # 获取中国GDP月度数据
    gdp_monthly = macro_fetcher.fetch_gdp_monthly(start_year='2007')

    # 保存GDP月度数据
    fs.save_to_csv(gdp_monthly, 'gdp_monthly')
    fs.save_to_parquet(gdp_monthly, 'gdp_monthly')

    logger.info(f'中国GDP月度数据已成功保存，共 {len(gdp_monthly)} 条记录')


def fetch_stock_dividend():
    logger = logging.getLogger(__name__)
    logger.info('开始获取A股股票分红数据...')

    # 初始化股票分红数据获取器
    stock_dividend_fetcher = StockDividendFetcher()

    # 获取所有A股股票的分红数据
    stock_dividends = stock_dividend_fetcher.get_stock_dividend_all(
        max_workers=1,
        delay=5
    )

    # stock_dividends = stock_dividend_fetcher.get_stock_dividend_batch(
    #     ["601398", "000001", "000002"]
    # )

    # 保存数据
    fs.save_to_csv(stock_dividends, 'stock_dividends')
    fs.save_to_parquet(stock_dividends, 'stock_dividends')

    logger.info(f'A股股票分红数据已成功保存，共 {len(stock_dividends)} 条记录')


def monthly_run():
    logger = logging.getLogger(__name__)
    logger.info('开始 monthly_run...')

    # 执行单元测试
    tests_passed = run_tests()
    if not tests_passed:
        logger.warning('单元测试未通过，请检查测试失败的原因')
        return

    end_date = '2023-08-31'
    # fetch_stock_a_price(end_date=end_date)
    # fetch_etf_price(end_date=end_date)
    # A股指标数据
    # fetch_stock_value(start_date='2025-01-01', end_date='2025-12-31')

    # 获取申万行业指数数据
    # fetch_sw_index_data()

    # 获取中证指数成分股权重数据
    # fetch_index_weights()

    # 获取A股股票基本信息
    # fetch_stock_info()

    # 获取A股股票股本结构数据
    # fetch_stock_share_info()

    # 获取中国宏观经济数据
    # fetch_macro_data_china()


def quarterly_run():
    logger = logging.getLogger(__name__)
    logger.info('开始 quarterly_run...')

    # 获取A股股票分红数据
    # fetch_stock_dividend()

<<<<<<< HEAD
    stock_list = ['000651', '600690', '000333',
                  '601398', '601318', '600030', '000568']  # 格力 海尔 美的 工商 平安 中信证券
=======
    # stock_list = ['000651', '600690', '000333']  # 格力 海尔 美的
>>>>>>> origin/main

    # 获取A股财务报表数据（资产负债表、利润表、现金流量表）
    fetch_a_financial_report(
        provider="tushare",
<<<<<<< HEAD
        symbols=None,               # None表示使用所有A股股票
=======
        symbols=None,              # None表示使用所有A股股票
>>>>>>> origin/main
        fetch_all=True,            # 获取所有A股股票
        max_workers=12,            # 最大线程数
        delay=2,                   # 请求延迟时间（秒）
<<<<<<< HEAD
        # 获取所有类型报表 ,'cash_flow_statement', 'balance_sheet'
        report_types=['income_statement']
=======
        report_types=['income_statement',
                      'cash_flow_statement']  # 获取所有类型报表 'balance_sheet'
>>>>>>> origin/main
    )

    # 获取港股通财务报表数据（资产负债表、利润表、现金流量表）
    # fetch_hk_connector_financial_report(
    #     symbols=None,              # None表示使用所有港股通股票
    #     fetch_all=True,            # 获取所有港股通股票
    #     max_workers=5,             # 最大线程数
    #     delay=3,                   # 请求延迟时间（秒）
    #     report_types=['balance_sheet', 'income_statement',
    #                   'cash_flow_statement']  # 获取所有类型报表
    # )


def dim_run():
    fetch_index_weights()
    # fetch_sw_index_data(get_data=True)
    # fetch_stock_share_info()
    # fetch_stock_info()


def test_run():
    import tushare as ts
<<<<<<< HEAD
    import os
    load_dotenv()
    # 从环境变量获取Tushare API Token
    tushare_token = os.environ.get('TUSHARE_TOKEN')

    # 初始化Tushare API
    pro = ts.pro_api(tushare_token)

    # 使用 stock_utils.py 中的 get_full_symbol 方法将股票代码转换为 tushare API 要求的格式
    stock_code = '000651.SZ'

    df = pro.income(ts_code=stock_code,
                    start_date='20180901', end_date='20250930', fields='ts_code,ann_date,net_after_nr_lp_correct')

    fs.save_to_csv(df, 'df')
=======
    pro = ts.pro_api(
        '2876ea85cb005fb5fa17c809a98174f2d5aae8b1f830110a5ead6211')

    # 使用 stock_utils.py 中的 get_full_symbol 方法将股票代码转换为 tushare API 要求的格式
    stock_code = '600519'
    full_symbol = get_full_symbol(
        stock_code, type='suffix')  # 转换为后缀格式，如 '600519.SH'
    df_a = pro.income(ts_code=full_symbol,
                      start_date='20000101', end_date='20251231')
    print(df_a)
    df_b = pro.income(ts_code=full_symbol,
                      start_date='20000101', end_date='20251231', report_type='6')
    print(df_b)
    # 添加报表类型标识列
    df_a['report_type'] = '合并报表'
    df_b['report_type'] = '母公司报表'

    # 合并两个DataFrame
    df_merged = pd.concat([df_a, df_b])

    # 打印和保存合并后的结果
    print(df_merged)
    fs.save_to_csv(df_merged, 'income_tushare_merged')
    fs.save_to_csv(df_a, 'df_a')
    fs.save_to_csv(df_b, 'df_b')
>>>>>>> origin/main


def main():
    # 设置日志
    setup_logging('INFO')
    logger = logging.getLogger(__name__)

    try:
        # 加载配置
        config = load_config()
        logger.info('应用启动成功')

        # fetch_stock_indicators()
        # 获取股票分红数据
        # fetch_stock_dividend()

        # monthly_run()
        quarterly_run()
        # dim_run()
        # test_run()

    except Exception as e:
        logger.error(f'程序运行出错: {str(e)}')
        raise


if __name__ == '__main__':
    main()
