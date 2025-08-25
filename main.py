#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import configparser
from tracemalloc import start
import unittest
import subprocess
from pathlib import Path
from fetcher import sw_index_fetcher
from fetcher.stock_a_price_fetcher import StockAPriceFetcher
from datautils import FileStorage, DBStorage
from fetcher.financial_report_fetcher import FinancialReportFetcher
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


def fetch_financial_report():
    logger = logging.getLogger(__name__)
    fetcher = StockAAllCodeFetcher()
    stock_list = fetcher.get_all_stock_codes()
    # stock_list = ['000651', '600690', '000333']  # 格力 海尔 美的

    fetcher = FinancialReportFetcher('akshare')

    # 获取财务数据

    # balance = fetcher.fetch_multiple_balance_sheets(
    #     symbols=stock_list, max_workers=3, delay=2)
    # income = fetcher.fetch_multiple_income_statements(
    #     symbols=stock_list, max_workers=3, delay=2)
    cash_flow = fetcher.fetch_multiple_cash_flow_statements(
        symbols=stock_list, max_workers=5, delay=2)

    # 保存数据
    storage = FileStorage()
    # storage.save_to_csv(balance, 'balance_sheet_statement')
    # storage.save_to_parquet(balance, 'balance_sheet_statement')

    # storage.save_to_csv(income, 'income_statement')
    # storage.save_to_parquet(income, 'income_statement')

    storage.save_to_csv(cash_flow, 'cash_flow_statement')
    storage.save_to_parquet(cash_flow, 'cash_flow_statement')

    logger.info('财务已成功保存到文件中')


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

    fetch_financial_report()


def dim_run():
    fetch_index_weights()
    # fetch_sw_index_data(get_data=True)
    # fetch_stock_share_info()
    # fetch_stock_info()


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
        # quarterly_run()
        dim_run()

    except Exception as e:
        logger.error(f'程序运行出错: {str(e)}')
        raise


if __name__ == '__main__':
    main()
