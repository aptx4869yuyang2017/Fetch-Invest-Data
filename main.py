#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import configparser
import unittest
import subprocess
from pathlib import Path
from fetcher.stock_price_fetcher import StockPriceFetcher
from storage.file_storage import FileStorage
from storage.db_storage import DBStorage
from fetcher.financial_report_fetcher import FinancialReportFetcher
from fetcher.financial_report_provider_akshare import FinancialReportProviderAkshare
from fetcher.etf_price_fetcher import ETFPriceFetcher


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


def fetch_stock_price():
    fetcher = StockPriceFetcher('akshare')
    stock_list = fetcher.get_all_stock_codes()

    res = fetcher.fetch_multiple_stock_price(
        stock_list[:10], '2025-01-01', '2025-02-28', 5)

    # 初始化文件存储并保存数据
    storage = FileStorage()
    storage.save_to_csv(res, 'stock_prices_test')
    storage.save_to_parquet(res, 'stock_prices_test')
    logger.info('股票数据已成功保存到文件中')


def fetch_etf_price():
    logger = logging.getLogger(__name__)
    etf_fetcher = ETFPriceFetcher('akshare')
    etf_list = etf_fetcher.get_all_etf_codes()

    # 获取ETF价格数据
    etf_data = etf_fetcher.fetch_multiple_etf_price(
        etf_list, '2000-01-01', '2025-02-28')

    # 保存数据
    storage = FileStorage()
    storage.save_to_csv(etf_data, 'etf_prices')
    storage.save_to_parquet(etf_data, 'etf_prices')
    logger.info('ETF数据已成功保存到文件中')


def fetch_financial_report():
    logger = logging.getLogger(__name__)
    fetcher = StockPriceFetcher('akshare')
    stock_list = fetcher.get_all_stock_codes()

    fetcher = FinancialReportFetcher('akshare')

    # 获取财务数据
    multiple_financial_data = fetcher.fetch_multiple_balance_sheets(
        symbols=['839729'], max_workers=4)
    # 保存数据
    storage = FileStorage()
    storage.save_to_csv(multiple_financial_data, 'balance_sheets_6000')
    storage.save_to_parquet(multiple_financial_data, 'balance_sheets_6000')

    logger.info('财务已成功保存到文件中')


def main():
    # 设置日志
    setup_logging('INFO')
    logger = logging.getLogger(__name__)

    try:
        # 加载配置
        config = load_config()
        logger.info('应用启动成功')

        # 执行单元测试
        # tests_passed = run_tests()
        # if not tests_passed:
        #     logger.warning('单元测试未通过，请检查测试失败的原因')
        # 可以选择在测试失败时退出程序
        # return

        fetch_financial_report()

    except Exception as e:
        logger.error(f'程序运行出错: {str(e)}')
        raise


if __name__ == '__main__':
    main()
