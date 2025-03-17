#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import configparser
import unittest
import subprocess
from pathlib import Path
from fetcher.stock_price_fetcher import AkshareProvider, StockPriceFetcher
from storage.file_storage import FileStorage
from storage.db_storage import DBStorage

# 设置日志配置


def setup_logging():
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
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


def main():
    # 设置日志
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # 加载配置
        config = load_config()
        logger.info('应用启动成功')

        # 执行单元测试
        tests_passed = run_tests()
        if not tests_passed:
            logger.warning('单元测试未通过，请检查测试失败的原因')
            # 可以选择在测试失败时退出程序
            # return

        # TODO: 在这里添加数据抓取、清洗和存储的主要逻辑

        fetcher = StockPriceFetcher('akshare')
        stock_list = fetcher.get_all_stock_codes()

        res = fetcher.fetch_multiple_stocks(
            stock_list[:10], '2000-01-01', '2025-02-28', 5)

        # 初始化文件存储并保存数据
        # storage = FileStorage()
        # storage.save_to_csv(res, 'stock_prices_2000-2025')
        # storage.save_to_parquet(res, 'stock_prices_2000-2025')
        # logger.info('股票数据已成功保存到文件中')

        # 初始化文件存储并保存数据
        # db_storage = DBStorage()
        # db_storage.save_df(res, 'stock_prices', if_exists='append')
        # logger.info('股票数据已成功保存到数据库中')

    except Exception as e:
        logger.error(f'程序运行出错: {str(e)}')
        raise


if __name__ == '__main__':
    main()
