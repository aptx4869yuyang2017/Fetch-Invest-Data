#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import configparser
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


def main():
    # 设置日志
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # 加载配置
        config = load_config()
        logger.info('应用启动成功')

        # TODO: 在这里添加数据抓取、清洗和存储的主要逻辑

        fetcher = StockPriceFetcher('akshare')
        stock_list = fetcher.get_all_stock_codes()
        res = fetcher.fetch_multiple_stocks(
            stock_list[:100], '2000-01-01', '2025-03-05')

        # 初始化文件存储并保存数据
        storage = DBStorage()
        storage.save_df(res, 'stock_prices', if_exists='replace')
        logger.info('股票数据已成功保存到数据库中')

    except Exception as e:
        logger.error(f'程序运行出错: {str(e)}')
        raise


if __name__ == '__main__':
    main()
