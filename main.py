#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import configparser
from pathlib import Path

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
        
    except Exception as e:
        logger.error(f'程序运行出错: {str(e)}')
        raise

if __name__ == '__main__':
    main()