#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import pandas as pd
from typing import Dict, Any, Optional, Union
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, DateTime, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
Base = declarative_base()


class DBStorage:
    """简化版数据库存储实现"""

    def __init__(self):
        """初始化数据库连接，从.env文件读取配置"""
        self.logger = logging.getLogger(__name__)
        try:
            # 加载.env文件
            load_dotenv()

            # 从环境变量获取数据库连接信息
            db_host = os.getenv('DB_HOST', 'localhost')
            db_port = os.getenv('DB_PORT', '5432')
            db_name = os.getenv('DB_NAME', 'postgres')
            db_user = os.getenv('DB_USER', 'postgres')
            db_password = os.getenv('DB_PASSWORD', '')

            # 构建连接字符串
            connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

            # 创建数据库连接
            self.engine = create_engine(connection_string)
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            self.logger.info('成功连接到数据库')

        except Exception as e:
            self.logger.error(f'连接数据库时发生错误: {str(e)}')
            raise

    def save_df(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append') -> None:
        """将DataFrame直接保存到数据库

        Args:
            df: 要保存的DataFrame
            table_name: 表名
            if_exists: 如果表存在时的处理方式，可选 'append'（增量）或 'replace'（覆盖）
        """
        try:
            # 直接使用pandas的to_sql方法保存数据
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=False
            )
            self.logger.info(f'成功保存 {len(df)} 条数据到表 {table_name}')

        except Exception as e:
            self.logger.error(f'保存数据到表 {table_name} 时发生错误: {str(e)}')
            raise

    def close(self) -> None:
        """关闭数据库连接"""
        try:
            self.session.close()
            self.engine.dispose()
            self.logger.info('成功关闭数据库连接')
        except Exception as e:
            self.logger.error(f'关闭数据库连接时发生错误: {str(e)}')
            raise
