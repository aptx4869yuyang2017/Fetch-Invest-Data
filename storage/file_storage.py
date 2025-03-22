#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import json
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any, Union

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import tempfile
import shutil
import os

logger = logging.getLogger(__name__)


class FileStorage:
    def __init__(self, base_dir: str = 'data'):
        self.logger = logging.getLogger(__name__)
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)

    def save_to_json(self, data: Dict[str, Any], filename: str) -> bool:
        """
        将数据保存为JSON文件
        :param data: 要保存的数据
        :param filename: 文件名
        :return: 保存是否成功
        """
        try:
            file_path = self.base_dir / f'{filename}.json'
            self.logger.info(f'开始保存 {file_path}')
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            self.logger.info(f'数据已成功保存到 {file_path}')
            return True

        except Exception as e:
            self.logger.error(f'保存JSON文件时发生错误: {str(e)}')
            return False

    def save_to_csv(self, data: Union[Dict[str, Any], pd.DataFrame], filename: str) -> bool:
        """
        将数据保存为CSV文件
        :param data: 要保存的数据（字典或DataFrame）
        :param filename: 文件名
        :return: 保存是否成功
        """
        try:
            file_path = self.base_dir / f'{filename}.csv'
            self.logger.info(f'开始保存 {file_path}')
            # 如果输入是字典，先转换为DataFrame
            if isinstance(data, dict):
                if 'prices' in data:
                    df = pd.DataFrame(data['prices'])
                else:
                    df = pd.DataFrame([data])
            else:
                df = data

            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            self.logger.info(f'数据已成功保存到 {file_path}')
            return True

        except Exception as e:
            self.logger.error(f'保存CSV文件时发生错误: {str(e)}')
            return False

    def save_to_parquet(self, data: Union[Dict[str, Any], pd.DataFrame], filename: str) -> bool:
        """
        将数据保存为Parquet文件
        :param data: 要保存的数据（字典或DataFrame）
        :param filename: 文件名
        :return: 保存是否成功
        """
        try:
            file_path = self.base_dir / f'{filename}.parquet'
            self.logger.info(f'开始保存 {file_path}')
            # 如果输入是字典，先转换为DataFrame
            if isinstance(data, dict):
                if 'prices' in data:
                    df = pd.DataFrame(data['prices'])
                else:
                    df = pd.DataFrame([data])
            else:
                df = data

            # 数据清洗：将空字符串替换为NaN，避免类型转换错误

            if isinstance(df, pd.DataFrame):
                df = df.replace('', np.nan)
            elif isinstance(df, dict):
                for key in df:
                    if isinstance(df[key], pd.DataFrame):
                        df[key] = df[key].replace('', np.nan)

            df.to_parquet(file_path, index=False)
            self.logger.info(f'数据已成功保存到 {file_path}')
            return True

        except Exception as e:
            self.logger.error(f'保存Parquet文件时发生错误: {str(e)}')
            return False

    def save_to_parquet_spark(self, data: Union[Dict[str, Any], pd.DataFrame], filename: str, spark=None) -> bool:
        """
        使用PySpark将数据保存为单个Parquet文件
        :param data: 要保存的数据（字典或DataFrame）
        :param filename: 文件名
        :param spark: 可选的SparkSession实例，如果为None则创建新的
        :return: 保存是否成功
        """
        try:
            file_path = self.base_dir / f'{filename}.parquet'
            self.logger.info(f'开始使用PySpark保存单个文件 {file_path}')

            # 如果没有提供SparkSession，创建一个新的
            if spark is None:
                spark = SparkSession.builder \
                    .appName("SaveToParquet") \
                    .getOrCreate()

            # 如果输入是字典，先转换为pandas DataFrame
            if isinstance(data, dict):
                if 'prices' in data:
                    pandas_df = pd.DataFrame(data['prices'])
                else:
                    pandas_df = pd.DataFrame([data])
            else:
                pandas_df = data

            # 数据清洗：将空字符串替换为None
            pandas_df = pandas_df.replace('', np.nan)

            # 将pandas DataFrame转换为Spark DataFrame
            spark_df = spark.createDataFrame(pandas_df)

            # 创建临时目录路径

            temp_dir = tempfile.mkdtemp()
            temp_path = os.path.join(temp_dir, "temp_parquet")

            try:
                # 先保存到临时目录
                spark_df.coalesce(1).write.mode("overwrite") \
                    .option("parquet.datetimeRebaseMode", "CORRECTED") \
                    .option("parquet.outputTimestampType", "TIMESTAMP_MICROS") \
                    .parquet(temp_path)

                # 找到生成的parquet文件并移动到目标位置
                parquet_files = [f for f in os.listdir(os.path.join(temp_path))
                                 if f.endswith(".parquet")]
                if parquet_files:
                    source_file = os.path.join(temp_path, parquet_files[0])
                    shutil.copy2(source_file, file_path)
                    self.logger.info(f'数据已成功使用PySpark保存到单个文件 {file_path}')
                    return True
                else:
                    raise Exception("未找到生成的Parquet文件")
            finally:
                # 清理临时目录
                shutil.rmtree(temp_dir)

        except Exception as e:
            self.logger.error(f'使用PySpark保存Parquet文件时发生错误: {str(e)}')
            return False

    def load_from_json(self, filename: str) -> Dict[str, Any]:
        """
        从JSON文件加载数据
        :param filename: 文件名
        :return: 加载的数据
        """
        try:
            file_path = self.base_dir / f'{filename}.json'
            self.logger.info(f'开始加载 {file_path}')
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.logger.info(f'成功从 {file_path} 加载数据')
            return data

        except Exception as e:
            self.logger.error(f'加载JSON文件时发生错误: {str(e)}')
            raise

    def load_from_csv(self, filename: str) -> pd.DataFrame:
        """
        从CSV文件加载数据
        :param filename: 文件名
        :return: 加载的数据DataFrame
        """
        try:
            file_path = self.base_dir / f'{filename}.csv'
            self.logger.info(f'开始加载 {file_path}')
            df = pd.read_csv(file_path)

            self.logger.info(f'成功从 {file_path} 加载数据')
            return df

        except Exception as e:
            self.logger.error(f'加载CSV文件时发生错误: {str(e)}')
            raise

    def load_from_parquet(self, filename: str) -> pd.DataFrame:
        """
        从Parquet文件加载数据
        :param filename: 文件名
        :return: 加载的数据DataFrame
        """
        try:
            file_path = self.base_dir / f'{filename}.parquet'
            self.logger.info(f'开始加载 {file_path}')
            df = pd.read_parquet(file_path)

            self.logger.info(f'成功从 {file_path} 加载数据')
            return df

        except Exception as e:
            self.logger.error(f'加载Parquet文件时发生错误: {str(e)}')
            raise
