#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Union

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
            
            # 如果输入是字典，先转换为DataFrame
            if isinstance(data, dict):
                if 'prices' in data:
                    df = pd.DataFrame(data['prices'])
                else:
                    df = pd.DataFrame([data])
            else:
                df = data
            
            df.to_csv(file_path, index=False, encoding='utf-8')
            self.logger.info(f'数据已成功保存到 {file_path}')
            return True
            
        except Exception as e:
            self.logger.error(f'保存CSV文件时发生错误: {str(e)}')
            return False

    def load_from_json(self, filename: str) -> Dict[str, Any]:
        """
        从JSON文件加载数据
        :param filename: 文件名
        :return: 加载的数据
        """
        try:
            file_path = self.base_dir / f'{filename}.json'
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
            df = pd.read_csv(file_path)
            
            self.logger.info(f'成功从 {file_path} 加载数据')
            return df
            
        except Exception as e:
            self.logger.error(f'加载CSV文件时发生错误: {str(e)}')
            raise