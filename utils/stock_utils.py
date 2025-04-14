#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def get_full_symbol(symbol: str, type: str = "prefix") -> str:
    """
    将6位股票代码转换为带有交易所前缀或后缀的完整代码

    Args:
        symbol: 股票代码，格式为 "600000"（不带市场前缀）
        type: 返回格式类型，"prefix"表示前缀格式（如"sh600000"），"suffix"或"hz"表示后缀格式（如"600000.SH"）

    Returns:
        完整股票代码，如 "sh600000"/"600000.SH"、"sz000001"/"000001.SZ"、"bj430047"/"430047.BJ" 等
    """
    # 根据股票代码规则确定交易所前缀
    if symbol.startswith('6'):  # 上海证券交易所
        market_prefix = "sh"
        market_suffix = "SH"
    elif symbol.startswith('0') or symbol.startswith('3'):  # 深圳证券交易所
        market_prefix = "sz"
        market_suffix = "SZ"
    # 北京证券交易所
    elif symbol.startswith('8') or symbol.startswith('4') or symbol.startswith('43') or symbol.startswith('92'):
        market_prefix = "bj"
        market_suffix = "BJ"
    else:
        market_prefix = "sz"  # 默认使用深圳交易所前缀
        market_suffix = "SZ"

    if type.lower() == "suffix" or type.lower() == "hz":
        return f"{symbol}.{market_suffix}"
    else:  # 默认使用前缀格式
        return f"{market_prefix}{symbol}"
