#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def get_full_symbol(symbol: str) -> str:
    """
    将6位股票代码转换为带有交易所前缀的完整代码

    Args:
        symbol: 股票代码，格式为 "600000"（不带市场前缀）

    Returns:
        完整股票代码，如 "sh600000"、"sz000001"、"bj430047" 等
    """
    # 根据股票代码规则确定交易所前缀
    if symbol.startswith('6'):  # 上海证券交易所
        market_prefix = "sh"
    elif symbol.startswith('0') or symbol.startswith('3'):  # 深圳证券交易所
        market_prefix = "sz"
    # 北京证券交易所
    elif symbol.startswith('8') or symbol.startswith('4') or symbol.startswith('43') or symbol.startswith('92'):
        market_prefix = "bj"
    else:
        market_prefix = "sz"  # 默认使用深圳交易所前缀

    return f"{market_prefix}{symbol}"
