import pandas as pd


def safe_concat(dfs, **kwargs):
    """
    安全拼接多个 DataFrame，自动跳过空表或全 NA 表，避免 Pandas FutureWarning
    """
    valid_dfs = []
    for df in dfs:
        if df is None:
            continue
        if df.empty:
            continue
        if df.dropna(how="all").empty:  # 全是 NA
            continue
        valid_dfs.append(df)

    if not valid_dfs:
        return pd.DataFrame()

    # 显式排除空列避免FutureWarning
    return pd.concat(
        [df.dropna(axis=1, how='all') for df in valid_dfs],
        **kwargs
    )
