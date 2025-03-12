import pandas as pd
import numpy as np
from datetime import datetime
from time import time

def tardis_times_to_dt(df, column):
    df[column] = df[column].apply(tardis_timestamp_to_dt)
    return df

def filter_add_ttm(df, ttm_name='ttm', time_name='timestamp', maturity_name='expiration'):
    """
    adds time to maturity
    Times must be as datetime objects
    """
    df[ttm_name] = df[maturity_name] - df[time_name]
    return df

def filter_0dte(df, ttm_name='ttm', days_to_maturity=1):
    """
    Times must be as datetime objects
    """
    fil = df[ttm_name].apply(lambda x:x.days < days_to_maturity)
    return df[fil], df[~fil]
    

def filter_split_call_put(df, col_name='type', call_name='call', put_name='put'):
    return df[df[col_name]==call_name], df[df[col_name]==put_name]

def filter_2stds_dfcol(df, col_name):
    mean = df[col_name].mean()
    std = df[col_name].std()
    up_limit = mean + 2 * std
    low_limit = mean - 2 * std

    condition = np.logical_and(df[col_name] < up_limit, df[col_name] < low_limit)
    return df[condition]

def load_tardis_data(file):
    header = pd.read_csv(file, nrows=0)
    column_names = header.columns.tolist()
    n_cols = len(column_names)
    columns_to_read = column_names[2:]

    dtype = {}
    for i in range(2,len(column_names)):
        name = column_names[i]
        if name in ['symbol', 'type', 'underlying_index']:
            dtype[name] = 'category'
        elif name in ['timestamp', 'local_timestamp']:
            dtype[name] = 'int64'
        elif name in ['expiration']:
            continue
        else:
            dtype[name] = 'float32'
    data_tardis = pd.read_csv(file,
                          usecols=columns_to_read,
                          dtype=dtype
                         )
    return data_tardis

def df_window(df, window='5min'):
    # Sorting to ensure correct resampling
    df = df.sort_values(by=["underlying_index", "strike_price", "timestamp"])
    
    # Group by col1 and col2, then resample every 5 minutes and take the last observation
    filtered_df = (
        df.groupby(["underlying_index", "strike_price"], observed=True)
        .resample("5min", on="timestamp")
        .last()
        .drop(columns=["underlying_index", "strike_price"])
        .reset_index()
    )
    return filtered_df

def filter_coin_tardis(df, coin='BTC', drop=True):
    cond = df['symbol'].apply(lambda x: x[:3]) == 'BTC'
    if drop:
        df = df.drop(columns=['symbol'])
    return df[cond]