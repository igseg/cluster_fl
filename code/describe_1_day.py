import pandas as pd
import numpy as np
from datetime import datetime
from time import time

tardis_timestamp_to_dt = lambda x:  datetime.fromtimestamp(x/1e6)

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

def filter_0dte(df, ttm_name='ttm'):
    """
    Times must be as datetime objects
    """
    filter = data_tardis[ttm_name].apply(lambda x:x.days < 1)
    return data_tardis[filter], data_tardis[~filter]

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

# Load Data

t0 = time()

path_data = '~/scratch/'
name_tardis = 'deribit_options_chain_2023-07-04_OPTIONS.csv.gz'
folder = '../data/'

# data_tardis = pd.read_csv(path_data + name_tardis)
data_tardis = load_tardis_data(path_data + name_tardis)

t_load = time()
print(f'Time loading data: {(t_load - t0)/60:.2f} minutes')

# Filters

data_tardis = tardis_times_to_dt(data_tardis, 'timestamp')
data_tardis = tardis_times_to_dt(data_tardis, 'expiration')
data_tardis = filter_add_ttm(data_tardis)
df_0dte, _  = filter_0dte(data_tardis)
df_0dte_calls, df_0dte_puts = filter_split_call_put(df_0dte)
df_0dte_calls_delta_2_std = filter_2stds_dfcol(df=df_0dte_calls,
                                                col_name='delta')
df_0dte_puts_delta_2_std = filter_2stds_dfcol(df=df_0dte_puts,
                                                col_name='delta')

t_filter = time()
print(f'Time filtering data: {(t_filter-t_load)/60:.2f} minutes')

# Save results

name1 = 'df_0dte_calls'
name2 = 'df_0dte_puts'
name3 = 'df_0dte_calls_delta_2_std'
name4 = 'df_0dte_puts_delta_2_std'

df_0dte_calls.describe().to_csv(folder + name1)
df_0dte_puts.describe().to_csv(folder + name2)
df_0dte_calls_delta_2_std.describe().to_csv(folder + name3)
df_0dte_puts_delta_2_std.describe().to_csv(folder + name4)

# data_tardis[~condition].describe().to_csv(folder + 'descriptives_long.csv')
# data_tardis[condition].describe().to_csv(folder + 'descriptives_short.csv')

# data_tardis[condition].to_csv(path_data + 'processed_data.csv.gz')

print(f'Total running time: {(time()- t0)/60:.2f} minutes')
