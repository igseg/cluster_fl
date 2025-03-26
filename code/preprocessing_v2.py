import pandas as pd
import numpy as np
from datetime import datetime
from time import time
from tools import *
from glob import glob
import re

columns_to_read = ['symbol',
 'timestamp',
 'local_timestamp',
 'type',
 'strike_price',
 'expiration',
 'open_interest',
 'last_price',
 'bid_price',
 'bid_amount',
 'bid_iv',
 'ask_price',
 'ask_amount',
 'ask_iv',
 'mark_price',
 'mark_iv',
 'underlying_index',
 'underlying_price',
 'delta',
 'gamma',
 'vega',
 'theta',
 'rho']

dtype={'timestamp': 'int64',
 'local_timestamp': 'int64',
 'type': 'category',
 'strike_price': 'float32',
 'open_interest': 'float32',
 'last_price': 'float32',
 'bid_price': 'float32',
 'bid_amount': 'float32',
 'bid_iv': 'float32',
 'ask_price': 'float32',
 'ask_amount': 'float32',
 'ask_iv': 'float32',
 'mark_price': 'float32',
 'mark_iv': 'float32',
 'underlying_index': 'category',
 'underlying_price': 'float32',
 'delta': 'float32',
 'gamma': 'float32',
 'vega': 'float32',
 'theta': 'float32',
 'rho': 'float32'}

tardis_timestamp_to_dt = lambda x:  datetime.fromtimestamp(x/1e6)
def tardis_times_to_dt(df, column):
    df[column] = df[column].apply(tardis_timestamp_to_dt)
    return df

def process_file(file, columns_to_read, dtype):
    data_tardis = pd.read_csv(file,
                              usecols=columns_to_read,
                              dtype=dtype
                             )

    data_tardis = filter_coin_tardis(data_tardis)
    # data_tardis = data_tardis.dropna(subset=["last_price"])
    data_tardis = tardis_times_to_dt(data_tardis, 'timestamp')
    data_tardis = tardis_times_to_dt(data_tardis, 'expiration')
    data_tardis = filter_add_ttm(data_tardis)
    df_0dte, _  = filter_0dte(data_tardis)
    df_0dte_calls, df_0dte_puts = filter_split_call_put(df_0dte)
    # df_0dte_calls = df_window(df_0dte_calls)
    # df_0dte_puts = df_window(df_0dte_puts)

    return df_0dte_calls, df_0dte_puts

t0 = time()
path = '/home/igseta/scratch/full_tardis_data/'
save_path = '/home/igseta/scratch/preprocessed_data/'
# path = '/media/ignacio/TOSHIBA EXT/data/Tardis/'
# save_path = '/media/ignacio/3b28df90-2e02-4c09-b580-8da764c01346/data/'
save_name = ['df_0dte_calls', 'df_0dte_puts']

files = glob(path + '*')
files = sorted(files, key=lambda x: re.search(r'(\d{4}-\d{2}-\d{2})', x).group())
files = files[::-1]
T = len(files)

chunksize = int(1e7)
idx = 0
for i, file in enumerate(files):
    i = T - i
    chunks = pd.read_csv(file, usecols=columns_to_read, dtype=dtype, chunksize=chunksize)
    first_iteration = True
    if i < 35 or i >= 1798:
        continue
    for chunk in chunks:

        data_tardis = filter_coin_tardis(chunk)
        # data_tardis = data_tardis.dropna(subset=["last_price"])
        data_tardis = tardis_times_to_dt(data_tardis, 'timestamp')
        data_tardis = tardis_times_to_dt(data_tardis, 'expiration')
        data_tardis = filter_add_ttm(data_tardis)
        df_0dte, _  = filter_0dte(data_tardis)
        df_0dte_calls_chunk, df_0dte_puts_chunk = filter_split_call_put(df_0dte)

        if first_iteration:
            first_iteration=False
            df_0dte_calls_tmp = df_0dte_calls_chunk
            df_0dte_puts_tmp = df_0dte_puts_chunk
        else:
            df_0dte_calls_tmp = pd.concat([df_0dte_calls_tmp, df_0dte_calls_chunk])
            df_0dte_puts_tmp = pd.concat([df_0dte_puts_tmp, df_0dte_puts_chunk])
            
    name = str(i).zfill(4)
    df_0dte_calls_tmp.to_csv(save_path + save_name[0] + f"_{name}.csv.gz", index=False, compression='gzip')
    df_0dte_puts_tmp.to_csv(save_path + save_name[1] + f"_{name}.csv.gz", index=False, compression='gzip')

    print(f'Total running time: {(time()- t0)/60:.2f} minutes')