import pandas as pd
import numpy as np
from datetime import datetime
from time import time
from tools import *


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

def process_file(file, columns_to_read, dtype):
    data_tardis = pd.read_csv(file,
                              usecols=columns_to_read,
                              dtype=dtype,
                             )
    
    data_tardis = filter_coin_tardis(data_tardis)
    data_tardis = data_tardis.dropna(subset=["last_price"])
    data_tardis = tardis_times_to_dt(data_tardis, 'timestamp')
    data_tardis = tardis_times_to_dt(data_tardis, 'expiration')
    data_tardis = filter_add_ttm(data_tardis)
    df_0dte, _  = filter_0dte(data_tardis)
    df_0dte_calls, df_0dte_puts = filter_split_call_put(df_0dte)
    df_0dte_calls = df_window(df_0dte_calls)
    df_0dte_puts = df_window(df_0dte_puts)

    return df_0dte_calls, df_0dte_puts

if __name__ == '__main__':
    t0 = time()
    path = './'
    files = glob(path + '*')
    save_path = ''
    save_name = ['df_0dte_calls_windowed', 'df_0dte_puts_windowed']
    first_iteration = True
    for i, file in enumerate(files):
        df_0dte_calls_tmp, df_0dte_puts_tmp = process_file(file, columns_to_read, dtype)

        if first_iteration:
            first_iteration = False
            df_0dte_calls = df_0dte_calls_tmp
            df_0dte_puts = df_0dte_puts_tmp
        else:
            df_0dte_calls = pd.concat([df_0dte_calls, df_0dte_calls_tmp])
            df_0dte_puts = pd.concat([df_0dte_puts, df_0dte_puts_tmp])
        if i == 10:
            break

    ## save results
    df_0dte_calls.to_csv(save_path + save_name[0], index=False)
    df_0dte_puts.to_csv(save_path + save_name[1], index=False)
    print(f'Total running time: {(time()- t0)/60:.2f} minutes')