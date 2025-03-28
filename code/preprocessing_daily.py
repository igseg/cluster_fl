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
 'underlying_price']

dtype={'timestamp': 'int64',
 'local_timestamp': 'int64',
 'type': 'category',
 'strike_price': 'float32',
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
 'underlying_price': 'float32'}

path = '/home/igseta/scratch/full_tardis_data/'
save_path = '/home/igseta/scratch/preprocessed_data_daily/'
files = glob(path + '*')
files = sorted(files, key=lambda x: re.search(r'(\d{4}-\d{2}-\d{2})', x).group())
files = files[::-1]
T = len(files)

for i, file in enumerate(files):
    df = pd.read_csv(file, usecols=['timestamp', 'symbol']).reset_index()
    df = df[df.symbol.apply(lambda x: 'BTC' in x)]
    df = df.sort_values('timestamp')
    df = df.groupby('symbol').last()
    df = df.sort_values('timestamp')

    y = df['index'].values + 1
    df_2 = pd.read_csv(file, skiprows = lambda x: x not in y and x > 0)
    df_2 = df_2.sort_values('timestamp')

    if (df_2.timestamp.values == df.timestamp.values).sum() != len(df):
        print(f'file {file} may be compromised.')

    i = T - i
    name = str(i).zfill(4)
    df_2.to_csv(save_path + 'df_daily' + f"_{name}.csv.gz", index=False, compression='gzip')
