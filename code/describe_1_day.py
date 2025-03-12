import pandas as pd
import numpy as np
from datetime import datetime
from time import time
from tools import *

tardis_timestamp_to_dt = lambda x:  datetime.fromtimestamp(x/1e6)

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

data_tardis = filter_coin_tardis(data_tardis)
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
