import pandas as pd
import numpy as np
from datetime import datetime

path_data = '~/scratch/'
name_tardis = 'deribit_options_chain_2023-07-04_OPTIONS.csv.gz'
folder = '../data/'

tardis_timestamp_to_dt = lambda x:  datetime.fromtimestamp(x/1e6)

data_tardis = pd.read_csv(path_data + name_tardis)

data_tardis['timestamp'] = data_tardis['timestamp'].apply(tardis_timestamp_to_dt)
data_tardis['expiration'] = data_tardis['expiration'].apply(tardis_timestamp_to_dt)

data_tardis['ttm'] = data_tardis['expiration'] - data_tardis['timestamp']
condition = data_tardis['ttm'].apply(lambda x:x.days < 1)

print(data_tardis[~condition].describe())
print('='*200)
print(data_tardis[condition].describe())

data_tardis[condition].to_csv(path_data + 'processed_data.csv.gz')
