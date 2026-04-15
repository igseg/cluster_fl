import pandas as pd
import numpy as np
from datetime import datetime

path_data = '../data/'
parser_ca = '%Y-%m-%d %H:%M:%S'
name_tardis = 'test_save_tardis.csv'
name_ca = 'test_save_ca.csv'
folder = '../data/'

tardis_timestamp_to_dt = lambda x:  datetime.fromtimestamp(x/1e6)
ca_str_to_dt = lambda x: datetime.strptime(x, parser_ca)

data_tardis = pd.read_csv(path_data + 'test_tardis.csv', nrows=1000)
data_ca = pd.read_csv(path_data + 'trade_history_Jan25.csv')

data_tardis['timestamp'] = data_tardis['timestamp'].apply(tardis_timestamp_to_dt)
data_ca['timestamp'] = data_ca['timestamp'].apply(ca_str_to_dt)

data_tardis.to_csv(folder + name_tardis)
data_ca.to_csv(folder + name_ca)
