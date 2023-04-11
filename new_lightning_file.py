import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pathlib
import time

def get_regional_dataframe(df, region_list):
    
    lon0 = region_list[2]
    lon1 = region_list[3]
    lat0 = region_list[0]
    lat1 = region_list[1]
    
    test_func = lambda x: ((x.Latitude.le(lat1).all() & x.Latitude.ge(lat0).all())&((x.Longitude.le(lon0).all())&(x.Longitude.ge(lon1).all())))
    
    cond = (df.Latitude.le(lat1) & df.Latitude.ge(lat0))&(df.Longitude.le(lon0) & df.Longitude.ge(lon1))
    threshold_crossed = cond.ne(cond.shift())
    groups = threshold_crossed.cumsum()
    new_set = df.groupby(groups)
    test_list = [group[1] for group in new_set if test_func(group[1])]  
    if test_list:
        new_df = pd.concat(test_list, axis=0)
    else:
        new_df = pd.DataFrame([], columns=['Datetime', 'Latitude', 'Longitude'])
    return new_df

def print_time(timediff):
    
    minutes, seconds = divmod(timediff, 60)
    print(f'{minutes: .0f} minutes, {seconds: .2f} seconds')
    
def get_regional_dataframe2(df, region_list):
    
    lon0 = region_list[2]
    lon1 = region_list[3]
    lat0 = region_list[0]
    lat1 = region_list[1]
    
    cond = (df.Latitude.le(lat1) & df.Latitude.ge(lat0))&(df.Longitude.le(lon0) & df.Longitude.ge(lon1))
    return df[cond]

def get_regional_dataframe3(df, region_list):
    
    lon0 = region_list[2]
    lon1 = region_list[3]
    lat0 = region_list[0]
    lat1 = region_list[1]
    
    cond_str = f'(Latitude <= {lat1}) & (Latitude >= {lat0}) & (Longitude <= {lon0}) & (Longitude >= {lon1})'
    return df.query(cond_str)

file_path = pathlib.Path(r"C:\Users\steiner.273\OneDrive - The Ohio State University\Lightning Data")
file_list = list(file_path.rglob('*.txt'))

#coord_list = [38.4, 42.32, -80.5, -84.8]
coord_list = [33.61, 43.01, -91.54, -104.84]
region_test = lambda x: ((x.Latitude.le(coord_list[0]).any())&(x.Latitude.ge(coord_list[1]).any()))&((x.Longitude.le(coord_list[2]).any())&(x.Longitude.ge(coord_list[3]).any()))

column_names = ['Date', 'Time', 'Latitude', 'Longitude', 'Current', 'Multi', 'Type']

data_list = [pd.read_csv(file, delim_whitespace=True, names=column_names, header=None, chunksize=200000) for file in file_list]
df_list = []
for chunk in data_list:
    chunk_df_list = []
    time_diff = 0
    for i, df in enumerate(chunk):
        begin = time.time()
        if region_test(df):
            df['Datetime'] = df['Date'] + ' ' + df['Time']
            new_data = df[['Datetime', 'Latitude', 'Longitude']]
            #chunk_df_list.append(get_regional_dataframe2(new_data, coord_list))
            get_regional_dataframe2(new_data, coord_list).to_csv(file_path / 'CentralPlains_lightning_data.csv', header=False, index=False, mode='a')
            time_diff += time.time() - begin
            if (i+1) % 50 == 0:
                print_time(time_diff)
                print(f'{df.memory_usage().sum() / (1024*1024): .3f} MB')
        else:
            time_diff += time.time() - begin
            print_time(time_diff)
            continue
        
    df_list.append(pd.concat(chunk_df_list, axis=0))
pd.concat(df_list, axis=0).to_csv(file_path / 'CentralPlains_lightning_data.csv')