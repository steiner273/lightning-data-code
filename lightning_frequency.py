import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pathlib
import time
    
def get_counts_by_day(df):
    
    list_of_dfs = []
    time_diff = 0
    for chunk in df:
        begin = time.time()
        chunk.index = pd.to_datetime(chunk.Datetime)
        list_of_dfs.append(chunk.groupby(chunk.index.date).Latitude.count())
        time_diff += time.time() - begin
        print(f'{time_diff: .3f} seconds')
    
    print(f'{time_diff: .3f} seconds')
    return pd.concat(list_of_dfs, axis=1).sum(axis=1).astype(np.int32)

def get_counts_by_hour(df):
    
    list_of_dfs = []
    for chunk in df:
        chunk.index = pd.to_datetime(chunk.Datetime)
        list_of_dfs.append(chunk.groupby(chunk.index.hour).Latitude.count())
        
    return pd.concat(list_of_dfs, axis=1).sum(axis=1).astype(np.int32)

def create_2dhistogram(df, lon_grid, lat_grid):
    
    lons = df.Longitude.values
    lats = df.Latitude.values
    counts, xbins, ybins = np.histogram2d(lons, lats, bins=[lon_grid, lat_grid])
    return counts, xbins, ybins

central_file = r"C:\Users\steiner.273\OneDrive - The Ohio State University\Lightning Data\CentralPlains_lightning_data.csv"

central_data = pd.read_csv(central_file, chunksize=2e5)
coord_list = [33.61, 43.01, -91.54, -104.84]
freq_arrays = []
lon_grid = np.linspace(coord_list[3], coord_list[2], 60)
lat_grid = np.linspace(coord_list[0], coord_list[1], 60)
    
month_dict = {i: [] for i in range(1,13)}
for i, chunkdata in enumerate(central_data):
    chunkdata.index = pd.to_datetime(chunkdata.Datetime)
    for j in range(1,13):
        df = chunkdata[chunkdata.index.month==j]
        month_dict[j].append(create_2dhistogram(df, lon_grid, lat_grid))
        
hour_dict = {i: [] for i in range(24)}
for chunk in central_data:
    chunk.index = pd.to_datetime(chunk.Datetime)
    for j in range(24):
        df = chunk[chunk.index.hour==j]
        hour_dict[j].append(create_2dhistogram(df, lon_grid, lat_grid))