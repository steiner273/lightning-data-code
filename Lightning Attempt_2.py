#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import os.path

work_dir = "D:\\Data\\Lightning Data\\"

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from calendar import month_abbr, month_name

column_names = ['Date', 'Time', 'Latitude', 'Longitude', 'Current', 'Multi', 'Type']

def get_data(wdir): 
    """this function retrieves the files from the lightning data folder
    then uses the Pandas read_csv function,breaks up the files into 
    chunksizes (in this case, 2,000,000 lines per chunk) and then stores 
    the chunked dataframes in a python dictionary for use later
    this method was used because it prevents a lot of overload on memory because of the large file sizes"""
    diction={file: pd.read_csv(os.path.join(wdir, file), delim_whitespace=True, header=None, names=column_names, chunksize=5e5) for file in os.listdir(wdir)}    #places the chunked dataframe with the specific key in the dictionary; this is how we
        #access the data later
    return diction

def set_year_month_day_hour(chunk):
    #I used this function to process the file as it was being read in (by chunksize)
    dates = pd.to_datetime(chunk.loc[:,'Date'])
    years = dates.dt.year
    months = dates.dt.month
    days = dates.dt.day
    times = pd.to_datetime(chunk.loc[:,'Time'], format='%H:%M:%S.%f')
    hours = times.dt.hour
    new_frame = pd.DataFrame({'Year': years, 'Month': months, 'Day': days, 'Time': times, 'Hour': hours})
    #creates new column for "hour"    #specifies the columns that we want to keep
    return new_frame

def calc_frequencies(data, type_freq):
    
    freq_type_dict = {'Month': list(range(1,13)),
                        'Hour': list(range(24))}
    
    array_len = freq_type_dict[type_freq][1]
    count = data[type_freq].value_counts().sort_index().reindex(array_len, fill_value=0).values
    return count

def create_lat_lon_grids(lat_range, lon_range, spacing):
    
    #creates the lat-lon grid boxes for binning the lightning data for our climatologies
    
    lons = np.arange(lon_range[0],lon_range[1], spacing)
    lats = np.arange(lat_range[0],lat_range[1], spacing)
    region_list = [[(j, j+spacing),(i,i+spacing)] for j in lats for i in lons]
    return region_list

def oldregion_func(data, region_list):
    #this function selects the data from a given lat-lon range from the dataframe
    lats = region_list[0]
    lons = region_list[1]
    lat1 = lats[0]
    lat2 = lats[1]
    lon1 = lons[0]
    lon2 = lons[1]
    data_1 = data[(data['Latitude'] > lat1) & (data['Latitude'] <= lat2) & (data['Longitude'] > lon1) & (data['Longitude'] <= lon2)]
    return data_1

def newregiongrid(chunk, grid):

    lats = grid[0]
    lons = grid[1]
    cdata = chunk.groupby([pd.cut(chunk['Latitude'], lats),pd.cut(chunk['Longitude'],lons)])
    regions_data = [x[1] for x in cdata]
    return regions_data

def new_func(region_grid_list, month_names):

    data_dict = get_data(work_dir)
    region_month_hourly_freqs = {f'{grid}': {month_abbr[i]: np.zeros(24,dtype=int) for i in range(1, 13)} for grid in region_grid_list}
    index_names = ['Month', 'Hour']
    midx = pd.MultiIndex.from_product([np.arange(1, 13), np.arange(24)], names=index_names)
    for key, val in data_dict.items():
        for chunk in val:
            region_chunks = {f'{grid}': newregiongrid(chunk, grid) for grid in region_grid_list}
            for grid, rchunks in region_chunks.items():
                if not rchunks[0].empty():
                    form_dat_chunk = set_year_month_day_hour(rchunks[0])
                    bymonth_hours = form_dat_chunk.groupby(['Month', 'Hour']).count()['Hour'].reindex(midx, fill_value=0).sort_index()
                    for i in range(1, 13):
                        region_monthly_hourly_freqs[f'{grid}'][month_abbr[i]]+=bymonth_hours[i].values

    return region_monthly_hourly_freqs

def calc_month_freq(region_grid_list):
    #this function calculates lightning frequency by month (total lightning within the month)
    data_dict = get_data(work_dir)
    
    region_monthly_freqs = {f'{grid}': np.zeros(12, dtype=int) for grid in region_grid_list}
    
    for key, val in data_dict.items():
        region_chunks = {f'{grid}': pd.concat([region_range(chunk, grid) for chunk in val]) for grid in region_grid_list}
        date_chunks = {f'{key}': set_year_month_day_hour(region_chunk) for key, region_chunk in region_chunks.items()}
        bymonth_groupbys = {f'{key}': form_chunk.groupby('Month').count()['Hour'].sort_index() for key, form_chunk in date_chunks.items()}
        for grid in region_grid_list:
            region_monthly_freqs[f'{grid}']+=bymonth_groupbys[f'{grid}'].values

    return region_monthly_freqs

region = [(30,50),(-105,-80)]
regiongrids = create_lat_lon_grids(region[0],region[1],5)

region_hourly_month_freqs = calc_hourly_month_freq(regiongrids, month_abbr)
regional_monthly_frequencies = calc_month_freq(regiongrids)
