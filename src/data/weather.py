import os
import pandas as pd
import xarray as xr
from wildfire import get_rounded_locations
from math import sqrt

def make_bronze_dataframe(raw_paths):
    fire_df = get_fire_dataframe()
    df_lat_rounded, df_lon_rounded = get_rounded_locations(fire_df)
    dfws = []
    for path in raw_paths:
        dfw = xr.open_dataset(path).to_dataframe()
        dfw.reset_index(drop=False, inplace=True)
        dfw = dfw[
            (dfw["latitude"].isin(set(df_lat_rounded))) & 
            (dfw["longitude"].isin(set(df_lon_rounded))) &
            (dfw["time"].dt.date.isin(set(pd.to_datetime(dfw["Date"]).dt.date)))]
        dfw.set_index(["latitude", "longitude", "time"], inplace=True)
        dfws.append(dfw)
    dfw = pd.concat(dfws, axis = 1)
    del dfws
    dfw['wind_speed'] = dfw.v10**2 + dfw.u10**2
    dfw['wind_speed'] = dfw['wind_speed'].apply(lambda x: sqrt(x))
    dfw.drop(['u10', 'v10'], axis = 1, inplace = True)
    return dfw

def make_silver_dataframe(path):
    dfw = pd.read_csv(path, parse_dates=['time'])
    dfw['date'] = dfw['time'].dt.date
    dfw.drop('time', axis = 1, inplace = True)
    dfw_grp = dfw.groupby(['latitude', 'longitude', 'date'], axis = 0).agg(['mean', 'std'])
    dfw_grp.columns = ['_'.join(item) for item in dfw_grp]
    dfw_grp.drop(['cvl_std', 'cvh_std'], axis = 1, inplace = True)
    return dfw_grp

def get_fire_dataframe():
    path_fires = os.path.abspath(os.path.join(os.getcwd(), '../data/processed/wildfire/silver/silver_chracteristics.csv'))
    return pd.read_csv(path_fires)
    