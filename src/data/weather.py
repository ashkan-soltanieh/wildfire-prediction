import os
import pandas as pd
import xarray as xr
from wildfire import get_rounded_location_and_date_of_fires
from math import sqrt

def make_bronze_dataframe(raw_paths):
    fire_df = get_fire_dataframe()
    lat_rounded, lon_rounded, dates = get_rounded_location_and_date_of_fires(fire_df)
    dfws = []
    for path in raw_paths:
        dfw = xr.open_dataset(path).to_dataframe()
        dfw.reset_index(drop=False, inplace=True)
        dfw = dfw[
            (dfw["latitude"].isin(lat_rounded)) & 
            (dfw["longitude"].isin(lon_rounded)) &
            (dfw["time"].dt.date.isin(dates))]
        dfw.set_index(["latitude", "longitude", "time"], inplace=True)
        dfws.append(dfw)
    dfw = pd.concat(dfws, axis = 1)
    del dfws
    dfw['wind_speed'] = dfw.v10**2 + dfw.u10**2
    dfw['wind_speed'] = dfw['wind_speed'].apply(lambda x: sqrt(x))
    dfw.drop(['u10', 'v10'], axis = 1, inplace = True)
    return dfw

def get_fire_dataframe():
    path_fires = os.path.abspath(os.path.join(os.getcwd(), '../data/processed/wildfire/silver/silver_chracteristics.csv'))
    return pd.read_csv(path_fires)
    