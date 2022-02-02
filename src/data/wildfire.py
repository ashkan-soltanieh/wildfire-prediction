from dis import dis
import geopandas
import pandas as pd
from shapely.wkt import loads

def make_bronze_dataframes(aob_path, chracteristics_path):
    # merging and converiting Area of burn and
    # characteristics shp files into dataframes
    gdf_extents = []
    for path in aob_path:
        gdf_extents.append(geopandas.GeoDataFrame.from_file(path))
    
    gdf_locations = []
    for path in chracteristics_path:
        gdf_locations.append(geopandas.GeoDataFrame.from_file(path))

    gdf_aob = pd.concat(gdf_extents, axis = 0)
    gdf_characteristics = pd.concat(gdf_locations, axis = 0)

    # filter merged data for Alberta and BC
    gdf_aob_AB_BC = gdf_aob[(gdf_aob["REF_ID"].apply(lambda s : s[0:2] == "AB")) | 
                            (gdf_aob["REF_ID"].apply(lambda s : s[0:2] == "BC"))]

    gdf_characteristics_AB_BC = gdf_characteristics[(gdf_characteristics["REF_ID"].apply(lambda s : s[0:2] == "AB")) | 
                            (gdf_characteristics["REF_ID"].apply(lambda s : s[0:2] == "BC"))]

    return gdf_aob_AB_BC, gdf_characteristics_AB_BC

def make_silver_dataframes(aob_path, characteristics_path):
    # clean Area of Burn(AoB) data
    df_aob = pd.read_csv(aob_path, dtype={'UID_Fire': str}) \
               .drop(['FD_Agency', 'JD', 'date_src', 'Year'], axis = 1)
    df_aob.rename(columns={"Map_Date": "Date_of_Burn"}, inplace=True)
    df_aob["area"] = get_area_of_polygon(df_aob['geometry'])
    df_aob.drop("geometry", axis = 1, inplace = True)
    df_aob = df_aob.groupby(['UID_Fire', 'REF_ID', 'Date_of_Burn'], axis = 0).sum()
    df_aob.rename(columns={'area': 'Total_AoB'}, inplace = True)
    
    # clean characteristics data
    df_characteristics = pd.read_csv(characteristics_path, low_memory = False) \
                           .drop(['FD_Agency', 'dn', 'HHMM', 'sample', 'type', 'geometry'], axis = 1)
    df_characteristics['rounded_lat'], df_characteristics['rounded_lon'] = get_rounded_locations(df_characteristics)
    df_characteristics['Date'] = get_formatted_date(df_characteristics['YYYYMMDD'])
    df_characteristics.drop(['YYYYMMDD'], axis = 1, inplace = True)
    df_characteristics['T21'] = df_characteristics['T21'] - 273.15
    df_characteristics['T31'] = df_characteristics['T31'] - 273.15
    grp_list = ['Date', 'sat', 'UID_Fire', 'Status', 'REF_ID', 'rounded_lat', 'rounded_lon']
    df_characteristics = df_characteristics.groupby(grp_list, axis = 0) \
                      .agg(['mean', 'std']) \
                      .fillna(0) # to replace NaN in std when there is single value for grouped row
    df_characteristics.columns = ['_'.join(item) for item in df_characteristics.columns]
    return df_aob, df_characteristics

def get_rounded_locations(df):
    df_lat_rounded = df.lat.map(lambda x: round(x * 4) / 4)
    df_lon_rounded = df.lon.map(lambda x: round(x * 4) / 4)
    return df_lat_rounded, df_lon_rounded

def get_area_of_polygon(df_geometry):
    df_polygon = df_geometry.apply(lambda shp: loads(shp))
    df_area = df_polygon.apply(lambda x: (x.area / 10**6))
    return df_area

def get_formatted_date(df_date):
    return df_date.map(lambda x: str(x)) \
                  .map(lambda x: str(x[0:4] + '-' + x[4:6] + '-' + x[6:8]))