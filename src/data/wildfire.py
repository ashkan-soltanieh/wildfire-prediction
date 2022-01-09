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
    df_aob["geometry"] = df_aob["geometry"].apply(lambda shp: loads(shp))
    df_aob["area"] = df_aob["geometry"].apply(lambda x: (x.area / 10**6))
    df_aob.drop("geometry", axis=1, inplace = True)

    # clean characteristics data
    df_characteristics = pd.read_csv(characteristics_path, low_memory=False) \
                           .drop(['FD_Agency', 'dn', 'HHMM', 'sample', 'type', 'geometry'], axis = 1)
    df_characteristics["YYYYMMDD"] = df_characteristics["YYYYMMDD"] \
                                        .map(lambda x: str(x)) \
                                        .map(lambda x: str(x[0:4] + '-' + x[4:6] + '-' + x[6:8]))
    df_characteristics.rename(columns={'YYYYMMDD': 'Date'}, inplace=True)
    
    return df_aob, df_characteristics