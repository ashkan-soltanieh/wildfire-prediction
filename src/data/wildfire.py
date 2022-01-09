import geopandas
import pandas as pd
import os
from shapely.wkt import loads

def make_bronze_dataframe(ext_paths, loc_paths):
    # merging and converiting shp files into data frame
    gdf_extents = []
    for path in ext_paths:
        gdf_extents.append(geopandas.GeoDataFrame.from_file(path))
    
    gdf_locations = []
    for path in loc_paths:
        gdf_locations.append(geopandas.GeoDataFrame.from_file(path))

    gdf_ext = pd.concat(gdf_extents, axis = 0)
    gdf_loc = pd.concat(gdf_locations, axis = 0)

    # filter merged data for Alberta and BC
    gdf_ext_AB_BC = gdf_ext[(gdf_ext["REF_ID"].apply(lambda s : s[0:2] == "AB")) | 
                            (gdf_ext["REF_ID"].apply(lambda s : s[0:2] == "BC"))]

    gdf_loc_AB_BC = gdf_loc[(gdf_loc["REF_ID"].apply(lambda s : s[0:2] == "AB")) | 
                            (gdf_loc["REF_ID"].apply(lambda s : s[0:2] == "BC"))]

    return gdf_ext_AB_BC, gdf_loc_AB_BC

def make_silver_dataset_csv(extents_path, locations_path):
    df_ext = pd.read_csv(extents_path, dtype={'UID_Fire': str, 'Year': str}) \
               .drop(['FD_Agency', 'JD', 'date_src' ,'Map_Date'], axis = 1)
    
    # to convert string declaration of polygon into Polygon object
    df_ext["geometry"] = df_ext["geometry"].apply(lambda shp: loads(shp))
    
    # to get area of polygons in km^2
    df_ext["area"] = df_ext["geometry"].apply(lambda x: (x.area / 10**6))
    
    # drop geometry column as it cannot be aggregated
    df_ext.drop("geometry", axis=1, inplace = True)
    
    # aggregated by describe to get all required information for generating distributions
    df_ext_grp = df_ext.groupby(['UID_Fire', 'Year', 'REF_ID']).describe()

    # flatten the column labels and revert the indices used in group by back to columns
    df_ext_grp.columns = ['_'.join(col) for col in df_ext_grp.columns.values]
    df_ext_grp.reset_index(drop=False, inplace=True)