import pandas as pd
import os

def make_dataset_csv(raw_extents, raw_locations):
    gdf_ext = pd.concat(raw_extents, axis = 0)
    gdf_loc = pd.concat(raw_locations, axis = 0)

    gdf_ext_AB_BC = gdf_ext[(gdf_ext["REF_ID"].apply(lambda s : s[0:2] == "AB")) | 
                            (gdf_ext["REF_ID"].apply(lambda s : s[0:2] == "BC"))]

    gdf_loc_AB_BC = gdf_loc[(gdf_loc["REF_ID"].apply(lambda s : s[0:2] == "AB")) | 
                            (gdf_loc["REF_ID"].apply(lambda s : s[0:2] == "BC"))]
    
    extents_file_path = os.path.abspath(
        os.path.join(os.getcwd(), "../../data/processed/wildfire/bronze/bronze_extents.csv"))
    locations_file_path = os.path.abspath(
        os.path.join(os.getcwd(), "../../data/processed/wildfire/bronze/bronze_locations.csv"))

    gdf_ext_AB_BC.to_csv(extents_file_path, index = False)
    gdf_loc_AB_BC.to_csv(locations_file_path, index = False)