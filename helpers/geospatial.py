"""Module containing methods for geospatial subsetting"""
import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import numpy as np

geojson_path = '/home/jovyan/h5cloud/helpers/antarctic_aoi.geojson'

def get_subset_region(geometry=None):
    """Returns bounds from geojson or list
    
    geometry : geojson file containing geometry or list containing bounds
    
    **Assumes CRS is 4326 - check if this matters**
    """
    if not geometry:
        geometry = geojson_path
    aoi = gpd.read_file(geometry, crs='EPSG:4326')
    return aoi.bounds.values[0]
    

def get_subset_indices(latitude, longitude, bounds):
    """Returns start and end index for subsetting"""
    lon0, lat0, lon1, lat1 = bounds
    indices_in_aoi = np.where((latitude > lat0) & (latitude < lat1) &
                              (longitude > lon0) & (longitude < lon1))[0]

    idx_start = indices_in_aoi[0]
    idx_end = indices_in_aoi[-1]
    
    return idx_start, idx_end
    