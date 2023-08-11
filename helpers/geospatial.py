"""Module containing methods for geospatial subsetting"""
import geopandas as gpd

geojson_path = '/home/jovyan/h5cloud/notebooks/antarctic_aoi.geojson'

def get_bounds(geometry):
    """Returns bounds from geojson or list
    
    geometry : geojson file containing geometry or list containing bounds
    
    **Assumes CRS is 4326 - check if this matters**
    """
    aoi = gpd.read_file(geometry, crs='EPSG:4326')
    bounds = aoi.bounds.values[0] 