from pathlib import Path
import os
import subprocess
import sys

import geopandas as gpd
import numpy as np

from .h5test import H5Test, timer_decorator

current = os.path.abspath('..')
sys.path.append(current)
from helpers.geospatial import get_subset_region, get_subset_indices

try:
    import h5coro
except:
    completed_process = subprocess.run([
        'mamba', 'install', '-c', 'conda-forge', 'h5coro', '--yes'
    ])
    import h5coro

from h5coro import h5coro, s3driver, filedriver
h5coro.config(errorChecking=True, verbose=False, enableAttributes=False)

class H5CoroSubsetMean(H5Test):       

    def __init__(self, data_format, geometry=None):
        """
        geometry : path to geojson file containing geometry
                   **Could be list containing [lonmin, lonmax, latmin, latmax]**
        """
        super().__init__(data_format)
        self.bounds = get_subset_region(geometry)
        
    @timer_decorator
    def run(self):      
        final_h5coro_array = []
        bounds_path = Path.cwd().parent/'helpers/antarctic_aoi.geojson'
        group = '/gt1l/heights'
        variable = 'h_ph' 
        
        
        h5coro.config(errorChecking=True, verbose=False, enableAttributes=False)
        
        for file in self.files:
            h5obj = h5coro.H5Coro(file.replace("s3://", ""), s3driver.S3Driver)

            h5obj.readDatasets(datasets=[f'{group}/lat_ph', f'{group}/lon_ph'], block=True)
            lat = h5obj[f'{group}/lat_ph'].values
            lon = h5obj[f'{group}/lon_ph'].values
            
            idx_start, idx_end = get_subset_indices(lat, lon, self.bounds)
            
            datasets = [{f'dataset': f'{group}/{variable}', 'startrow': idx_start, 'numrows': idx_end}]

#             ph_in_aoi = np.where((lat > bounds[1]) & (lat < bounds[3]) \
#                                  & (lon > bounds[0]) & (lon < bounds[2]))[0]

#             idx_start = ph_in_aoi[0]
#             idx_end = ph_in_aoi[-1]
            
            h5obj.readDatasets(datasets=datasets, block=True)
            print(len(h5obj[f'{group}/{variable}'].values))
         
            return h5obj[f'{group}/{variable}'].values.mean()
 