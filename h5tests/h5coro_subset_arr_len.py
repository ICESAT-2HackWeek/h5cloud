from pathlib import Path

from .h5test import H5Test, timer_decorator

import geopandas as gpd
import numpy as np
import subprocess

try:
    import h5coro
except:
    completed_process = subprocess.run([
        'mamba', 'install', '-c', 'conda-forge', 'h5coro', '--yes'
    ])
    import h5coro

from h5coro import h5coro, s3driver, filedriver
h5coro.config(errorChecking=True, verbose=False, enableAttributes=False)

class H5CoroSubsetArrLen(H5Test):       
        
    @timer_decorator
    def run(self):      
        final_h5coro_array = []
        bounds_path = Path.cwd().parent/'helpers/antarctic_aoi.geojson'
        group = '/gt1l/heights'
        variable = 'h_ph' 
        
        # read in the area of interest geojson
        aoi = gpd.read_file(bounds_path, crs='EPSG:4326')
        bounds = [v for v in aoi.bounds.values[0]]  
        
        h5coro.config(errorChecking=True, verbose=False, enableAttributes=False)
        
        for file in self.files:
            h5obj = h5coro.H5Coro(file.replace("s3://", ""), s3driver.S3Driver)

            h5obj.readDatasets(datasets=[f'{group}/lat_ph', f'{group}/lon_ph'], block=True)
            lat = h5obj[f'{group}/lat_ph'].values
            lon = h5obj[f'{group}/lon_ph'].values

            ph_in_aoi = np.where((lat > bounds[1]) & (lat < bounds[3]) \
                                 & (lon > bounds[0]) & (lon < bounds[2]))[0]

            idx_start = ph_in_aoi[0]
            idx_end = ph_in_aoi[-1]
            
            h5obj.readDatasets(datasets=[f'{group}/{variable}'], block=True)
         
            return h5obj[f'{group}/{variable}'].values.mean()
 