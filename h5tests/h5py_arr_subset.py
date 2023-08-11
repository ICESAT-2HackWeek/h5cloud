import os
import sys

from .h5test import H5Test, timer_decorator
import h5py
import numpy as np

current = os.path.abspath('..')
sys.path.append(current)
from helpers.geospatial import get_subset_region, get_subset_indices

class H5pyArrSubset(H5Test):
    
    def __init__(self, data_format, geometry=None):
        """
        geometry : path to geojson file containing geometry
                   **Could be list containing [lonmin, lonmax, latmin, latmax]**
        """
        super().__init__(data_format)
        self.bounds = get_subset_region(geometry)
        
    @timer_decorator
    def run(self):
        final_h5py_array = []  
        # TODO: Do we need to make this configurable or consistent?
        group = '/gt1l/heights'
        variable = 'h_ph'        
        for file in self.files:
            with h5py.File(self.s3_fs.open(file, 'rb')) as f:
                
                lat = f[f'{group}/lat_ph'][:]
                lon = f[f'{group}/lon_ph'][:]
        
                idx_start, idx_end = get_subset_indices(lat, lon, self.bounds)
                
                # Leaving this code here so that we can create a DataFrame or
                # Dataset at a later date.  Suggest creating dict which can be 
                # passsed to xarray or (geo)pandas
                # lat[idx_start:idx_end])
                # lon[idx_start:idx_end])

                data = f[f'{group}/{variable}'][idx_start:idx_end]
                # Need to test if using concatenate is faster
                final_h5py_array = np.insert(
                    final_h5py_array,
                    len(final_h5py_array),
                    data, axis=None
                )
        return len(final_h5py_array)    