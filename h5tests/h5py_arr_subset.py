from .h5test import H5Test, timer_decorator
import h5py
import numpy as np

class H5pyArrSubset(H5Test):
    
    def __init__(self, geometry=None):
        """
        geometry : path to geojson file containing geometry
                   **Could be list containing [lonmin, lonmax, latmin, latmax]**
        """
        super().__init__()
        self.bounds = get_bounds(geometry)
        
    @timer_decorator
    def run(self):
        final_h5py_array = []  
        # TODO: Do we need to make this configurable or consistent?
        group = '/gt1l/heights'
        variable = 'h_ph'        
        for file in self.files:
            s3_filename = f"s3://{self.bucket}/{file}"
            with h5py.File(self.s3_fs.open(s3_filename, 'rb')) as f:
                data = f[f'{group}/{variable}'][:]
                # Need to test if using concatenate is faster
                final_h5py_array = np.insert(
                    final_h5py_array,
                    len(final_h5py_array),
                    data, axis=None
                )
        return len(final_h5py_array)    