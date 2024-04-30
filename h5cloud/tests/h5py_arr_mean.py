from .h5test import H5Test, timer_decorator
import h5py
import numpy as np

class H5pyArrMean(H5Test):
    @timer_decorator
    def run(self, file_format: str):
        final_h5py_array = []         
        tc = self.test_config
        file = tc.files[file_format]
        with h5py.File(self.s3_fs.open(file.link, 'rb')) as f:
            data = f[f'{tc.group}/{tc.variable}'][:]
            # Need to test if using concatenate is faster
            final_h5py_array = np.insert(
                final_h5py_array,
                len(final_h5py_array),
                data, axis=None
            )
        return np.mean(final_h5py_array)
