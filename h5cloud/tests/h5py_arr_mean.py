from .h5test import H5Test, timer_decorator
import h5py

class H5pyArrMean(H5Test):
    @timer_decorator
    def run(self, file_format: str, io_params: dict = {}):
        tc = self.test_config
        file = tc.files[file_format]        
        final_h5py_array = []
        with h5py.File(self.s3_fs.open(file.link, 'rb', **io_params.get('fsspec_params', {})), 'r', **io_params.get('h5py_params', {})) as h5obj:
            data = h5obj[f'{tc.group}/{tc.variable}'][:].flatten()
        return data.mean()
