from .h5test import H5Test, timer_decorator
import fsspec
import xarray as xr
import numpy as np

class XarrayArrMean(H5Test):
    def open_reference_ds(self, file: str, io_params: dict = {}):
        fs = fsspec.filesystem(
            'reference', 
            fo=file, 
            remote_protocol='s3', 
            remote_options=dict(anon=False), 
            skip_instance_cache=True,
            **io_params.get('fsspec_params', {})
        )
        return xr.open_dataset(fs.get_mapper(""), engine='zarr', consolidated=False, group=self.test_config.group, cache=False)

    @timer_decorator
    def run(self, file_format: str, io_params: dict = {}):
        tc = self.test_config
        file = tc.files[file_format]        
        if 'kerchunk' in file_format:
            xrds = self.open_reference_ds(file=file.link)
        else:
            file_opener = self.s3_fs.open(file.link, **io_params.get('fsspec_params', {}))
            xrds = xr.open_dataset(file_opener, group=tc.group, engine='h5netcdf', **io_params.get('h5py_params', {}), cache=False)
        data = xrds[tc.variable]
        xrds.close()
        return float(data.mean().values)
