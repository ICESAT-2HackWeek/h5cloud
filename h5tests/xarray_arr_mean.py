from .h5test import H5Test, timer_decorator
import fsspec
import xarray as xr
import numpy as np

class XarrayArrMean(H5Test):
    def open_reference_ds(self, file):
        fs = fsspec.filesystem(
            'reference', 
            fo=file, 
            remote_protocol='s3', 
            remote_options=dict(anon=False), 
            skip_instance_cache=True
        )
        return xr.open_dataset(fs.get_mapper(""), engine='zarr', consolidated=False, group='gt1l/heights')

    @timer_decorator
    def run(self):
        group = '/gt1l/heights'
        variable = 'h_ph'
        if 'kerchunk' in self.data_format:
            datasets = [self.open_reference_ds(file) for file in self.files]
            h_ph_values = []
            for dataset in datasets:
                h_ph_values = np.append(h_ph_values, dataset['h_ph'].values)
            return np.mean(h_ph_values).compute()
        else:
            s3_fileset = [self.s3_fs.open(file) for file in self.files]
            xrds = xr.open_mfdataset(s3_fileset, group=group, combine='by_coords', engine='h5netcdf')
            final_xr_array = xrds['h_ph']
            return np.mean(final_xr_array).compute()
