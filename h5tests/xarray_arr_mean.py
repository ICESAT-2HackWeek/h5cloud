import fsspec
import numpy as np
import xarray as xr
from h5test import H5Test, timer_decorator


class XarrayArrMean(H5Test):
    def open_reference_ds(self, file):
        fs = fsspec.filesystem(
            "reference",
            fo=file,
            remote_protocol="s3",
            remote_options=dict(anon=False),
            skip_instance_cache=True,
        )
        return xr.open_dataset(
            fs.get_mapper(""), engine="zarr", consolidated=False, group="gt1l/heights"
        )

    @timer_decorator
    def run(self, io_params={}):
        group = "/gt1l/heights"
        variable = "h_ph"

        if "kerchunk" in self.data_format:
            datasets = [self.open_reference_ds(file) for file in self.files]
            h_ph_values = []
            for dataset in datasets:
                h_ph_values = np.append(h_ph_values, dataset["h_ph"].values)
            return np.mean(h_ph_values)
        else:
            fsspec_params = {}
            h5py_params = {}
            if "fsspec_params" in io_params:
                fsspec_params = io_params["fsspec_params"]
            if "h5py_params" in io_params:
                h5py_params = io_params["h5py_params"]
                print(h5py_params)

            s3_fileset = [self.s3_fs.open(file, **fsspec_params) for file in self.files]
            xrds = xr.open_mfdataset(s3_fileset, group=group, combine='by_coords', engine='h5netcdf', **h5py_params)
            h_ph_values = xrds['h_ph']
            return float(np.mean(h_ph_values).values)
