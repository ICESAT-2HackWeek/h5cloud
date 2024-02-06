import fsspec
import numpy as np
import xarray as xr

from h5test import H5Test, timer_decorator


class XarrayArrMean(H5Test):
    def open_reference_ds(self, file: str, dataset: str):
        fs = fsspec.filesystem(
            "reference",
            fo=file,
            remote_protocol="s3",
            remote_options=dict(anon=self.anon_access),
            skip_instance_cache=True,
        )
        return xr.open_dataset(
            fs.get_mapper(""), engine="zarr", consolidated=False, group=dataset
        )

    @timer_decorator
    def run(self, io_params={}, dataset="/gt1l/heights", variable="h_ph"):
        if "kerchunk" in self.data_format:
            datasets_ref = [
                self.open_reference_ds(file, dataset) for file in self.files
            ]
            h_ph_values = []
            for ds in datasets_ref:
                h_ph_values = np.append(h_ph_values, ds[variable].values)
            return np.mean(h_ph_values)
        else:
            if "fsspec_params" in io_params:
                fsspec_params = io_params["fsspec_params"]
            if "h5py_params" in io_params:
                h5py_params = io_params["h5py_params"]

            s3_fileset = [self.s3_fs.open(file, **fsspec_params) for file in self.files]
            xrds = xr.open_mfdataset(
                s3_fileset,
                group=dataset,
                combine="by_coords",
                engine="h5netcdf",
                **h5py_params
            )
            h_ph_values = xrds[variable]
            return float(np.mean(h_ph_values).values)
