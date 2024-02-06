import h5py
import numpy as np

from h5test import H5Test, timer_decorator


class H5pyArrMean(H5Test):
    @timer_decorator
    def run(self, io_params={}, dataset="/gt1l/heights", variable="h_ph"):
        final_h5py_array = []
        fsspec_params = {}
        h5py_params = {}
        if "fsspec_params" in io_params:
            fsspec_params = io_params["fsspec_params"]
        if "h5py_params" in io_params:
            h5py_params = io_params["h5py_params"]
        for file in self.files:
            with self.s3_fs.open(file, mode="rb", **fsspec_params) as fo:
                print("h5py params: ", h5py_params)
                with h5py.File(fo, **h5py_params) as f:
                    data = f[f"{dataset}/{variable}"][:]
                    final_h5py_array = np.insert(
                        final_h5py_array, len(final_h5py_array), data, axis=None
                    )
        return np.mean(final_h5py_array)
