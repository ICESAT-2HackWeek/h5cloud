import subprocess

import numpy as np
from h5test import H5Test, timer_decorator

try:
    import h5coro
except:
    completed_process = subprocess.run(
        ["pip", "install", "git+https://github.com/ICESat2-SlideRule/h5coro.git@main"]
    )
    import h5coro

from h5coro import h5coro, s3driver

driver = s3driver.S3Driver


class H5CoroArrMean(H5Test):
    @timer_decorator
    def run(self, dataset="/gt1l/heights", variable="h_ph"):
        group = dataset
        variable = variable
        final_h5coro_array = []

        for file in self.files:
            if link.startswith("s3://nasa-cryo-persistent/"):
                h5obj = h5coro.H5Coro(link.replace("s3://", ""), s3driver.S3Driver)
            else:
                h5obj = h5coro.H5Coro(
                    link.replace("s3://", ""),
                    s3driver.S3Driver,
                    credentials={"annon": True},
                )
            ds = h5obj.readDatasets(datasets=[f"{group}/{variable}"], block=True)
            data = ds[f"{group}/{variable}"][:]
            final_h5coro_array = np.insert(
                final_h5coro_array, len(final_h5coro_array), data, axis=None
            )
        return np.mean(final_h5coro_array)
