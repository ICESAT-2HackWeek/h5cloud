from h5test import H5Test, timer_decorator
import numpy as np
import subprocess

try:
    import h5coro
except:
    completed_process = subprocess.run([
        'mamba', 'install', '-c', 'conda-forge', 'h5coro', '--yes'
    ])
    import h5coro

from h5coro import h5coro, s3driver, filedriver
h5coro.config(errorChecking=True, verbose=False, enableAttributes=False)

driver =  s3driver.S3Driver
    
class H5CoroArrMean(H5Test):
    @timer_decorator
    def run(self, dataset="/gt1l/heights", variable="h_ph"):
        group = dataset
        variable = variable     
        final_h5coro_array = []
        if self.files[0].startswith("s3://cryo"):
            credentials = {}
        else:
            credentials = {"region_name": "us-west-2",
                           "anon": True}
        for file in self.files:
            h5obj = h5coro.H5Coro(file.replace("s3://", ""), s3driver.S3Driver, credentials=credentials)
            output = h5obj.readDatasets(datasets=[f'{group}/{variable}'], block=True)
            data = h5obj[f'{group}/{variable}'].values
            final_h5coro_array = np.insert(final_h5coro_array, len(final_h5coro_array), data, axis=None)
        return np.mean(final_h5coro_array)
