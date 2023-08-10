from .h5test import H5Test, timer_decorator
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
    
class H5CoroArrLen(H5Test):
    @timer_decorator
    def run(self):
        group = '/gt1l/heights'
        variable = 'h_ph'        
        final_h5coro_array = []
        for file in self.files:
            h5obj = h5coro.H5Coro(f"{self.bucket}/{file}", s3driver.S3Driver)
            output = h5obj.readDatasets(datasets=[f'{group}/{variable}'], block=True)
            data = h5obj[f'{group}/{variable}'].values
            final_h5coro_array = np.insert(final_h5coro_array, len(final_h5coro_array), data, axis=None)
        return len(final_h5coro_array)
