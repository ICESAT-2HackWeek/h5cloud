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
    
class H5CoroArrMean(H5Test):
    @timer_decorator
    def run(self, file_format: str):
        tc = self.test_config
        final_h5coro_array = []
        file = tc.files[file_format]
        h5obj = h5coro.H5Coro(file.link.replace("s3://", ""), s3driver.S3Driver)
        h5obj.readDatasets(datasets=[f'{tc.group}/{tc.variable}'], block=True)
        return h5obj[f'{tc.group}/{tc.variable}'].values.mean()
