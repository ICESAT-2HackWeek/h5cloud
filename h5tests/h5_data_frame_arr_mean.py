from .h5test import H5Test, timer_decorator
import h5py
import pandas as pd
import subprocess

try:
    from gedi_subset.h5frame import H5DataFrame
except ImportError:
    completed_process = subprocess.run([
        'pip', 'install', 'git+https://github.com/MAAP-Project/gedi-subsetter.git@0.6.0'
    ])
    from gedi_subset.h5frame import H5DataFrame

class H5DataFrameArrMean(H5Test):
    @timer_decorator
    def run(self):
        group = '/gt1l/heights'
        variable = 'h_ph'        
        dataframes = []
        for file in self.files:
            with h5py.File(name=self.s3_fs.open(file, 'rb')) as h5:
                df = H5DataFrame(h5[f"{group[1:]}"])
                dataframes.append(df[variable])
        final_dataframe: pd.Series = pd.concat(objs=dataframes, axis="index")
        return np.mean(final_dataframe)
