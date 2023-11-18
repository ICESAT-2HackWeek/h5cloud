import os
import subprocess

import h5py
import numpy as np
import pandas as pd

from .h5test import H5Test, timer_decorator

try:
    os.environ["USE_PYGEOS"] = "0"
    import geopandas as gpd
    import pyogrio
except ImportError:
    completed_process = subprocess.run(
        ["mamba", "install", "-c", "conda-forge", "pyogrio", "--yes"]
    )
    import pyogrio


class GeopandasPyogrioArrMean(H5Test):
    @timer_decorator
    def run(self):
        group = "/gt1l/heights"  # not used
        variable = "h_ph"
        geodataframes = []
        for file in self.files:
            # file = "s3://nasa-cryo-scratch/h5cloud/flatgeobuf/ATL03_20230211164520_08111812_006_01.fgb"
            # print(f"Loading {file} into geopandas via pyogrio")
            gdf = gpd.read_file(filename=file, engine="pyogrio")
            geodataframes.append(gdf[variable])
        final_geodataframe: gpd.Series = pd.concat(objs=geodataframes, axis="index")
        return np.mean(final_geodataframe)
