# H5Cloud Optimization Research

The goal of this research was 2 fold:

1. Replicate the results of cloud-optimized formatting (h5repack) and open file options for fsspec and f5py for ATL03 found in [HDF at the speed of Zarr](https://docs.google.com/presentation/d/1iYFvGt9Zz0iaTj0STIMbboRKcBGhpOH_LuLBLqsJAlk/edit?usp=sharing)
2. Understand if those results applied to other NASA HDF5 datasets.

## Discoveries:

1. In [01-check-co-hdf5-cmr.ipynb](01-check-co-hdf5-cmr.ipynb) we discovered that **all NASA HDF5 datasets in Earthdata Cloud** use the HDF5 defaults for file space management - that is:
    a. `strategy = FSM_AGGR`: Use free-space managers, aggregators and virtual file driver for file space allocation.
    b. `page size = 4096`: File space page size in bytes is 4kb. ([source](https://docs.hdfgroup.org/archive/support/HDF5/doc/RM/Tools.html))
2. This finds the same results as `HDF at the speed of Zarr` for ATL03, such as:
    a. h5py library - best performance is seen using "informed parameters". Repacking makes a much less significant difference.
    b. h5coro - Difference in performance between the original and repacked versions seems insignificant.
    c. xarray - best performance was seen with kerchunk, but a repacked version performs better than the original. Informed parameters may also improve performance with the repacked version.
3. In testing with other datasets, results varied and more testing may be needed.
    * For reading with h5py, repacking and informed parameters does appear to make a difference, except in the case of ATL08. But more analysis and robust testing should be done.
    * For reading with h5coro, repacking does not appear to make a significant difference.
    * For reading with xarray, repacking and kerchunk both make a significant difference. Kerchunk especially. The exception was that for SNDRSNIML2CCPRETN (Sounder SIPS: Suomi NPP CrIMSS Level 2 CLIMCAPS Normal Spectral Resolution: Atmosphere cloud and surface geophysical state V2) `surf_temp`, the repacked version was slower.

## Additional considerations

* Caching - In [benchmark-small-file-h5repack.ipynb](../benchmark-small-file-h5repack.ipynb) it seems clear the first test run always takes the longest. It is unclear if this is due to S3 caching or caching in the underlying libraries. The test notebooks were run more than once (without restarting the kernel) but more could be done to make these results robust to the impact of caching.

## In conclusion

* The results for ATL03 were consistent with results in slide 13 of [HDF at the speed of Zarr](https://docs.google.com/presentation/d/1iYFvGt9Zz0iaTj0STIMbboRKcBGhpOH_LuLBLqsJAlk/edit?usp=sharing). 
* Informed parameters for fsspec and h5py appear to improve performance when using h5py and xarray.
* Repacking does not matter to h5coro.
* The use of kerchunk (and possibly repacking) significantly improves the performance of xarray with HDF5 datasets in the cloud.
