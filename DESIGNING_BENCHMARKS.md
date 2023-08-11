# Designing Benchmarks

## Introduction

This document describes considerations for designing benchmarks for achieving a science result from geospatial data.

## Output

When designing benchmarks, you will likely want to arrive at one or more outputs. Such as (in order of complexity):
* Reading 1 variable 
* Spatial subsetting
* Temporal subsetting
* Multivariable computation
* A combination of the above

For this project we considered:
* Reading 1 full variable (i.e. `h_ph` from 5 granules, from the `gt1l/heights` beam and group)
* Spatial subsetting, using an AOI over the antartic

## Input

When designing benchmarks, you will consider multiple different inputs to measure the performance of those options. You can likely partition the options by:
* library
* data format
* output data structure
* science workflow or output
* web service or not
* whether a copy of the data of data was produced or not. If so, you may want to report how long it took to process the data to the new format and how much would it cost to process and store.

The last 2, given practical considerations, may be used to constrain the tests. For example, if there is no data provider able to reliably provide a copy of the optimized data or there is a cost constraing, perhaps it is not a reasonable option to consider. Unless of course, you are trying to make the case for a new data product to a data provider.

For this project, we are considering the following options:
* library: h5coro, h5py, xarray, geopandas, etc.
* data format: original hdf5, repacked hdf5, kerchunked (original and repacked) hdf5, geoparquet, flatgeobuf
* output data structure: We considered just a final result (i.e. the mean of the vairalbe or spatial subset)
* science workflow or output: Mean of the variable for the full dataset (of 5 files), mean of the variable for the spatial subset
* web service or not: we did not consider a web service as a part of this study, such as sliderule or harmony.
* whether a copy of the data of data was produced or not: We did consider copies of the data, as noted in the data format list. We are working on estimating the time and cost to produce those copies.

## Additional Test Considerations

* Storing results - how do you want to store results and parse them later? For this project, we stored them on S3 and used a python script to generate plots.
* End to end testing vs low level profiling of libraries. You may find that before or after end to end testing you need to understand what types of operations are being taken by the libraries you are testing, especially when results are different from what you expected. For example, you may want to use `S3FS_LOGGING_LEVEL=DEBUG` when using s3fs to understand what types of requests are being made to S3.
* CACHING 🤯 - Caching can happen at many levels - from data service (e.g. S3) to HTTP Caching (e.g. Cloudfront) to library/local server caching. 


## Additional Science Workflow Considerations

* Do users need to know about files? Science users are used to first discovering the files they want to access and then reading the data they need from that data. It is possible to consolidate operations so the user only has to think in terms of the end result and the libraries can handle both discovering where the underlying data lives AND parsing the data to produce the result. This is what xarray is designed for
* There is difference between reading the data and what additional processing may be required to make the data meaningful. You may want to consider how fast one approach is to read the data vs what additional steps may be required to achieve a meaningful result and how that may inform development of supporting libraries.
