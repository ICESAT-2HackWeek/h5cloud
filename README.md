# H5Cloud - Cloud-Optimized Access of Hierarchical ICESat-2 Photon Data

## Collaborators
Aimee Barciauskas, Development Seed <br>
Andy Barrett, NSIDC <br>
Wei Ji, Development Seed <br>
Alex Lewandowski, Alaska Satellite Facility <br>
Luis Lopez, NSIDC <br>
Alex Mandel, Development Seed <br>
Jonathan Markel, UT Austin <br>
Suman Shekhar, Rutgers University <br>
Rachel Wegener, University of Maryland, College Park <br>
JP Swinski, NASA/GSFC <br>

## The Problem
ICESat-2 photon-data is formatted as HDF5 files, which provide many advantages for scientific applications including being self-describing and able to store heterogenous data.
However, ICESat-2 granules are frequently over a larger spatial extent than is needed for scientific workflows, meaning users must read in the full ATL03 HDF5 file to geolocate the data, then subset to a given area of interest. Applications like EarthData and NSIDC data portals have simplified this process for users by allowing them to provide a bounding box and only returning the subset data. Still, because HDF5 files are serialized, the original ATL03 H5 file must be read fully into memory by the cloud provider.

This is in contrast to raster data, where cloud-optimized GeoTIFFs are organized internally such that it is easy to access only a specific subset of the total area using HTTP GET range requests. A similar capability for ICESat-2 along-track photon data would provide measurable read performance improvements for cloud data providers and local data users alike. The current aims of this Hackweek group are to benchmark current methods of accessing/subsetting ATL03 data from a public cloud data source (AWS S3), investigate methods of repacking photon data, and determine how libraries like [kerchunk](https://fsspec.github.io/kerchunk/) can be used for more efficient requesting of data from specific along-track locations.
 
## Sample Data
ATL03 files used for testing were selected to maximize baseline ATL03 filesize, and are available at [s3://is2-cloud-experiments](s3://is2-cloud-experiments).

## Benchmark table

This project includes benchmark tests in 'h5tests/` for the libraries listed as rows below. Tests should be run against the formats listed as columns in the table below.

Notebooks exist in the [`notebooks/`](./notebooks/) folder for generating those formats listed.

| Library \ File Format | Original HDF5 | h5repack | GeoParquet | kerchunk original | kerchunk repacked |
|--|--|--|--|--|--|
| 1a - h5py                              |   |   | n/a | n/a | n/a |  
| 1b - gedi_subsetter H5DataFrame        |   |   | n/a | n/a | n/a |
| 2 - xarray via h5netcdf engine         |   |   | n/a |   |   |
| 3 - h5coro                             |   |   | n/a | n/a | n/a |
| 4a - geopandas via pyogrio/GDAL driver | n/a | n/a |   | n/a | n/a |
| 4b - geopandas via parquet driver      | n/a | n/a |   | n/a | n/a |

Key:
- n/a = Not applicable as the file format is not supported by library.

## Resources
- [ICESat-2 Hackweek Data Access Tutorials](https://icesat-2-2023.hackweek.io/tutorials/data-access-and-format/index.html)
- [SlideRule](https://github.com/ICESat2-SlideRule)
- [gedi-subsetter](https://github.com/MAAP-Project/gedi-subsetter)
- [geopandas](https://geopandas.org/en/v0.13.2/index.html)
- [h5coro](https://github.com/ICESat2-SlideRule/h5coro)
- [h5py](https://docs.h5py.org/en/stable/index.html)
- [icepyx](https://icepyx.readthedocs.io/en/latest/index.html)
- [xarray](https://docs.xarray.dev/en/v2023.06.0)
- [xarray-h5coro-backend](https://github.com/ICESAT-2HackWeek/xarray)

## Folders

### `contributors`
Each team member has it's own folder under contributors, where he/she can
work on their contribution. Having a dedicated folder for one-self helps to 
prevent conflicts when merging with master.

### `notebooks`
Notebooks that are considered delivered results for the project should go in
here.

### `scripts`
Helper utilities that are shared with the team

