# H5Cloud - Cloud-Optimized Access of Hierarchical ICESat-2 Photon Data

## Collaborators
Aimee Barciauskas, Development Seed <br>
Luis Lopez, NSIDC <br>
Jonathan Markel, UT Austin <br>
Alex Lewandowski, Alaska Satellite Facility <br>
Suman Shekhar, Rutgers University <br>
Andy Barrett, NSIDC <br>

## The Problem
ICESat-2 photon-data is formatted as HDF5 files, which provide many advantages for scientific applications including being self-describing and able to store heterogenous data.
However, ICESat-2 granules are frequently over a larger spatial extent than is needed for scientific workflows, meaning users must read in the full ATL03 HDF5 file to geolocate the data, then subset to a given area of interest. Applications like EarthData and NSIDC data portals have simplified this process for users by allowing them to provide a bounding box and only returning the subset data. Still, because HDF5 files are serialized, the original ATL03 H5 file must be read fully into memory by the cloud provider.

This is in contrast to raster data, where cloud-optimized GeoTIFFs are organized internally such that it is easy to access only a specific subset of the total area using HTTP GET range requests. A similar capability for ICESat-2 along-track photon data would provide measurable read performance improvements for cloud data providers and local data users alike. The current aims of this Hackweek group are to benchmark current methods of accessing/subsetting ATL03 data from a public cloud data source (AWS S3), investigate methods of repacking photon data, and determine how libraries like [kerchunk](https://fsspec.github.io/kerchunk/) can be used for more efficient requesting of data from specific along-track locations.
 
## Sample Data
ATL03 files used for testing were selected to maximize baseline ATL03 filesize, and are available at [s3://is2-cloud-experiments](s3://is2-cloud-experiments).

## Resources
- [h5coro](https://github.com/ICESat2-SlideRule/h5coro)
- [icepyx](https://icepyx.readthedocs.io/en/latest/index.html)
- [SlideRule](https://github.com/ICESat2-SlideRule)
- [ICESat-2 Hackweek Data Access Tutorials](https://icesat-2-2023.hackweek.io/tutorials/data-access-and-format/index.html)

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

