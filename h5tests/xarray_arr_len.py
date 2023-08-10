from .h5test import H5Test, timer_decorator
import xarray as xr

class XarrayArrLen(H5Test):
    @timer_decorator
    def run(self):
        group = '/gt1l/heights'
        variable = 'h_ph'        
        s3_fileset = [self.s3_fs.open(file) for file in self.files]
        xrds = xr.open_mfdataset(s3_fileset, group=group, combine='by_coords', engine='h5netcdf')
        final_xr_array = xrds['h_ph']
        return len(final_xr_array)
