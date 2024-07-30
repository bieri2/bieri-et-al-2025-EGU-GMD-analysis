# Import packages 
import xarray as xr
import numpy as np
import os
import pandas as pd
from netCDF4 import Dataset
from wrf import (to_np, getvar, smooth2d, get_cartopy, cartopy_xlim,
                 cartopy_ylim, latlon_coords, CoordPair, GeoBounds)
import datetime as dt
import os

# Define paths
in_path  = '/glade/derecho/scratch/cab478/NoahMP_out/prod/rootnew/'
out_path = '/glade/derecho/scratch/cab478/noahmp_extracted/rootnew/'
wrf_path = '/glade/u/home/cab478/hrldas_resources/resources/wrfinput_d01'

# Get lats and lons of model grid
# Open the NetCDF file
ncfile     = Dataset(wrf_path)
dummy      = getvar(ncfile, "SHDMAX")
cart_proj  = get_cartopy(dummy)
# Get the latitude and longitude points
lats, lons = latlon_coords(dummy)

def read_noahmp_2d(path, files, var):
    '''
    This function reads 2-D (time, lat, lon) Noah-MP output variables as an xarray DataArray and takes the daily mean.
    Returns an xarray DataArray with the output variable at all output time steps.
    If the variable is the DynaRoot root water uptake depth, the variable is converted from layer number (native format) to layer depth in m. 
    '''
    data = []
    temp = []
    print(var)

    mask = {0:0.0, 1:0.1, 2:0.4, 3:1.0, 4:2.0, 5:3.0, 6:4.0, 7:6.0, 8:8.0, 
            9:10.0, 10:12.0, 11:15.0, 12:20.0}
    
    for f in files:
        print(f)
        darray = xr.DataArray(data=Dataset(path + f)[var][:],
                 dims=['time','lat','lon'],
                 coords=dict(
                 time = pd.to_datetime([f[:10]], format='%Y%m%d%H'),
                 XLAT = (['lat','lon'],lats.values),
                 XLONG = (['lat','lon'],lons.values)))
        if var == 'GWRD':
           darray = xr.apply_ufunc(lambda data: mask.get(data), darray, vectorize=True)

        temp.append(darray)

        if f[8:10]=='21':
           temp = xr.concat(temp, dim='time').rename(var)
           data.append(temp.groupby('time.date').mean(dim='time'))
           temp = []

    data = xr.concat(data, dim='date')
    data = data.assign_coords(date=pd.to_datetime(data.date))

    return data

def read_noahmp_3d(path, files, var):
    '''
    This function reads 3-D (time, lat, soil layers, lon)  Noah-MP output variables as an xarray DataArray and takes the daily mean.
    Returns an xarray DataArray with the output variable at all output time steps.
    '''
    data = []
    temp = []
    print(var)
    
    for f in files:
        print(f)
        darray = xr.DataArray(data=Dataset(path + f)[var][:],
                 dims=['time','lat','soil_layers_stag','lon'],
                 coords=dict(
                 time = pd.to_datetime([f[:10]], format='%Y%m%d%H'),
                 XLAT = (['lat','lon'],lats.values),
                 XLONG = (['lat','lon'],lons.values)))
        temp.append(darray)

        if f[8:10]=='21':
           temp = xr.concat(temp, dim='time').rename(var)
           data.append(temp.groupby('time.date').mean(dim='time'))
           temp = []

    data = xr.concat(data, dim='date')
    data = data.assign_coords(date=pd.to_datetime(data.date))

    return data

# List variables you want to read and process
var      = ['GWRD']

# Get list of files in Noah-MP output directory
files    = [x for x in os.listdir(in_path) if x.endswith('LDASOUT_DOMAIN1')]
files    = sorted(files)

# Call one of the above functions, depending on variable dimensions
for v in var:
  data2d = read_noahmp_2d(in_path, files, v)
  data2d.to_netcdf(out_path + 'noahmp_out_' + v + '.nc')

#  data3d = read_noahmp_3d(in_path, files, v)
#  data3d.to_netcdf(out_path + 'noahmp_out_' + v + '.nc')
