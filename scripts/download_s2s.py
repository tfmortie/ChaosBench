from pathlib import Path
import xarray as xr
import pandas as pd
import argparse
import calendar
from datetime import datetime, timedelta
import config

import logging
logging.basicConfig(level=logging.INFO)

from ecmwfapi import ECMWFDataServer

def _increment_days(all_dates, end_date):
    "This is a utility function especially for ECMWF/CMA data processing involving sparse forecast"
    
    # Define the increments
    increments = [3, 4]

    # Convert start_date and end_date from string to datetime objects
    start_date = all_dates[0]
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # Loop until the current_date is less than or equal to the end_date
    while current_date <= end_date:
        
        # Get the next increment value
        increment_value = increments.pop(0)
        
        # Add the increment to the current date
        current_date += timedelta(days=increment_value)
        all_dates.append(current_date.strftime('%Y-%m-%d'))
        
        # Re-add the used increment value to the end of the list
        increments.append(increment_value)
        
    return all_dates

def main(args):
    """
    Main driver to download S2S physics-based benchmark data based on individual variable
    Usage example: `python download_s2s.py --s2s_name ncep`
    """
    assert args.s2s_name in list(config.S2S_CENTERS.keys())
    center_id = config.S2S_CENTERS[args.s2s_name]
    
    # Initialize ECMWF API
    server = ECMWFDataServer()
    
    # Set output directory
    output_dir = Path(config.DATA_DIR) / args.s2s_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # NOTE: for ECMWF/CMA (from 2020 onward), it is not a continuous timeseries, but rather, the product is available every 3/4 days
    if (args.s2s_name == 'ecmwf') or (args.s2s_name == 'cma'):
        all_ecmwf_dates = ["2016-01-04"] # the start date for ECMWF
        end_date = "2023-12-31" # the end date
        all_ecmwf_dates = _increment_days(all_ecmwf_dates, end_date)

        all_cma_dates = ["2019-12-02"] # the start date for CMA (previous dates are mostly dense)
        all_cma_dates = _increment_days(all_cma_dates, end_date)
    
    # Download the corresponding data based on year/month
    for year in config.YEARS:
        
        for month in config.MONTHS:
            
            logging.info(f'Downloading {year}/{month}...')
    
            # Skip downloading if .zarr files exist
            output_nc_file = output_dir / f'{args.s2s_name}_full_1.5deg_{year}{month}.nc'
            output_files = list(output_dir.glob(f'{args.s2s_name}_full_1.5deg_{year}{month}*.zarr'))

            if len(output_files) > 0:
                continue

            else:
                
                num_days = calendar.monthrange(int(year), int(month))[1]
                temp_files = list()
                is_exception = True
                
                # Construct date string: ECMWF is slightly different from the rest, with 3/4-days interval...
                
                if (args.s2s_name == 'ecmwf') and (f'{year}-{month}' in config.ECMWF_EXCEPTIONS):
                    date_str = config.ECMWF_EXCEPTIONS[f'{year}-{month}']
                    
                elif (args.s2s_name == 'ecmwf') and (any(f'{year}-{month}' in curr_date for curr_date in all_ecmwf_dates)):
                    curr_mmyy_dates = [curr_mmyy_date for curr_mmyy_date in all_ecmwf_dates if f'{year}-{month}' in curr_mmyy_date]
                    date_str = "/".join(curr_mmyy_dates)
                
                elif (args.s2s_name == 'ukmo') and (f'{year}-{month}' in config.UKMO_EXCEPTIONS):
                    date_str = config.UKMO_EXCEPTIONS[f'{year}-{month}']
                
                elif (args.s2s_name == 'cma') and (f'{year}-{month}' in config.CMA_EXCEPTIONS):
                    date_str = config.CMA_EXCEPTIONS[f'{year}-{month}']

                elif (args.s2s_name == 'cma') and (any(f'{year}-{month}' in curr_date for curr_date in all_cma_dates)):
                    curr_mmyy_dates = [curr_mmyy_date for curr_mmyy_date in all_cma_dates if f'{year}-{month}' in curr_mmyy_date]
                    date_str = "/".join(curr_mmyy_dates)

                else:
                    date_str = f"{year}-{month}-01/to/{year}-{month}-{num_days}"
                    is_exception = False

                for i, (param, level) in enumerate(config.S2S_PARAM_LEVEL.items()):

                    temp_file = output_dir / f"temp_{args.s2s_name}_{i}.grib"

                    server.retrieve({
                        "class": "s2",
                        "dataset": "s2s",
                        "date": date_str,
                        "expver": "prod",
                        "levelist": level,
                        "levtype": "pl",
                        "model": "glob",
                        "origin": center_id,
                        "param": param,
                        "step": config.STEPS,
                        "stream": "enfo",
                        "time": "00:00:00",
                        "type": "cf",
                        "target": str(temp_file)
                    })

                    temp_files.append(temp_file)

                # Combine and post-process each individual temp file (for now only 3)...
                assert len(temp_files) == 3
                temp_ds_0 = xr.open_dataset(temp_files[0], engine='cfgrib')
                temp_ds_1 = xr.open_dataset(temp_files[1], engine='cfgrib')
                temp_ds_2 = xr.open_dataset(temp_files[2], engine='cfgrib')
                temp_ds_2 = temp_ds_2.expand_dims({"isobaricInhPa": [temp_ds_2.isobaricInhPa.item()]})

                # Merge and perform post-porocessing (eg. change variable/coordinate names to match ERA5)
                full_ds = xr.merge([temp_ds_0, temp_ds_1, temp_ds_2])
                full_ds = full_ds.transpose('time', 'step', 'isobaricInhPa', 'latitude', 'longitude')
                full_ds = full_ds.rename({'isobaricInhPa': 'level'})
                full_ds = full_ds.rename_vars({'gh': 'z'})
                
                ## Extra post-processing with ECMWF: densify sparse dates
                dense_days = pd.date_range(f"{year}-{month}-01", f"{year}-{month}-{num_days}")
                full_ds = full_ds.reindex(time=dense_days)

                # Save as NetCDF file
                full_ds.to_netcdf(output_nc_file)

                # Remove temp files (including idx files left behind by GRIB operation)
                for temp_file in temp_files:
                    temp_file.unlink()

                idx_files = list(output_dir.glob('*.idx'))
                for idx_file in idx_files:
                    idx_file.unlink()
                    
                # Break down into daily .zarr (cloud-optimized)
                ds = xr.open_dataset(output_nc_file)
                n_timesteps = len(ds.time)
                
                for n_idx in range(n_timesteps):
                    subset_ds = ds.isel(time=n_idx)
                    yy, mm, dd = ds.time[n_idx].dt.strftime('%Y-%m-%d').item().split('-')
                    
                    # Intermediate processing for missing days: fix valid_steps
                    if is_exception:
                        valid_steps = pd.date_range(
                            start=subset_ds.time.item(), 
                            periods=len(subset_ds.step), 
                            freq='D'
                        ).to_numpy().astype('datetime64[ns]')
                        
                        da = xr.DataArray(valid_steps, dims=['step'])
                        subset_ds['valid_time'] = da
                        
                    output_daily_file = output_dir / f'{args.s2s_name}_full_1.5deg_{yy}{mm}{dd}.zarr'
                    subset_ds.to_zarr(output_daily_file)
                
                output_nc_file.unlink()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--s2s_name', help='Provide the name of the S2S center...')
    
    args = parser.parse_args()
    main(args)
