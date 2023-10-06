import xarray as xr
import cdsapi
from pathlib import Path
import config

import logging
logging.basicConfig(level=logging.INFO)

def main():
    """
    Main driver to download ERA5 data based on individual variable
    """
    # Initialize CDS API
    c = cdsapi.Client()
    
    # Set output directory
    output_dir = Path(config.DATA_DIR) / 's2s' / 'era5'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Download the corresponding data based on year/month
    for year in config.ERA5_YEARS:
        
        for month in config.MONTHS:
            
            logging.info(f'Downloading {year}/{month}...')
            
            output_file = output_dir / f'era5_full_1.5deg_{year}{month}.nc'
            
            # Skip downloading if file exists 
            if output_file.exists():
                pass
                
            else:
            
                c.retrieve(
                    'reanalysis-era5-pressure-levels',
                    {
                        'product_type': 'reanalysis',
                        'variable': config.VARIABLE_LIST,
                        'pressure_level': config.PRESSURE_LEVELS,
                        'year': year,
                        'month': month,
                        'day': config.DAYS,
                        'time': '00:00',
                        'grid': ['1.5', '1.5'],
                        'format': 'netcdf',
                    },
                    output_file)
                
                # Break down into daily .zarr (cloud-optimized)
                ds = xr.open_dataset(output_file)
                ds['z'] = ds['z'] / config.G_CONSTANT ## Convert to gpm
                n_timesteps = len(ds.time)
                
                ## list() over multiple days
                for n_idx in range(n_timesteps):
                    subset_ds = ds.isel(time=n_idx)
                    yy, mm, dd = ds.time[n_idx].dt.strftime('%Y-%m-%d').item().split('-')
                    output_daily_file = output_dir / f'era5_full_1.5deg_{yy}{mm}{dd}.zarr'
                    subset_ds.to_zarr(output_daily_file)
                
                output_file.unlink()

if __name__ == "__main__":
    main()
