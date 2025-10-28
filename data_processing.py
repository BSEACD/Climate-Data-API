import pandas as pd
import requests
import numpy as np
import rasterio
from rasterio.mask import mask
import geopandas as gpd
from shapely.geometry import mapping
import os
import csv
import logging
import traceback
import sys
from datetime import datetime, timedelta, date
from napi import ante_conditions, calculate_napi
from utils import function_divider, progress_bar, parse_filename, file_sorter, zip_extract, file_mover

# Datetime objects for the current date and time
now = datetime.now()
todays_date = now.strftime('%b %d, %Y')
time_now = now.strftime('%I:%M:%S %p')
# For use in filenames
filename_date = now.strftime('%m-%d-%Y')
filename_time = now.strftime('%H.%M.%S')

# --- Logging Setup --- #
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s'
LOG_FILE = f'prism_download_log_{filename_date}_{filename_time}.log'

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, filename=LOG_FILE)
logger = logging.getLogger(__name__)
# --- End Logging Setup --- #

## Data Transformation and Calculation Functions


def calculate_stats(data_series, climate, unit, decimals=2):
    '''
    Calculates statistics and converts units based on the climate variable.
    Returns: A dictionary of statistics.
    '''
    stats = {}
    # Initialize all calculation variables to None or 0 to avoid NameError later
    if data_series.size == 0 or np.all(np.isnan(data_series)):
        return {
            'min': np.nan, 'max': np.nan, 'mean': np.nan, 'median': np.nan
        }
    cmin = np.min(data_series)
    cmax = np.max(data_series)
    cmean = np.mean(data_series)
    cmed = np.median(data_series)
    # NumPy operaters = &(AND), |(OR)
    # Calculate statistics for valid data
    if climate == 'ppt':
        # Prism downloads data in Millimeters
        if unit == 'in':
            stats['min'] = round(cmin / 25.40, decimals)
            stats['max'] = round(cmax / 25.40, decimals)
            stats['mean'] = round(cmean / 25.40, decimals)
            stats['median'] = round(cmed / 25.40, decimals)
        else:  # Default to mm
            stats['min'] = round(cmin, decimals)
            stats['max'] = round(cmax, decimals)
            stats['mean'] = round(cmean, decimals)
            stats['median'] = round(cmed, decimals)
    elif climate.startswith('t'):  # Handles tmean, tmax, tmin, tdmean
        # Prism downloads data in Celsius
        if unit == 'f':
            stats['min'] = round((cmin * 9/5) + 32, decimals)
            stats['max'] = round((cmax * 9/5) + 32, decimals)
            stats['mean'] = round((cmean * 9/5) + 32, decimals)
            stats['median'] = round((cmed * 9/5) + 32, decimals)
        else: # Default to c
            stats['min'] = round(cmin, decimals)
            stats['max'] = round(cmax, decimals)
            stats['mean'] = round(cmean, decimals)
            stats['median'] = round(cmed, decimals)
    else:
        # Generic calculation if climate variable isn't specifically handled
        logger.warning(f'Climate variable "{climate}" not explicitly handled. Calculating generic statistics.')
        stats['min'] = round(cmin, decimals)
        stats['max'] = round(cmax, decimals)
        stats['mean'] = round(cmean, decimals)
        stats['median'] = round(cmed, decimals)
    return stats
    
def climate_data(clippedPath, csv_dir, csv_file, clipped_rasters, climate, resolution, unit, decimals):
    '''
    Saves extracted statistical data from a series of clipped raster files (.tif) and saves data into a CSV file
    '''
    # Setup the output directory for the CSV file if it doesn't already exist
    os.makedirs(csv_dir, exist_ok=True)
    # Create full path to the CSV file
    csvPath = os.path.join(csv_dir, csv_file)
    # Map climate units to the correct header fieldnames
    unit_map = {
        'in': ['Min. (in)', 'Max. (in)', 'Arithmetic Mean (in)', 'Median (in)'],
        'mm': ['Min. (mm)', 'Max. (mm)', 'Arithmetic Mean (mm)', 'Median (mm)'],
        'f': ['Min. (F)', 'Max. (F)', 'Arithmetic Mean (F)', 'Median (F)'],
        'c': ['Min. (C)', 'Max. (C)', 'Arithmetic Mean (C)', 'Median (C)']
    }
    fieldnames = ['Filename', 'Climate Variable', 'Date']
    # Dynamically get the correct stat columns based on the unit
    stat_columns = unit_map.get(unit, ['Min.', 'Max.', 'Arithmetic Mean', 'Median'])
    fieldnames.extend(stat_columns)
    try:
        with open(csvPath, 'w', newline='\n', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            logger.info(f'CSV header written to: {csvPath}')
            raster_total = len(clipped_rasters)
            # Initial progress bar for the start
            progress_bar(0, raster_total, prefix=f'Starting data save to CSV file', suffix='Ready\n')
            for i, raster in enumerate(clipped_rasters):
                try:
                    with rasterio.open(raster) as src:
                        data = src.read(1)  # Reads first band only
                        '''
                        Create user input for no data source?
                        '''
                        nodata_mask = src.nodata
                        # Get scale and offset from raster metadata
                        scale_factor = src.scales[0] if src.scales else 1.0
                        offset_value = src.offsets[0] if src.offsets else 0.0
                        # Apply scale and offset to the raw data *first* if they exist and are not default
                        if scale_factor != 1.0 or offset_value != 0.0:
                            clean_data = (data * scale_factor) + offset_value
                            print(f'Applied scale {scale_factor} and offset {offset_value}.')
                        # Mask out nodata values after scaling
                        if nodata_mask is not None:
                            if isinstance(nodata_mask, (int, float)) and not np.isnan(nodata_mask):
                                # Handle specific numeric nodata values (e.g., -9999.0)
                                data_series = data[data != nodata_mask].flatten()
                            elif np.isnan(nodata_mask):
                                # Handle NaN nodata values (if src.nodata is NaN)
                                data_series = data[~np.isnan(data)].flatten()
                            else:
                                # Fallback if nodata_mask is unexpected type
                                data_series = data.flatten()
                                print(f'Unexpected nodata_mask type ({type(nodata_mask)}) for {os.path.basename(raster)}. Proceeding without specific nodata filter.')
                                logger.warning(f'Unexpected nodata_mask type ({type(nodata_mask)}) for {os.path.basename(raster)}. Proceeding without specific nodata filter.')
                        else:
                            data_series = data.flatten() # No specific nodata value defined
                        data_series = data_series.astype(np.float64) # Use float64 for better precision
                        if data_series.size == 0 or np.all(np.isnan(data_series)):
                            logger.warning(f'All valid data is NoData/NaN in {os.path.basename(raster)}. Skipping.')
                            progress_bar(i + 1, raster_total, prefix=f'Saving {climate} data: {i + 1} of {raster_total}', suffix=f'Skipped (NoData)')
                            continue
                        # Calculate statistics using the helper function
                        stats = calculate_stats(data_series, climate, unit, decimals)
                        # Parse filename for CSV using the helper function
                        variable, _, _, formatted_date = parse_filename(os.path.basename(raster))
                        # If parsing failed, use a generic fallback for filename and date
                        if not variable:
                            filename = os.path.basename(raster)
                            parts = filename.split('_')
                            if len(parts) >= 5:
                                variable = parts[1]
                                resolution = parts[2]
                                formatted_date = parts[3]
                            else:
                                variable = climate
                                formatted_date = 'unknown'
                                resolution = 'unknown'
                        raster_filename = os.path.basename(raster)
                        # Prepare the row dictionary matching the fieldnames
                        row = {
                            'Filename': raster_filename,
                            'Climate Variable': climate,
                            'Date': formatted_date
                        }
                        # Use the dynamically created list of column names
                        row[stat_columns[0]] = stats['min']
                        row[stat_columns[1]] = stats['max']
                        row[stat_columns[2]] = stats['mean']
                        row[stat_columns[3]] = stats['median']
                     
                        writer.writerow(row)
                        info_msg = f'Data from "{raster_filename}" has been saved to "{os.path.basename(csvPath)}"'
                        print(info_msg)
                        logger.info(info_msg)
                        # Update the overall progress 
                        progress_bar(i + 1, raster_total, prefix=f'Saving {climate} data: {i + 1} of {raster_total}', suffix=f'Raster data saved')
                except rasterio.errors.RasterioIOError as e:
                    error_msg = f'Error opening raster {os.path.basename(raster)}: {e}'
                    print(error_msg)
                    logger.error(error_msg)
                    logger.exception(e)
                    raise
                except Exception as process_error:
                    error_msg = f'Error processing raster {os.path.basename(raster)}: {process_error}'
                    print(error_msg)
                    logger.error(error_msg)
                    logger.exception(process_error)
                    raise
    except (IOError, FileNotFoundError) as writing_error:
        error_msg = f'Failed to write to CSV file: {writing_error}'
        print(error_msg)
        logger.error(error_msg)
        logger.exception(writing_error)
        raise
    except Exception as csv_error:
        error_msg = f'Failed to save data to CSV file: {csv_error}'
        print(error_msg)
        logger.error(error_msg)
        logger.exception(csv_error)
        raise
        
def raster_clip(rasters, shapefile, clippedPath, resolution):
    '''
    Clips a GeoTIFF raster using a district analysis area boundary shapefile as the raster extent
    '''
    clipped_rasters_list = []
    try:
        # Read the shapefile
        gdf = gpd.read_file(shapefile)
        logger.info(f'Shapefile loaded: {shapefile}')
        # Ensure geotiffPath is a list
        if not isinstance(rasters, list):
            rasters = [rasters]
        tif_total = len(rasters)
        # Initial progress bar for the start
        progress_bar(0, tif_total, prefix=f'Clipping rasters to analysis area extent', suffix='Ready\n')
        for i, raster in enumerate(rasters):
            if not raster.lower().endswith('.tif'):  # Checks to only process .tif files
                logger.info(f'Skipping non-TIFF file: {raster}')
                continue
            try:
                # Use rasterio to transform raster to the shapefile extent
                with rasterio.open(raster) as src:
                    logger.info(f'Opened raster: {raster}')
                    # Ensure the shapefile and raster have the same CRS
                    gdf = gdf.to_crs(src.crs)
                    logger.info(f'Shapefile CRS set to raster CRS: {src.crs}')
                    # Get the geometry from the GeoDataFrame
                    geometries = [mapping(shape) for shape in gdf.geometry]

                    # Clip the raster
                    out_raster, out_transform = mask(src, geometries, crop=True)
                    metadata = src.meta.copy()
                    # Update the metadata
                    metadata.update({
                        'driver': 'GTiff',
                        'height': out_raster.shape[1],
                        'width': out_raster.shape[2],
                        'transform': out_transform
                    })
                    # Extract parts from filename
                    filename = os.path.basename(raster)
                    parts = filename.split('_')
                    if len(parts) >= 5:
                        source = parts[0]
                        climate_var = parts[1]
                        region = parts[2]
                        resolution = parts[3]
                        date = parts[4].split('.')[0]  # removes extension
                        rasterName = f'{source}_{climate_var}_{resolution}_{date}_clip.tif'
                    else:
                        rasterName = f'{os.path.splitext(filename)[0]}_clip.tif'
                        logger.warning(f'Could not parse filename for clipped output: {filename}')
                        
                    rasterPath = os.path.join(clippedPath, rasterName)
                    # Write the clipped raster to a new file
                    with rasterio.open(rasterPath, 'w', **metadata) as dest:
                        dest.write(out_raster)
                        info_msg = f'Successfully clipped "{os.path.basename(raster)}"'
                        print(info_msg)
                        logger.info(info_msg)
                        clipped_rasters_list.append(rasterPath)
                    # Update progress 
                    progress_bar(i + 1, tif_total, prefix=f'Clipping rasters: {i + 1} of {tif_total}', suffix=f'Rasters clipped')
            
            except Exception as clip_error:
                error_msg = f'\nError clipping {raster}: {clip_error}'
                print(error_msg)
                logger.error(error_msg)
                logger.exception(clip_error)
                raise
        logger.info(f'Raster clipping process completed. Clipped rasters: {clipped_rasters_list}')
        return clipped_rasters_list
    except Exception as rasterClip_error:
        error_msg = f'\nFailed to clip {rasters}: {rasterClip_error}'
        print(error_msg)
        logger.error(error_msg)
        logger.exception(rasterClip_error)
        raise
        return []

def prism_download(climate, startDate, endDate, outPath, url, currentDate_dt, resolution):
    '''
    Downloads daily or monthly PRISM climate data for a specified date range as a zipfile 
    Returns a single zipfile (one grid) per request
    '''
    # Download daily or monthly climate data for a specified date range using requests library
    try:
        print(f'\nRetrieving PRISM {climate} data for {currentDate_dt}...')
        logger.info(f'Retrieving PRISM {climate} data for {currentDate_dt} from {url}')
        download = requests.get(url, timeout=30)
        for i, file in enumerate(download):
            if download.status_code == 200:
                filename = f'prism_{climate}_{resolution}_{startDate}.zip'
                filepath = os.path.join(os.getcwd(), filename)
                with open(filepath, 'wb') as f:
                    f.write(download.content)
                print(f'Downloaded "{filename}" file for {currentDate_dt}')
                logger.info(f'Downloaded {filename} for {currentDate_dt} to {filepath}')
                return [filename]
            else:
                error_msg = f'Error downloading data for {currentDate_dt} (status code: {download.status_code})'
                print(error_msg)
                logger.error(error_msg)
                return []
    except Exception as download_error:
        error_msg = f'Failed to download data for {currentDate_dt}: {download_error}'
        print(error_msg)
        logger.error(error_msg)
        logger.exception(download_error)
        raise
        return []
    
def data_processing(base_url, climate, resolution, unit, outPath, csv_dir, csv_file, startDate, endDate, subDirectory_map, shapefile, ante):
    '''
    Downloads various PRISM climate data, organizes downloaded files to sub-directories,
    clips rasters to District analysis area boundaries, and saves extracted data to a CSV file
    '''
    # Transform climate variables for accessibility
    if climate == 'ppt':
        climate_str = 'precipitation'
    elif climate == 'tmean':
        climate_str = 'mean temperature'
    elif climate == 'tmax':
        climate_str = 'max temperature'
    elif climate == 'tmin':
        climate_str = 'minimum temperature'
    elif climate == 'tdmean':
        climate_str = 'mean dew point temperature'
##    elif climate == 'vpdmin':
##        climate_str = 'Minimum vapor pressure deficit'
##    elif climate == 'vpdmax':
##        climate_str = 'Max vapor pressure deficit'
##    elif climate in ('soltotal', 'solslope', 'solclear', 'soltrans'): 
##        climate_str = 'Solar Radiation'  # Unified name for solar variables
##    elif climate in ('pet', 'et'):
##        climate_str = 'Potential Evapotranspiration'
    else:
        error_msg = f'Please input a valid PRISM climate variable: {climate} is not available.'
        print(error_msg)
        logger.error(error_msg)
        return  # Exit if climate variable is invalid
    
    print(f'\n*** Starting processing of {resolution} PRISM {climate_str} data from {startDate} through {endDate} ***\n')
    logger.info(f'Starting processing of {resolution} PRISM {climate_str} data from {startDate} through {endDate}')

    # Set up data containers and trackers
    if climate == 'ppt':
        step_total = 7  # 1. download data; 2. move zipped folders; 3. unzip data folders; 4. sort extracted data files; 5. clip rasters; 6. save data to csv file; 7. antecedent conditions calculations
    else:
        step_total = 6
    current_step = 0
    successful_download = True  
    try:
        downloaded_files = []  # To track files for moving
        while True:
            # Create portion of program that downloads PRISM climate data
            download = input(f'Download new PRISM {climate_str} data? (y/n): ').lower()
            if download in ('y', 'yes'):
                current_step += 1 
                print(f'\n> Step {current_step} of {step_total}: Download PRISM climate data')
                logger.info(f'Step {current_step} of {step_total}: Download PRISM climate data')
                logger.info(f'Attempting to download PRISM {climate_str} data files...')
                    
                # Handle different resolution requests
                if resolution == 'daily':
                    startDate_dt = datetime.strptime(str(startDate), '%Y-%m-%d').date()  # Parse to datetime, then get the date
                    endDate_dt = datetime.strptime(str(endDate), '%Y-%m-%d').date()
                    total_days = (endDate_dt - startDate_dt).days + 1
                    print(f'\nTotal requested daily downloads: {total_days}')
                    logger.info(f'Total requested daily downloads: {total_days}')

                    # Initial progress bar for the start of extraction
                    progress_bar(0, total_days, prefix=f'Starting {climate_str} daily data download', suffix='Ready')
                    
                    # Set up while loop containers and trackers
                    currentDate_dt = startDate_dt
                    download_count = 0  # Initialize a counter for downloads
                    while currentDate_dt <= endDate_dt:
                        currentDate_str = currentDate_dt.strftime('%Y%m%d')  # Converts date object back to a string
                        # url = <base_url>/<element>/<date>
                        url = f'{base_url}/{climate}/{currentDate_str}'
                        data_download = prism_download(climate, currentDate_str, currentDate_str, outPath, url, currentDate_dt, resolution)  # downloads one day at a time
                        if data_download:
                            download_count += 1  # Increment the counter on successful (non-None) download
                            downloaded_files.extend(data_download) # Track successfully downloaded files
                        else:
                            successful_download = False

                        # Update progress
                        progress_bar(download_count, total_days, prefix=f'Daily data download: {download_count} of {total_days}', suffix=f'- Downloading')
                        currentDate_dt += timedelta(days=1)

                elif resolution == 'monthly':
                    startDate_dt = datetime.strptime(str(startDate), '%Y-%m').date()  # Parse to datetime, then get the date
                    endDate_dt = datetime.strptime(str(endDate), '%Y-%m').date()
                    current_date_dt = startDate_dt
                    end_date_dt = endDate_dt
                    total_months = 0
                    download_count = 0
                    while current_date_dt <= end_date_dt:
                        total_months += 1
                        current_date_dt += relativedelta(months=1)
                    print(f'\nTotal requested monthly downloads: {total_months}')
                    logger.info(f'Total requested monthly downloads: {total_months}')

                    current_date_dt = startDate_dt
                    progress_bar(0, total_months, prefix=f'Starting {climate_str} monthly data download', suffix='Ready')

                    while current_date_dt <= end_date_dt:
                        current_date_str = current_date_dt.strftime('%Y%m')  # Format for monthly
                        url = f'{base_url}/{climate}/{current_date_str}'
                        data_download = prism_download(climate, current_date_str, current_date_str, outPath, url, current_date_dt, resolution)
                        if data_download:
                            download_count += 1
                            downloaded_files.extend(data_download)
                        else:
                            successful_download = False
                        progress_bar(download_count, total_months, prefix=f'Monthly data download: {download_count} of {total_months}', suffix=f'- Downloading')
                        current_date_dt += relativedelta(months=1)

                function_divider(current_step, step_total, prefix='Data download complete', suffix='of program completed')
                break
            
            elif download in ('n', 'no'):
                current_step += 1
                info_msg = f'Skipping step {current_step} of {step_total}: Data download...'
                print(info_msg)
                logger.info(info_msg)
                break
            else:
                warning_msg = f'"{download}" input is not valid. Please enter either yes or no (y/n).'
                print(warning_msg)
                logger.warning(warning_msg)
                
        # Create portion of the program that moves downloaded files to specified directory path
        current_step += 1
        print(f'\n> Step {current_step} of {step_total}: Move downloaded files')
        logger.info(f'Step {current_step} of {step_total}: Move downloaded files')
        if downloaded_files:
            download_dir = subDirectory_map.get('.zip')
            if os.path.abspath(outPath) == os.path.abspath(os.getcwd()):
                print(f'Output directory {outPath} is the same as the current directory. Skipping move.')
                logger.info(f'Output directory {outPath} is the same as the current directory. Skipping move.')
            else:
                print(f'User specified directory is different from the current directory. Moving downloads to the user specified directory: {outPath}.')
                logger.info(f'User specified directory is different from the current directory. Moving downloads to: {outPath}.')
                logger.info(f'Attempting to move downloaded files to {outPath}...')

                os.makedirs(download_dir, exist_ok=True)
                for datum in downloaded_files:
                    sourceFile = os.path.join(os.getcwd(), datum)
                    if os.path.exists(sourceFile):
                        file_mover(sourceFile, download_dir, datum, outPath)
                        print(f'\n{climate_str} data file "{datum}" moved successfully.')
                        logger.info(f'{climate_str} data file "{datum}" moved successfully.')
                    else:
                        warning_msg = f'\nWarning: Downloaded file "{datum}" not found for moving.'
                        print(warning_msg)
                        logger.warning(warning_msg)
                    
                function_divider(current_step, step_total, prefix='File move completed', suffix='of program completed')
            
            if not successful_download:
                print(f'\n*** NOTE: Some downloads failed. Successfully moved available files. ***')
                logger.warning(f'Some downloads failed, but available files were moved.')
        else: # This block now covers the case where 'downloaded_files' is an empty list
            warning_msg = f'Warning: No files were successfully downloaded, and no files were moved.'
            print(warning_msg)
            logger.error(warning_msg)
        if download in ('y', 'yes') and download_files is not None:
            while True:
                restart = input('\nDo you want to restart the program? (y/n): ').lower()
                if restart in ('yes', 'y'):
                    return 'restart'
                elif restart in ('no', 'n'):
                    info_msg = f'Skipping step {current_step} of {step_total}: Move downloaded files...'
                    print(info_msg)
                    logger.info(info_msg)
                    break  # Exit the loop
                else:
                    print('Invalid input. Please enter yes or no (y/n).')
        else:
            pass
        
        # Create portion of the program that extracts all downloaded zip files
        current_step += 1
        # if download == yes, automatically extract downloaded files
        if download in ('y', 'yes'):
            print(f'\n> Step {current_step} of {step_total}: Extract ZIP files')
            logger.info(f'Step {current_step} of {step_total}: Extract ZIP files')
            logger.info(f'Attempting to unzip downloaded data files in {outPath}...')
            zip_extract(outPath)
            
            function_divider(current_step, step_total, prefix='File extraction completed', suffix='of program completed')
            # if download == yes, automatically sort extracted files
            current_step += 1
            print(f'\n> Step {current_step} of {step_total}: Sorting files')
            logger.info(f'Step {current_step} of {step_total}: Sorting files')
            logger.info(f'Attempting to sort extracted data files into sub-directories...')
            file_sorter(outPath, subDirectory_map)
                        
            function_divider(current_step, step_total, prefix='File sorting completed', suffix='of program completed')

        elif download in ('n', 'no'):
            while True:
                extract = input('\nDoes the dataset contain zipped files to extract? (y/n): ').lower()
                if extract in ('y', 'yes'):
                    print(f'\n> Step {current_step} of {step_total}: Extract ZIP files')
                    logger.info(f'Step {current_step} of {step_total}: Extract ZIP files')
                    logger.info(f'Attempting to unzip downloaded data files in {outPath}...')
                    zip_extract(outPath)
                    
                    function_divider(current_step, step_total, prefix='File extraction completed', suffix='of program completed')
                    # if extract == yes, automatically sort extracted files
                    current_step += 1
                    print(f'\n> Step {current_step} of {step_total}: Sorting files')
                    logger.info(f'Step {current_step} of {step_total}: Sorting files')
                    logger.info(f'Attempting to sort extracted data files into sub-directories...')
                    file_sorter(outPath, subDirectory_map)
                                
                    function_divider(current_step, step_total, prefix='File sorting completed', suffix='of program completed')
                    break  # Exit loop after a valid 'yes' or 'y' input
            
                elif extract in ('n', 'no'):
                    info_msg = f'Skipping step {current_step} of {step_total}: File extraction...'
                    print(info_msg)
                    logger.info(info_msg)
                    break
                else:
                    warning_msg = f'"{extract}" input is not valid. Please enter either yes or no (y/n).'
                    print(warning_msg)
                    logger.warning(warning_msg)
            # Create portion of program that sorts all files in the outPath directory into subdirectories
            current_step += 1
            while True:
                sort = input('\nDoes the dataset contain files to sort? (y/n): ').lower()
                if sort in ('y', 'yes'):
                    print(f'\n> Step {current_step} of {step_total}: Sorting files')
                    logger.info(f'Step {current_step} of {step_total}: Sorting files')
                    logger.info(f'Attempting to sort extracted data files in {outPath}...')
                    file_sorter(outPath, subDirectory_map)

                    function_divider(current_step, step_total, prefix='File sorting completed', suffix='of program completed')
                    break
                
                elif sort in ('n', 'no'):
                    info_msg = f'Skipping step {current_step} of {step_total}: File sorting...'
                    print(info_msg)
                    logger.info(info_msg)
                    break
                else:
                    warning_msg = f'"{sort}" input is not valid. Please enter either yes or no (y/n).'
                    print(warning_msg)
                    logger.warning(warning_msg)
                    # The loop will automatically restart here, asking for input again.
          
        # Create portion of the program that clips each geotiff raster extent to the District analysis area boundary
        current_step += 1
        while True:
            clip = input('\nDoes the raster dataset need to be clipped to a different extent? (y/n): ').lower()
            if clip in ('y', 'yes'):
                print(f'\n> Step {current_step} of {step_total}: Clipping rasters')
                logger.info(f'Step {current_step} of {step_total}: Clipping rasters')
                logger.info(f'Attempting to clip GeoTIFF rasters...')
                processedPath = os.path.join(outPath, 'clipped')
                # Setup the output directory for the CSV file if it doesn't already exist
                os.makedirs(processedPath, exist_ok=True)
                shapefile = shapefile
                if not shapefile.lower().endswith('.shp'):  # Checks for a shapefile extension
                    shapefile += '.shp'  # Adds shp ext. if one was not input by user

                geotiffPath = os.path.join(outPath, 'geotiffs')
                geotiff_rasters = [os.path.join(geotiffPath, f) for f in os.listdir(geotiffPath) if f.lower().endswith('.tif')]
                if shapefile and os.path.exists(shapefile) and geotiff_rasters:
                    processed_rasters = raster_clip(geotiff_rasters, shapefile, processedPath, resolution)
                else:
                    warning_msg = f'Warning: Shapefile is missing OR GeoTIFF rasters were not found in {geotiffPath}'
                    print(warning_msg)
                    logger.warning(warning_msg)
                    processed_rasters = []
                    
                function_divider(current_step, step_total, prefix='Raster clipping completed', suffix='of program completed')
                break
            elif clip in ('n', 'no'):
                info_msg = f'Skipping step {current_step} of {step_total}: Raster clipping...'
                print(info_msg)
                logger.info(info_msg)
                processedPath = os.path.join(outPath, 'geotiffs')
                processed_rasters = [os.path.join(processedPath, f) for f in os.listdir(processedPath) if f.lower().endswith('.tif')]
                break
            else:
                warning_msg = f'"{clip}" input is not valid. Please enter either yes or no (y/n).'
                print(warning_msg)
                logger.warning(warning_msg)
    
        # Create portion of the program that saves processded data to a CSV file
        current_step += 1
        print(f'\n> Step {current_step} of {step_total}: Saving {climate_str} data')
        logger.info(f'Step {current_step} of {step_total}: Saving {climate_str} data')
        logger.info(f'Attempting to save processed PRISM {climate_str} data to {csv_dir}/{csv_file}...')
        decimals = 2  # For rounding
        climate_data(processedPath, csv_dir, csv_file, processed_rasters, climate, resolution, unit, decimals)
        
        function_divider(current_step, step_total, prefix=f'{climate} data saved successfully', suffix='of program completed')

        # Create portion of program that calculates antecedent indices
        ppt_field = 'Arithmetic Mean (mm)' if unit == 'mm' else 'Arithmetic Mean (in)'
        date_field = 'Date'
        if climate == 'ppt':
            current_step += 1
            print(f'\n> Step {current_step} of {step_total}: Calculating a NAPI')
            logger.info(f'Step {current_step} of {step_total}: Calculating a NAPI')
            logger.info(f'Attempting to calculate antecedent conditions...')
            ante_conditions(csv_dir, csv_file, ppt_field, date_field, resolution, decimals)
            
            function_divider(current_step, step_total, prefix=f'Calculated Normalized Antecedent Precipitation Index', suffix='program complete')
        else:
            pass

        print(f'\n*** Processing of {resolution} PRISM {climate_str} data from {startDate} through {endDate} is complete. ***')
        logger.info(f'Processing of {resolution} PRISM {climate_str} data from {startDate} through {endDate} is complete.')
        
    except Exception as overall_error:
        error_msg = f'Failed to process PRISM {climate_str} data: {overall_error}'
        print(error_msg)
        logger.error(error_msg)
        logger.exception(overall_error)
        raise
