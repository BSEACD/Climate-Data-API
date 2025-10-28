import os
import sys
import logging
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from data_processing import calculate_stats, climate_data, raster_clip, prism_download, data_processing


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


def main():
    while True:
        '''
        Need to include validation checks for user input to ensure inputs can be
        handled by the rest of the program
        '''
        # Define variables and paths
        # url = https://services.nacse.org/prism/data/get/<region>/<resolution>/<element>/<date>
        base_url = 'https://services.nacse.org/prism/data/get/us/800m'
        climate = input('Enter the climate variable to download: ').lower()        
        unit = input('\nEnter the desired unit of measurement (ex. in or mm): ').lower()        
        resolution = input('\nEnter the desired resolution (daily or monthly): ').lower()        
        outPath = input('\nEnter the directory path to store downloaded data: ')        
        shapefile = input('\nEnter the directory path to the shapefile for raster clipping. Include filename and extension: ')       
        csv_dir = input('\nEnter the directory path to store the generated CSV file: ')       
        csv_file = input('\nEnter the name of the CSV file to save the downloaded data. Include filename and extension: ')

        if not csv_file.lower().endswith('.csv'):  # Checks for a csv file extension
            csv_file += '.csv'  # Adds csv ext. if one was not input by user

        # Create output directories if they don't exist
        os.makedirs(outPath, exist_ok=True)
        os.makedirs(csv_dir, exist_ok=True)

        subDirectory_map = {
                    '.tif': os.path.join(outPath, 'geotiffs'),
                    '.prj': os.path.join(outPath, 'metadata'),
                    '.xml': os.path.join(outPath, 'metadata'),
                    '.stx': os.path.join(outPath, 'metadata'),
                    '.csv': os.path.join(outPath, 'metadata'),
                    '.txt': os.path.join(outPath, 'metadata'),
                    '.zip': os.path.join(outPath, 'downloads')
                }
        # Create subdirectories if they don't exist
        for dirPath in subDirectory_map.values():
            os.makedirs(dirPath, exist_ok=True)

        ante = ''

        # Code block to direct program based on the input resolution and climate
        if resolution == 'daily':
            startDate = str(input('\nEnter the start date for the download (YYYY-MM-DD): '))
            endDate = str(input('\nEnter the end date for the download (YYYY-MM-DD): '))
            if climate == 'ppt':
                print('\nYou are attempting to process precipitation data.')
                ante = input('Calculate a normalized antecedent precipitation index (NAPI)? (y/n): ')
                if ante in ('y', 'yes'):
                    # Adjust startDate to capture 30 days before requested time series 
                    startDate_dt = datetime.strptime(str(startDate), '%Y-%m-%d').date()
                    endDate_dt = datetime.strptime(str(endDate), '%Y-%m-%d').date()
                    startDate_ante = startDate_dt - timedelta(days=30)
                    startDate = startDate_ante.strftime('%Y-%m-%d')
                    print(f'Start date adjusted to {startDate} for antecedent conditions.')
                    logger.info(f'Start date adjusted to {startDate} for antecedent conditions.')
                    endDate = endDate_dt.strftime('%Y-%m-%d')
                else:
                    pass
            else:
                pass
        elif resolution == 'monthly':
            startDate = str(input('\nEnter the start date for the download (YYYY-MM): '))
            endDate = str(input('\nEnter the end date for the download (YYYY-MM): '))

            if climate == 'ppt':
                print('\nYou are attempting to process precipitation data.')
                ante = input('Calculate a normalized antecedent precipitation index (NAPI)? (y/n): ')
                if ante in ('y', 'yes'):
                    # Adjust startDate to capture 90 days before requested time series 
                    startDate_dt = datetime.strptime(str(startDate), '%Y-%m').date()
                    endDate_dt = datetime.strptime(str(endDate), '%Y-%m').date()
                    startDate_ante = startDate_dt - relativedelta(months=3)
                    startDate = startDate_ante.strftime('%Y-%m')
                    print(f'\nStart date adjusted to {startDate} for antecedent conditions.')
                    logger.info(f'Start date adjusted to {startDate} for antecedent conditions.')
                    endDate = endDate_dt.strftime('%Y-%m')
                else:
                    pass
            else:
                pass

        try:
            data_processing(base_url, climate, resolution, unit, outPath, csv_dir, csv_file, startDate, endDate, subDirectory_map, shapefile, ante)
        except Exception as e:
            error_msg = f'An unexpected error occurred while attempting to run the program: {e}'
            print(error_msg)
            logger.error(error_msg)
            raise

        restart = input('\nDo you want to run the program again? (y/n): ').lower()
        if restart not in ('y', 'yes'):
            print('Exiting climate data API program.')
            break
        
if __name__ == '__main__':
    main()
