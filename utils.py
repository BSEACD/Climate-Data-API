import os
import sys
import shutil
import zipfile
import traceback
import logging
import regex as re
from datetime import datetime, timedelta, date

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

## Helper and Utility Functions

def function_divider(current_step, step_total, prefix='', suffix='', decimals=1, length=20):
    '''
    Call anywhere in the script to create a visual divison in program functions
    @params:
        current_step   - Required  : current iteration (Int)
        step_total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
    '''
    percent = ('{0:.' + str(decimals) + 'f}').format(100 * (current_step / float(step_total)))
    #filledLength = int(length * iteration // total)
    #bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\n*****{prefix} - {percent}% {suffix}*****\n\n', end='\r')
    logger.info(f'{prefix} - {percent}% {suffix}')
    # Print New Line on Complete
    if current_step == step_total:
        print()

def progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=20, fill='█'):
    '''
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    '''
    if total == 0:
        print (f'\r{prefix}{suffix}', end='\r')
        return
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    sys.stdout.write(f'\n\r{prefix} |{bar}| {percent}% {suffix}\n')
    sys.stdout.flush()
    logger.info(f'{prefix} |{bar}| {percent}% {suffix}')
    # Print New Line on Complete
    if iteration == total:
        print()

def parse_filename(filename):
    '''
    Parses a PRISM filename to extract key components and reformat the date.
    Returns: A tuple (variable, resolution, formatted_date) or (None, None, None) on failure.
    '''
    # Extract full filename using regex library
    # Note: This regex has 4 CAPTURED groups: (Variable), (resolution), (region - Optional), (Date)
    pattern = r'prism_([a-zA-Z]+)(?:_([\da-zA-Z]+))?_([\da-zA-Z]+)_(\d{8}|\d{6})(?:_([\da-zA-Z]+))?.tif'
    match = re.search(pattern, filename)
    if match:
        logger.info('Regex matched successfully.')
        # Extract individual parts of the filename
        variable = match.group(1)
        region = match.group(2)
        resolution = match.group(3)
        date = match.group(4)
        logger.info(f'Extracted - variable: {variable}, region: {region}, resolution: {resolution}, date: {date}')
        # Apply date reformatting logic
        formatted_date = date
        try:
            if len(date) == 8:
                date_object = datetime.strptime(date, '%Y%m%d')
                # Format the date as YYYY-MM-DD
                formatted_date = date_object.strftime('%Y-%m-%d')
            elif len(date) == 6:
                date_object = datetime.strptime(date, '%Y%m')
                # Format the date as YYYY-MM
                formatted_date = date_object.strftime('%Y-%m')
        except ValueError as e:
            # Handle cases where strptime fails (e.g., invalid date like 20181301)
            logger.error(f'Failed to parse date string {date}: {e}')
            pass  # Fallback to original date if parsing fails
        return variable, region, resolution, formatted_date
    else:
        error_message = f'Regex did not match filename: {filename}. Could not parse.'
        raise ValueError(error_message)
        logger.error(error_message)
        return None, None, None, None  # Return None if no match

def file_sorter(outPath, subDirectories):
    '''
    Sorts extracted files in the outPath directory based on their extensions
    '''
    try:
        data_files = [f for f in os.listdir(outPath) if os.path.isfile(os.path.join(outPath, f))]
        datafile_total = len(data_files)
        logger.info(f'{datafile_total} data files found in {outPath} for sorting.')
        if datafile_total == 0:
            error_msg = f'Error: No files were found in {outPath} for sorting. Please restart program operations.'
            print(error_msg)
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        # Initial progress bar for the start 
        progress_bar(0, datafile_total, prefix=f'Starting to sort files into sub-directories', suffix='Ready\n')
        for i, filename in enumerate(data_files):
            source = os.path.join(outPath, filename)
            if os.path.isfile(source):
                _, ext = os.path.splitext(filename)
                ext = ext.lower()
                if ext in subDirectories:
                    subPath = subDirectories[ext]
                    destination = os.path.join(subPath, filename)
                    if not os.path.exists(destination):
                        try:
                            shutil.move(source, subPath)
                            print(f'Moved "{filename}" to sub-directory: {os.path.basename(subPath)}')
                            logger.info(f'Moved {filename} to sub-directory: {os.path.basename(subPath)}')
                        except Exception as sort_error:
                            error_msg = f'Failed to sort {filename}: {sort_error}'
                            print(error_msg)
                            logger.error(error_msg)
                            logger.exception(sort_error)
                            traceback.print_exc()
                    else:
                        warning_msg = f'Warning: {filename} already exists in {os.path.basename(subPath)}. Skipping.'
                        print(warning_msg)
                        logger.warning(warning_msg)
                else:
                    warning_msg = f'Unable to sort {filename} - extension "{ext}" not in subDirectories.'
                    print(warning_msg)
                    logger.warning(warning_msg)
            else:
                logger.info(f'Skipping non-file: {filename}')
            # Update progress 
            progress_bar(i + 1, datafile_total, prefix=f'Sorting files: {i + 1} of {datafile_total}', suffix=f'Files sorted')
        logger.info(f'File sorting process completed in {outPath}.')
    except Exception as e:
        error_msg = f'An error occurred during file sorting in {outPath}: {e}'
        print(error_msg)
        logger.error(error_msg)
        logger.exception(e)
        raise
        
def zip_extract(outPath):
    '''
    Extracts all zip files found in the outPath directory
    '''
    try:
        zip_files = [f for f in os.listdir(outPath) if f.lower().endswith('.zip')]
        zip_total = len(zip_files)
        logger.info(f'{zip_total} zip files found in {outPath} for extraction.')

        if zip_total > 0:
            # Initial progress bar for the start of extraction
            progress_bar(0, zip_total, prefix=f'Starting zip extraction to {os.path.basename(outPath)} directory', suffix='Ready\n')
            for i, folder in enumerate(zip_files):
                dataPath = os.path.join(outPath, folder)
                try:
                    with zipfile.ZipFile(dataPath, 'r') as zfile:
                        zfile.extractall(outPath)
                        print(f'\nFiles from "{folder}" have been extracted to: {os.path.basename(outPath)}')
                        logger.info(f'Successfully extracted files from {folder} to {outPath}')
                except zipfile.BadZipFile as bad_zip:
                    error_msg = f'\nError extracting {folder}: {bad_zip}'
                    print(error_msg)
                    logger.error(error_msg)
                    logger.exception(bad_zip)
                    traceback.print_exc()
                except Exception as zip_error:
                    error_msg = f'\nUnexpected error occurred during extraction of {folder}: {zip_error}'
                    print(error_msg)
                    logger.error(error_msg)
                    logger.exception(zip_error)
                    traceback.print_exc()
                # Update the overall progress of zip folder extraction
                progress_bar(i + 1, zip_total, prefix=f'Zip extraction: {i + 1} of {zip_total}', suffix='- Folders processed')
        else:
            logger.info(f'No zip files found in {outPath} for extraction.')
    except Exception as e:
        error_msg = f'An error occurred during zip extraction in {outPath}: {e}'
        print(error_msg)
        logger.error(error_msg)
        logger.exception(e)
        raise
           
def file_mover(sourcePath, download_dir, filename, outPath):
    '''
    Moves downloaded zip files to a specified directory path (outPath)
    '''
    try:
        logger.info(f'Moving file {os.path.basename(filename)} from {sourcePath} to {outPath}')
        destinationPath = os.path.join(outPath, filename)
        if not os.path.exists(destinationPath):
            shutil.move(sourcePath, outPath)
            logger.info(f'{filename} moved successfully to {outPath}')
        else:
            warning_msg = f'Warning: {os.path.basename(filename)} already exists in output directory: {os.path.basename(outPath)} - Skipping move'  
            print(warning_msg)
            logger.warning(warning_msg)
    except Exception as move_error:
        error_msg = f'Error moving {os.path.basename(filename)} file: {move_error}'
        print(error_msg)
        logger.error(error_msg)
        logger.exception(error_msg)
        raise
