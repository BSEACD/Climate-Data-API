import pandas as pd
import numpy as np
import os
import logging
import traceback
import sys
import csv
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

# --- Normalized Antecedent Precipitation Index ---
def calculate_napi(filepath, decay_constant, ante_days, ppt_field, date_field, resolution, decimals=2):
    '''
    Calculates the Normalized Antecedent Precipitation Index (NAPI) and adds it to an existing CSV file.
    Args:
        filepath (str): The path to the CSV file.
        decay_constant (float): The decay constant 'k' for the NAPI calculation.
        ante_days (int): The number of antecedent days to consider for NAPI calculation (N).
        ppt_field (str): The name of the precipitation column.
        date_field (str): The name of the date column.
    Returns:
        bool: True if calculation was successful, False otherwise.
    '''
    try:
        df = pd.read_csv(filepath)
        if ppt_field not in df.columns or date_field not in df.columns:
            logger.error(f'Required columns "{ppt_field}" or "{date_field}" not found in the CSV.')
            print(f'Required columns "{ppt_field}" or "{date_field}" not found in the CSV. Please check if your CSV file returned empty values.')
            return False

        if resolution == 'daily':
            df[date_field] = pd.to_datetime(df[date_field], format='%Y-%m-%d')
        elif resolution == 'monthly':
            df[date_field] = pd.to_datetime(df[date_field], format='%Y-%m')
        else:
            df[date_field] = pd.to_datetime(df[date_field])
            
        df.sort_values(by=date_field, inplace=True)
        df.reset_index(drop=True, inplace=True)

        # Calculate overall mean precipitation for normalization
        mean_ppt = df[ppt_field].mean()

        if mean_ppt == 0:
            logger.warning('Mean precipitation is zero. NAPI cannot be calculated due to division by zero. Assigning NaN to NAPI column.')
            print('Mean precipitation is zero. NAPI cannot be calculated due to division by zero. Assigning NaN to NAPI column.')
            df['NAPI'] = np.nan
            df.to_csv(filepath, index=False)
            return True

        df['NAPI'] = np.nan  # Initialize NAPI column with NaN

        # Calculate the denominator sum: sum(k^j) for j=1 to antecedent days
        # This sum is for the normalizing factor in the denominator.
        denominator = sum([decay_constant**j for j in range(1, ante_days + 1)])

        for i in range(len(df)):
            if i >= ante_days:
                numerator = 0.0
                for j in range(1, ante_days + 1):
                    numerator += df.loc[i - j, ppt_field] * (decay_constant**j)

                # Calculate NAPI
                if (mean_ppt * denominator) != 0:
                    df.loc[i, 'NAPI'] = round(numerator / (mean_ppt * denominator), decimals)
                else:
                    df.loc[i, 'NAPI'] = np.nan
        # Add antecedent conditions based on NAPI values
        df['Conditions'] = np.nan  # Initialize wetness conditions column with NaN
        df['Conditions'] = df['Conditions'].astype(str)
        for i in range(len(df)):
            napi_value = df.loc[i, 'NAPI'] # Get the actual NAPI value for the current row

            if pd.isna(napi_value): # Check if NAPI is NaN
                df.loc[i, 'Conditions'] = np.nan
            elif abs(napi_value - 1) < 1e-6: # Check for floating-point equality with tolerance
                df.loc[i, 'Conditions'] = 'normal'
            elif napi_value > 1:
                df.loc[i, 'Conditions'] = 'wet'
            elif napi_value < 1:
                df.loc[i, 'Conditions'] = 'dry'
            else:
                df.loc[i, 'Conditions'] = np.nan # Fallback for any other case
                
        df.to_csv(filepath, index=False)
        logger.info(f'NAPI calculation complete. Data saved to "{filepath}" ')
        print(f'\nNAPI calculation complete. Data saved to "{os.path.basename(filepath)}" ')
        return True

    except Exception as e:
        logger.error(f'An error occurred during NAPI calculation: {e}')
        traceback.print_exc()
        return False

def ante_conditions(csv_dir, csv_file, ppt_field, date_field, resolution, decimals=2):
    k_value = 0.98  # input variable
    if resolution == 'daily':
        ante_days = 30
    elif resolution == 'monthly':
        ante_days = 3
    print(f'The K constant is preset to a value of: {k_value}')
    k_input = input('Would you like to use this preset? (y/n): ')
    if k_input in ('n', 'no'):
        k_value = float(input('\nEnter a K constant value between 0 and 1: '))
    else:
        print(f'\nProceeding with the preset K constant value: {k_value}')
        pass    
    try:
        while True:
            if 0 < k_value < 1:
                break
            else:
                print('Warning: Decay constant "K" is typically between 0 and 1.')
                constant = input('Do you want to proceed using {k_value}? (y/n): ').lower()
                if constant in ('y', 'yes'):
                    print('Proceeding with provided value: {k_value}.', file=sys.stderr)
                    logging.warning(f'User provided a k_value of "({k_value})", which is outside typical range (0, 1).')
                    break
                elif constant in ('n', 'no'):
                    info_msg = 'Accepting new user input for k constant value.'
                    logging.info(info_msg)
                    k_value = float(input('Please enter a new constant between 0 and 1: '))
                else:
                    print('Invalid input. Please enter yes or no (y/n).')
                
        while True:
            try:
                # antecedent days are calculated in data_processing. Inform user that they can use the pre-calculated days, or enter their own.
                if resolution == 'daily':
                    print('\nAntecedent days preset to 30 days.')
                    ante_num = str(input('Would you like to use this preset? (y/n): ')).lower()
                    if ante_num in ('n', 'no'):
                        ante_days = int(input('\nEnter the number of antecedent days for NAPI (e.g., 7 for 7 days prior): '))
                    else:
                        print('\nProceeding with the preset antecedent days.')
                        pass
                elif resolution == 'monthly':
                    print('\nAntecedent months preset to 3 months.')
                    ante_num = str(input('Would you like to use this preset? (y/n): ')).lower()
                    if ante_num in ('n', 'no'):
                        ante_days = int(input('\nEnter the number of antecedent months for NAPI (e.g., 3 for 3 months prior): '))
                    else:
                        print('\nProceeding with the preset antecedent months.')
                        pass
                if ante_days > 0:
                    print(f'\nCalculating Normalized Antecedent Precipitation Index (NAPI) with k={k_value} and {ante_days} antecedent days...')
                    logging.info(f'Calculating NAPI with k={k_value}, ante_days={ante_days} for {os.path.join(csv_dir, csv_file)}')
                    napi = calculate_napi(os.path.join(csv_dir, csv_file), k_value, ante_days, ppt_field, date_field, resolution, decimals)
                    if napi:
                        info_msg = 'NAPI calculation completed successfully.'
                        logging.info(info_msg)
                        
                        break
                    else:
                        print('NAPI calculation failed. See log for details.')
                        logging.error('NAPI calculation failed.')
                        break
                else:
                    print('Number of antecedent days must be positive. Please try again.', file=sys.stderr)
                    logging.warning(f'Invalid antecedent days value of "{ante_days}" entered for NAPI. Taking new user input.')
            except ValueError:
                print('Invalid input. Please enter a whole number for antecedent days.')
                logging.warning('Invalid input for antecedent days.')
    except Exception as calc_fail:
        error_msg = f'Failed to calculate the NAPI for the precipitation dataset: {calc_fail}'
        print(error_msg)
        logger.error(error_msg)
        traceback.print_exc()
