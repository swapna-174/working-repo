"""
zip_utilities.py

Utility functions for formatting file names with date placeholders and for creating
or appending to ZIP archives.

Features:
- format_file_name: Replaces date placeholders in file name formats
  (e.g., 'YYYYMMDD', '%Y%m%d') with today's date.
- append_zip_from_files: Creates a new ZIP file or appends files to an 
  existing ZIP file, avoiding duplicate entries.

Intended for use in ETL and data export workflows where output files must be named 
with dynamic dates and archived efficiently.

Usage Example:
--------------
from zip_utilities import append_zip_from_files

file_list = [
    r'C:\data\export1.csv',
    r'C:\data\export2.csv'
]
zip_file_name_format = 'export_YYYYMMDD.zip'
zip_path = append_zip_from_files(file_list, zip_file_name_format)
print(f"Created or updated zip file at: {zip_path}")

Dependencies:
- datetime
- os
- shutil
- tempfile
- zipfile
- glob
- typing.List
"""

from datetime import datetime
import os
import shutil
from zipfile import BadZipFile
import tempfile
import zipfile
import glob
from typing import List
import logging

logging.basicConfig(level=logging.INFO)



def format_file_name(file_name_format: str) -> str:
    """
    Formats the file name based on the given format string.

    Args:
        file_name_format (str): The format string for the file name.

    Returns:
        str: The formatted file name

    Rules:

        Calculate the value of new file name by replacing the date portion with today's date
        using the specified format as follows:
        1) if new_file_name contains the strings 'YYYYMMDD' or '%Y%m%d' then use the format '%Y%m%d'
        2) if new_file_name contains the strings 'MMDDYYYY' or '%m%d%Y' then use the format '%m%d%Y'
        3) if new_file_name contains the strings 'DDMMYYYY' or '%d%m%Y' then use the format '%d%m%Y'

    """
    new_file_name = file_name_format

    # Get today's date
    today = datetime.now()

    # Apply the replacement rules
    if 'YYYYMMDD' in new_file_name or '%Y%m%d' in new_file_name:
        new_file_name = new_file_name.replace('YYYYMMDD', today.strftime('%Y%m%d'))
        new_file_name = new_file_name.replace('%Y%m%d', today.strftime('%Y%m%d'))
    elif 'MMDDYYYY' in new_file_name or '%m%d%Y' in new_file_name:
        new_file_name = new_file_name.replace('MMDDYYYY', today.strftime('%m%d%Y'))
        new_file_name = new_file_name.replace('%m%d%Y', today.strftime('%m%d%Y'))
    if 'DDMMYYYY' in new_file_name or '%d%m%Y' in new_file_name:
        new_file_name = new_file_name.replace('DDMMYYYY', today.strftime('%d%m%Y'))
        new_file_name = new_file_name.replace('%d%m%Y', today.strftime('%d%m%Y'))

    return new_file_name



# Utility function to create or append to a zip file from a list of files
def append_zip_from_files(
    file_list: List[str],
    zip_file_name_format: str,
    log_file_path: str
) -> str:
    """
    Creates or appends to a zip file from the specified list of files, and deletes the original files
    only if all were successfully added to the zip.

    Args:
        file_list (List[str]): A list of file paths to include in the zip file.
        zip_file_name_format (str): The file name format for the zip file.
        log_file_path (str, optional): Full path to the log file. Defaults to "file_zipping_YYYYMMDD.log".

    Returns:
        str: The path to the created or updated zip file.

    Raises:
        Exception: If any file fails to be added to the zip, raises an exception and does not delete any files.
    """

    # Set up logging
    today_str = datetime.now().strftime('%Y%m%d')
    if not log_file_path:
        log_file_path = os.path.join(
            os.getcwd(), f"file_zipping_{today_str}.log"
        )
    logging.basicConfig(
        filename=log_file_path,
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )

    # Get the file path of the first file in the file_list
    first_file_path = os.path.dirname(file_list[0])

    # Calculate the file name of the zip file using the zip_file_name_format
    zip_file_name = format_file_name(zip_file_name_format)

    # Build the full path of the zip file using the first_file_path and zip_file_name
    zip_file_full_path = os.path.join(first_file_path, zip_file_name)
    logging.info(f"Zip file will be created/appended at: {zip_file_full_path}")

    try:
        # Create a temporary directory to hold the files to be zipped
        with tempfile.TemporaryDirectory() as temp_dir:
            # Copy the specified files to the temporary directory
            for file_path in file_list:
                try:
                    shutil.copy(file_path, temp_dir)
                except FileNotFoundError as e:
                    logging.error(f"File not found: {e.filename}")
                    raise
                except PermissionError as e:
                    logging.error(f"Permission denied: {e.filename}")
                    raise
                except shutil.SameFileError as e:
                    logging.error(f"Source and destination are the same file: {e}")
                    raise
                except OSError as e:
                    logging.error(f"OS error while copying file: {e}")
                    raise

            # Determine the mode: 'a' to append if exists, 'w' to create new
            mode = 'a' if os.path.exists(zip_file_full_path) else 'w'

            try:
                with zipfile.ZipFile(zip_file_full_path, mode) as zip_file:
                    added_files = []
                    failed_files = []
                    for file_path in glob.glob(os.path.join(temp_dir, '*')):
                        arcname = os.path.basename(file_path)
                        # Only add if not already present in the zip (avoid duplicates)
                        if arcname not in zip_file.namelist():
                            try:
                                zip_file.write(file_path, arcname)
                                logging.info(f"Added file to zip: {arcname}")
                            except PermissionError as e:
                                logging.error(f"Permission denied while writing to zip: {e.filename}")
                                failed_files.append(arcname)
                            except OSError as e:
                                logging.error(f"OS error while writing to zip: {e}")
                                failed_files.append(arcname)
                        # After attempting to add, check if it is in the zip
                        if arcname in zip_file.namelist():
                            added_files.append(arcname)
                        else:
                            if arcname not in failed_files:
                                failed_files.append(arcname)

                    # Check if all files were added
                    original_basenames = [os.path.basename(f) for f in file_list]
                    not_added = [f for f in original_basenames if f not in added_files]
                    if not_added:
                        for f in not_added:
                            logging.error(f"File was not added to zip: {f}")
                        raise Exception(f"Not all files were added to the zip. Failed files: {not_added}")

            except BadZipFile as e:
                logging.error(f"Corrupt zip file: {e}")
                raise
            except ValueError as e:
                logging.error(f"Value error: {e}")
                raise
            except PermissionError as e:
                logging.error(f"Permission denied while opening zip: {e.filename}")
                raise
            except OSError as e:
                logging.error(f"OS error while opening zip: {e}")
                raise

        # If all files were added, delete them from the file system
        for file_path in file_list:
            try:
                os.remove(file_path)
                logging.info(f"Deleted original file: {file_path}")
            except Exception as e:
                logging.error(f"Failed to delete file {file_path}: {e}")

    except Exception as e:
        logging.error(f"Unexpected error in append_zip_from_files: {e}")
        raise

    return zip_file_full_path
