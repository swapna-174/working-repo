"""
This script copies files from a local path or network share to a specified folder in 
a Google Cloud Platform (GCP) bucket.

Usage: 
    Can be called directly from the command line or integrated into other Python modules.

Module usage example:
# from copy_to_bucket_via_key import setup_logging, initiate_transfer, authenticate
# setup_logging('transfer.log', 
#                   console_output=True,
#                   console_log_level=logging.INFO,
#                   file_log_level=logging.DEBUG)
# credentials = authenticate('/path/to/service-account-key.json')
# copy_files_to_gcp("C:/NW_JAVA/data", "*.txt", "tst-bucket-20250430",
#                       credentials, "mytest/mypath/")


Features:
- Filters files based on a specified Windows file name pattern (e.g., "*.txt").
- Logs all operations and errors to a specified log file, with an option to display 
  them on the console.
- Validates command-line arguments before proceeding.
- Confirms successful copies of each file to the bucket and logs failures for review.
- Uses service account key file to authenticate to GCP services

Args:

    source-path        (str) : Local path or network share.
    file-pattern       (str) : Windows file name pattern to match (e.g., "*.txt").
    bucket-name        (str) : Name of the destination GCP bucket.
    dest-folder        (str) : Destination folder path in the GCP bucket.
    log-file           (str) : Full path to log file.
    key-path           (str) : Path to the service account JSON key file.
    console-output     (bool): Enable console output of logs.
"""

import argparse
import logging
import os
from fnmatch import fnmatch
# from google.cloud import storage_transfer
from google.oauth2 import service_account
from google.cloud import storage


def setup_logging(log_file: str,
                  console_output: bool = True,
                  console_log_level=logging.INFO,
                  file_log_level=logging.DEBUG) -> None:
    """Sets up logging to both console and a log file.

    Args:
        log_file          (str): Path for the log file.
        console_output   (bool): True to enable console logging
        console_log_level (int): Logging level for console output.
        file_log_level    (int): Logging level for file output.
    """
    # Create handlers: console and file
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_log_level)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(file_log_level)

    # Create a formatter and set it for both handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add both handlers to the logger
    handlers = [file_handler]
    if console_output:
        handlers.append(console_handler)

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s: %(message)s',
                        handlers=handlers)


def authenticate(key_path: str) -> service_account.Credentials:
    """Authenticates using the provided Service Account JSON key file.

    Args:
        key_path (str): Path to the JSON key file.

    Returns:
        service_account.Credentials: Authenticated credentials.
    """
    try:
        credentials = service_account.Credentials.from_service_account_file(key_path)
        return credentials
    except Exception as error:
        logging.error("Failed to authenticate using Service Account key file: %s", error)
        raise


def copy_files_to_gcp(source_path: str,
                      pattern: str,
                      bucket_name: str,
                      credentials: service_account.Credentials,
                      destination_folder: str):
    """
    Copies files that match the specified pattern from a source path to a GCP bucket.

    Args:
        source_path        (str): Path to the source directory.
        pattern            (str): Windows file name pattern to match (e.g., "*.txt").
        bucket_name        (str): Name of the destination GCP bucket.
        credentials (service_account.Credentials): Authenticated credentials. 
                                  Service Accounts: JSON Web Token (JWT) Profile for OAuth 2.0)
        destination_folder (str): Destination folder path in the GCP bucket.
    """
    try:
        # Initialize GCP storage client and bucket
        #@EA storage_client = storage.Client()
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        logging.info(f"Connected to GCP bucket: {bucket_name}")
    except Exception as e:
        logging.error(f"Failed to connect to GCP: {e}")
        return

    for filename in os.listdir(source_path):
        # Match files based on pattern
        if not fnmatch(filename, pattern):
            # commented line below to reduce verbose in log file
            # logging.info(f"Skipping {filename}: pattern mismatch.")
            continue

        local_file_path = os.path.join(source_path, filename)

        if not os.path.isfile(local_file_path):
            # commented line below to reduce verbose in log file
            # logging.warning(f"Skipping {filename}: not a file.")
            continue

        # Ensure that destination folders are separated with forward slashes '/'
        # before copying to bucket
        full_file_destination_path = os.path.join(destination_folder, filename).replace('\\','/')

        try:
            # Create blob and upload file
            blob = bucket.blob(full_file_destination_path)
            blob.upload_from_filename(local_file_path)
            logging.info(f"Uploaded {filename} to {bucket_name}/{destination_folder}.")

            # Verify file upload
            if blob.exists():
                logging.info(f"Confirmation success for {filename}.")
            else:
                logging.error(f"Confirmation failed for {filename}.")

        except Exception as e:
            logging.error(f"Failed to upload {filename}: {e}")


def validate_and_start_transfer(args: argparse.Namespace) -> None:
    """
    Validates command-line arguments and starts the file transfer process if valid.

    Args:
        args (argparse.Namespace): Parsed command-line arguments.
    """
    if not os.path.exists(args.source_path):
        logging.error("Source path does not exist: %s", args.source_path)
        raise FileNotFoundError(f"Source path does not exist: {args.source_path}")

    if not os.path.exists(args.key_path):
        logging.error("Service account key file does not exist: %s", args.key_path)
        raise FileNotFoundError(f"Service account key file does not exist: {args.key_path}")

    credentials = authenticate(args.key_path)

    copy_files_to_gcp(args.source_path, args.file_pattern, args.bucket_name,
                      credentials, args.dest_folder)


def main() -> None:
    """Entry point for the script."""
    parser = argparse.ArgumentParser(description='Transfer files to a GCP bucket.')
    parser.add_argument('--source-path', type=str, required=True,
                        help='Local path or network share.')
    parser.add_argument('--file-pattern', type=str, required=True, default='*.txt',
                        help='File pattern to filter files, e.g., "*.txt".')
    parser.add_argument('--bucket-name', type=str, required=True, help='GCP bucket name.')
    parser.add_argument('--dest-folder', type=str, required=True,
                        help='Path to destination folder in the bucket.')
    parser.add_argument('--console-output', type=bool, required=False, default=True,
                        help='Enable console output of logs.')
    parser.add_argument('--log-file', type=str, required=False, default='copy_to_bucket_via_key.log',
                        help='Path to the log file.')
    parser.add_argument('--key-path', type=str, required=False,
                        help='Path to the service account JSON key file.')


    args = parser.parse_args()

    setup_logging(args.log_file, 
                  args.console_output,
                  console_log_level=logging.INFO,
                  file_log_level=logging.DEBUG)

    try:
        validate_and_start_transfer(args)
    except Exception as e:
        logging.error("An error occurred: %s", e)


if __name__ == '__main__':
    main()

# Module usage example:
# from this_module import setup_logging, initiate_transfer, authenticate
# setup_logging('transfer.log', 
#                   console_output=True,
#                   console_log_level=logging.INFO,
#                   file_log_level=logging.DEBUG)
# credentials = authenticate('/path/to/service-account-key.json')
# copy_files_to_gcp("C:/NW_JAVA/data", "*.txt", "tst-bucket-20250430",
#                       credentials, "mytest/mypath/")



# initiate_transfer(credentials, '/path/to/files', 'my-gcp-bucket', 'destination-folder', '*.txt')
