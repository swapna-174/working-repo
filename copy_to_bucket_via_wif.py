"""
This script copies files from a local path or network share to a specified folder in 
a Google Cloud Platform (GCP) bucket.


Command-Line Arguments
----------------------

The following arguments are expected when running this module as a standalone script:

    --source-path      (str, required)
        Local path or network share containing files to upload.

    --file-pattern     (str, required, default: "*.txt")
        File pattern to filter files (e.g., "*.txt", "myfile.txt").

    --bucket-name      (str, required)
        Name of the destination GCP bucket.

    --dest-folder      (str, required)
        Path to the destination folder in the GCP bucket.

    --wwconfig-path    (str, required)
        Path to the Workload Identity Federation (.wwconfig) configuration file.

    --console-output   (bool, optional, default: True)
        Enable console output of logs (True or False).

    --log-file         (str, optional, default: "copy_to_bucket_via_wif.log")
        Path to the log file.

Example:
    python copy_to_bucket_via_wif.py \
        --source-path "C:\\local\\data" \
        --file-pattern "*.txt" \
        --bucket-name "your-gcp-bucket" \
        --dest-folder "your/destination/folder/" \
        --wwconfig-path "C:\\path\\to\\your\\config.wwconfig" \
        --console-output True \
        --log-file "transfer.log"



Usage: 
    Can be called directly from the command line or integrated into other Python modules.

Module usage example:

import logging
from copy_to_bucket_via_wif import setup_logging, authenticate_with_wif, copy_files_to_gcp

# Set up logging
setup_logging(
    log_file='transfer.log',
    console_output=True,
    console_log_level=logging.INFO,
    file_log_level=logging.DEBUG
)

# Authenticate using WIF and your .wwconfig file
authenticate_with_wif(r'C:/GCP_WWAUTH/LW10LS198MIS111_GCP_WWAUTH.wwconfig')

# Copy files to GCP bucket
copy_files_to_gcp(
    source_path=r'C:/tmp/TEST_WIF/data',
    pattern='*.txt',
    bucket_name='extracts-silver-dev-rev',
    destination_folder='mytest/mypath/'
)


Features:

- Workload Identity Federation (WIF) Authentication:
  Uses GCP's Workload Identity Federation and a .wwconfig file to securely authenticate 
  without service account keys.

- File Pattern Filtering:
  Copies only files matching a specified Windows-style pattern (e.g., *.txt) from a 
  local directory or network share.

- Google Cloud Storage Integration:
  Uploads files to a specified folder within a Google Cloud Storage bucket.

- Robust Logging:
  Logs all operations and errors to a specified log file, with optional console output. 
  Logging levels are configurable.

- Command-Line and Programmatic Usage:
  Can be run as a standalone script with command-line arguments or imported and used 
  in other Python modules.

- Argument Validation:
  Validates the existence of source paths and WIF configuration files before starting 
  the transfer.

- Upload Confirmation:
  Verifies each file upload and logs confirmation success or failure.

- Security Best Practices:
  Avoids storing long-lived credentials on disk and leverages short-lived access tokens 
  for improved security.

- Easy Integration:
  Functions such as setup_logging, authenticate_with_wif, and copy_files_to_gcp can be 
  reused in other Python scripts.



Notes on Workload Identity Federation and the use of GCP's authenticator tool (wwauth.exe):

'wwauth.exe' is the command-line authentication tool for Workload Identity Federation (WIF).  
It's used to obtain access tokens for applications running outside of 
Google Cloud (e.g., on-premises servers, developer workstations) so that these applications 
can securely access Google Cloud resources.

Here's a breakdown of its purpose and role:

1. Bridging the Authentication Gap: Applications running outside of GCP don't have direct access 
   to Google's authentication services.  `wwauth.exe` acts as a bridge, allowing these workloads 
   to authenticate using their existing identity provider (e.g., Active Directory, Azure AD) and 
   obtain the necessary credentials (access tokens) to interact with GCP services.

2. Leveraging Workload Identity Federation:  'wwauth.exe' works in conjunction with Workload 
   Identity Federation.  WIF configures a trust relationship between your external identity 
   provider and GCP.  This trust allows GCP to accept identity assertions from your provider 
   as a valid way to authenticate workloads.

3. Obtaining Access Tokens:  The primary function of 'wwauth.exe' is to get an access token. 
   The application uses the 'get-token' command along with a configuration file ('.wwconfig') 
   to authenticate with the configured identity provider and obtain a short-lived access token. 
   This access token can then be used to authorize requests to Google Cloud APIs.

4. Configuration (.wwconfig): The '.wwconfig' file contains essential 
   configuration details, including:
   - The identity provider being used (e.g., 'azure', 'oidc').
   - Parameters specific to the identity provider (e.g., client ID, tenant ID for Azure AD).
   - The service account being impersonated in GCP (optional, but a common and recommended 
     security best practice).

5. Security: 'wwauth.exe' enhances security by:
   - Avoiding the need to store long-lived service account keys on your workloads.
   - Allowing you to manage identities and access control centrally within your existing 
     identity provider.
   - Supporting short-lived access tokens, which minimizes the impact of potential token compromise.

In simple terms: 'wwauth.exe' allows your on-premises applications to securely
"prove their identity" to Google Cloud without needing to store sensitive credentials 
directly on the server.  It achieves this by leveraging the trust relationship established 
through Workload Identity Federation and using your organization's existing identity provider.

CRITICAL VARIABLES:

- GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES:
    This variable is essential when using wwauth.exe. Setting it to '1' (or
    any non-empty string) explicitly allows external credential processes (like wwauth) to be used
    by the Google Cloud client libraries. Without this variable set, your application will likely
    ignore the .wwconfig file and any federated token obtained by wwauth.

- GOOGLE_APPLICATION_CREDENTIALS:
    While typically used to point to a service account key file, in the context of 
    workload identity federation, this variable should point to your .wwconfig file. This tells 
    the Google Cloud client libraries to use the configuration specified in the .wwconfig file for 
    authentication.

"""
import sys
import argparse
from ast import arg
import logging
import os
import getpass
from fnmatch import fnmatch
# from google.cloud import storage_transfer
# from google.oauth2 import service_account
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


def log_and_print(message, level="info", exit_code=None, error=None):
    """Log and print a message, optionally with exit code and error details."""
    msg = message
    if exit_code is not None:
        msg += f" | exit_code={exit_code}"
    if error is not None:
        msg += f" | error={error}"
    if level == "info":
        logging.info(msg)
        print(msg)
    elif level == "error":
        logging.error(msg)
        print(msg, file=sys.stderr)
    elif level == "warning":
        logging.warning(msg)
        print(msg)


# def authenticate_with_wif(wwconfig_path: str, gcp_project_name: str):
#     """Authenticates using Workload Identity Federation.

#     Args:
#         wwconfig_path (str): Path to the wwauth config file.

#     Returns:
        
#     """
    # Set environment variables for WIF authentication
    # os.environ['GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES'] = '1'
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = wwconfig_path
    # os.environ['GOOGLE_CLOUD_PROJECT'] = gcp_project_name


def copy_files_to_gcp(source_path: str,
                      pattern: str,
                      bucket_name: str,
                      destination_folder: str,
                      wwconfig_path: str,
                      gcp_project_name: str
                      ):
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

    logging.info("Initiating file copy process...")

    # Log the user ID under which the script is running
    user_id = getpass.getuser()
    logging.info("Script is running under user: %s", user_id)

    try:
        # logging.info("Setting environment variables for WIF authentication...")
        # os.environ['GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES'] = '1'
        # os.environ['GOOGLE_CLOUD_PROJECT'] = gcp_project_name
        # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = wwconfig_path
        
        # logging.info("Successfully set environment variables for WIF authentication.")

        logging.info("os.environ['GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES']: %s", os.environ['GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES'])
        logging.info("os.environ['GOOGLE_CLOUD_PROJECT']: %s", os.environ['GOOGLE_CLOUD_PROJECT'])
        logging.info("os.environ['GOOGLE_APPLICATION_CREDENTIALS']: %s", os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        
    except Exception as e:
        logging.error("Failed to set environment variables for WIF authentication: %s", e) 

    # Log the user ID under which the script is running
    user_id = getpass.getuser()
    logging.info("Script is running under user: %s", user_id)

    
    
    # logging.info("Verifying existence of WIF configuration file: %s", wwconfig_path)

    # logging.info("Current Working Directory: %s", os.getcwd())
    # logging.info("Checking path: %s", repr(wwconfig_path))
    # logging.info("Is absolute path: %s", os.path.isabs(wwconfig_path))
    # logging.info("Absolute path: %s", os.path.abspath(wwconfig_path))
    # logging.info("Exists: %s", os.path.exists(wwconfig_path))

    # if os.path.exists(wwconfig_path):
    #     logging.info("Verified existence of WIF configuration file: %s", wwconfig_path)
    # else:
    #     logging.error("WIF configuration file does not exist: %s", wwconfig_path)
    #     # return 1
    
    # try:
    #     with open(wwconfig_path, 'r') as f:
    #         logging.info("File opened successfully. %s", wwconfig_path)
    #         logging.info("File contents: %s", f.read())
    # except Exception as e:
    #     logging.error("Error opening file: %s", e)
    # finally:
    #     logging.info("Finished attempting to open WIF configuration file.")







    # try:
    #     logging.info("Authenticating with Workload Identity Federation...")
    #     authenticate_with_wif(wwconfig_path, gcp_project_name)
    #     logging.info("Successfully authenticated with WIF.")
    # except Exception as e:
    #     logging.error("Failed to authenticate with WIF: %s", e)
        #return 1



    # if os.path.exists(source_path):
    #     logging.info("Verified existence of source path: %s", source_path)
    # else:
    #     logging.error("Source path does not exist: %s", source_path)
    #     return 1
    


    try:
        logging.info("Connecting to GCP Storage...")
        storage_client = storage.Client(project=gcp_project_name,credentials=None)
        logging.info("Successfully connected to GCP Storage.")
    except Exception as e:
        logging.error(f"Failed to connect to GCP Storage: {e}")
        return 1

    try:
        logging.info("Connecting to GCP bucket...")
        bucket = storage_client.bucket(bucket_name)
        logging.info(f"Connected to GCP bucket: {bucket_name}")
    except Exception as e:
        logging.error(f"Failed to connect to GCP bucket: {e}")
        return 1

    logging.info("Preparing file transfer...")
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
            logging.info("Creating a blob file in the bucket for %s", filename)
            blob = bucket.blob(full_file_destination_path)

            logging.info(f"Uploading {filename} to {bucket_name}/{destination_folder}...")
            blob.upload_from_filename(local_file_path)
            logging.info(f"Uploaded {filename} to {bucket_name}/{destination_folder}.")

            # Verify file upload
            if blob.exists():
                logging.info(f"Confirmation success for {filename}.")
            else:
                logging.error(f"Confirmation failed for {filename}.")

        except Exception as e:
            logging.error(f"Failed to upload {filename}: {e}")
            return 1

    return 0


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
    parser.add_argument('--wwconfig-path', type=str, required=True,
                        help='Path to the WIF configuration file.')
    parser.add_argument('--console-output', type=bool, required=False, default=True,
                        help='Enable console output of logs.')
    parser.add_argument('--log-file', type=str, required=False, 
                        default='copy_to_bucket_via_wif.log',
                        help='Path to the log file.')
    parser.add_argument('--gcp-project', type=str, required=True,
                        help='GCP project ID.')

    args = parser.parse_args()

    setup_logging(args.log_file, 
                  args.console_output,
                  console_log_level=logging.INFO,
                  file_log_level=logging.DEBUG)

    exit_code = 0

    try:

        exit_code = copy_files_to_gcp(args.source_path, args.file_pattern, args.bucket_name,
                          args.dest_folder, args.wwconfig_path, args.gcp_project)

        if exit_code == 0:
            log_and_print("Script completed successfully.", exit_code=0)
            sys.exit(0)
        else:
            log_and_print("Script failed.", level="error", exit_code=exit_code)
            sys.exit(exit_code)

    except Exception as e:
        logging.error("An error occurred: %s", e)
    finally:
        # Log script exit
        log_and_print("Script exiting.", exit_code=exit_code)


if __name__ == '__main__':
    main()

