import csv
import os
import zipfile
import shutil
import argparse
import time


def calc_seq_index(sequence_format, index):
    # Determine suffix format based on user input
    """
    index format name              config index format   python format       expected results
    ------------------------------ --------------------- ------------------- -----------------------------------
    simple iterator                {index}               '{index}'           1, 2, 3, ...
    zero-left-paded up to X digits {index:0X}            '{index:03}'        001, 002, 003, ...
    string with iterator           _part{index}          '_part{index}'      _part1, _part2, _part3, ...
    string with zero-left-paded    _part{index:0X}       '_part{index:03}'   _part001, _part002, _part003, ...
    """ 
    formatted_string = sequence_format.format(index=index)
    return formatted_string




def split_csv_file(file_path,
                   delimiter,
                   max_size=2,
                   unit='GB',
                   file_seq_format='numeric',
                   base_file_name=None,
                   zip_output=False,
                   zip_file_name=None,
                   dest_path=None,
                   archive=False,
                   archive_path=None):
    """
    Splits a large CSV text file into smaller files.

    Args:
        file_path      (str)            : Path to the input CSV file.
        delimiter      (str)            : Field delimiter used in the CSV file.
        max_size       (str)            : Maximum size for each smaller file.
        unit           (str)            : Unit for max_size (KB, MB, or GB).
        file_seq_format (str, optional) : Format used to append a sequence number the output files ('numeric', 'zero_padded', 'part').
        base_file_name (str, optional)  : Base name for the output files.
        zip            (bool, optional) : Compress the output files into a ZIP archive.
        zip_file_name  (str, optional)  : Name for the output ZIP file (without extension).
        dest_path      (str, optional)  : Destination folder to save the final ZIP file or smaller files.
        archive        (bool, optional) : Make a 2nd Copy of the output file(s) and place it in the archive folder.
        archive_path   (str, optional)  : Path to archive folder where a 2nd Copy of the output file(s) will be placed.

    Usage:        
        
        a) Without Compression and Default Naming:

            python split_csv.py large_file.csv ',' 5 MB

        b) Without Compression, Default Naming, and Specific Folder Destination:

            python split_csv.py large_file.csv ',' 5 MB --dest_path /path/to/destination

        c) With Compression and Default ZIP Name:
        
            python split_csv.py large_file.csv ',' 5 MB --zip

        d) With Compression, Default ZIP Name, and Specific Folder Destination:

            python split_csv.py large_file.csv ',' 5 MB --zip --dest_path /path/to/destination
        

            
        OTHER COMBINATIONS TO BE TESTED ......................
            
        e) With Specified Base File Name:

            python split_csv.py large_file.csv ',' 5 MB --base_file_name custom_base_name

        f) With Compression, Custom ZIP Name, and Destination Folder:

            python split_csv.py large_file.csv ',' 5 MB --zip --zip_file_name my_output --dest_path /path/to/destination


        cd C:/Temp/file_split

        a) Without Compression and Default Naming:

            - FW_NW_EPIC_ACCESSLOG_20250210.log - 2,951 KB
            c:/Users/earamayo/AppData/Local/Programs/Python/Python313/python.exe "C:/NW_GitHub_Ent/repos/datamgmt-dataeng-epic-extracts/dataintegration/python-libs/cogito_utils_file_ops.py" "FW_NW_EPIC_ACCESSLOG_20250210.log" '|' 500 KB --file_seq_format='numeric'
            
            - FW_NW_EPIC_ACCESSLOG_20250121.log - 9,412 KB
            c:/Users/earamayo/AppData/Local/Programs/Python/Python313/python.exe "C:/NW_GitHub_Ent/repos/datamgmt-dataeng-epic-extracts/dataintegration/python-libs/cogito_utils_file_ops.py" "FW_NW_EPIC_ACCESSLOG_20250121.log" '|' 4 MB --file_seq_format='zero_padded'
            c:/Users/earamayo/AppData/Local/Programs/Python/Python313/python.exe "C:/NW_GitHub_Ent/repos/datamgmt-dataeng-epic-extracts/dataintegration/python-libs/cogito_utils_file_ops.py" "FW_NW_EPIC_ACCESSLOG_20250121.log" '|' 4 MB --file_seq_format='zero_padded' --archive --archive_path='C:/Temp/file_split/archive_folder'
            c:/Users/earamayo/AppData/Local/Programs/Python/Python313/python.exe "C:/NW_GitHub_Ent/repos/datamgmt-dataeng-epic-extracts/dataintegration/python-libs/cogito_utils_file_ops.py" "FW_NW_EPIC_ACCESSLOG_20250121.log" '|' 4 MB --file_seq_format='zero_padded' --zip --archive --archive_path='C:/Temp/file_split/archive_folder'

            - FW_NW_EPIC_ACCESSLOG_20250321.log - 26,939 KB
            c:/Users/earamayo/AppData/Local/Programs/Python/Python313/python.exe "C:/NW_GitHub_Ent/repos/datamgmt-dataeng-epic-extracts/dataintegration/python-libs/cogito_utils_file_ops.py" "FW_NW_EPIC_ACCESSLOG_20250321.log" '|' 10 MB

            - FW_NW_EPIC_ACCESSLOG_20250211.log - 3,829,386 KB
            c:/Users/earamayo/AppData/Local/Programs/Python/Python313/python.exe "C:/NW_GitHub_Ent/repos/datamgmt-dataeng-epic-extracts/dataintegration/python-libs/cogito_utils_file_ops.py" "FW_NW_EPIC_ACCESSLOG_20250211.log" '|' 1 GB

        b) Without Compression, Default Naming, and Specific Folder Destination:
        
            c:/Users/earamayo/AppData/Local/Programs/Python/Python313/python.exe "C:/NW_GitHub_Ent/repos/datamgmt-dataeng-epic-extracts/dataintegration/python-libs/cogito_utils_file_ops.py" "FW_NW_EPIC_ACCESSLOG_20250210.log" ',|' 1000 KB --dest_path "C:/Temp/file_split/dest_path"
        
        c) With Compression and Default ZIP Name:

            c:/Users/earamayo/AppData/Local/Programs/Python/Python313/python.exe "C:/NW_GitHub_Ent/repos/datamgmt-dataeng-epic-extracts/dataintegration/python-libs/cogito_utils_file_ops.py" "FW_NW_EPIC_ACCESSLOG_20250210.log" '|' 1000 KB --zip

        d) With Compression, Default ZIP Name, and Specific Folder Destination:
        
            - FW_NW_EPIC_ACCESSLOG_20250210.log - 2,951 KB
            c:/Users/earamayo/AppData/Local/Programs/Python/Python313/python.exe "C:/NW_GitHub_Ent/repos/datamgmt-dataeng-epic-extracts/dataintegration/python-libs/cogito_utils_file_ops.py" "FW_NW_EPIC_ACCESSLOG_20250210.log" '|' 500 KB --zip  --dest_path "C:/Temp/file_split/dest_path"

            - FW_NW_EPIC_ACCESSLOG_20250121.log - 9,412 KB
            c:/Users/earamayo/AppData/Local/Programs/Python/Python313/python.exe "C:/NW_GitHub_Ent/repos/datamgmt-dataeng-epic-extracts/dataintegration/python-libs/cogito_utils_file_ops.py" "FW_NW_EPIC_ACCESSLOG_20250121.log" '|' 1 MB --zip  --dest_path "C:/Temp/file_split/dest_path"

            - FW_NW_EPIC_ACCESSLOG_20250321.log - 26,939 KB
            c:/Users/earamayo/AppData/Local/Programs/Python/Python313/python.exe "C:/NW_GitHub_Ent/repos/datamgmt-dataeng-epic-extracts/dataintegration/python-libs/cogito_utils_file_ops.py" "FW_NW_EPIC_ACCESSLOG_20250321.log" '|' 10 MB --zip  --dest_path "C:/Temp/file_split/dest_path"
            
            - FW_NW_EPIC_ACCESSLOG_20250211.log - 3,829,386 KB
            c:/Users/earamayo/AppData/Local/Programs/Python/Python313/python.exe "C:/NW_GitHub_Ent/repos/datamgmt-dataeng-epic-extracts/dataintegration/python-libs/cogito_utils_file_ops.py" "FW_NW_EPIC_ACCESSLOG_20250211.log" '|' 1 GB --zip  --dest_path "C:/Temp/file_split/dest_path"


    """


    # Define unit conversion factors
    unit_factors = {'KB': 1024, 'MB': 1024**2, 'GB': 1024**3}
    max_size_bytes = max_size * unit_factors.get(unit.upper(), 1024**2)

    original_base_name, ext = os.path.splitext(file_path)
    base_name = base_file_name if base_file_name else original_base_name
    output_files = []

    try:
        # When using encoding='utf-8', the below error occurs when processing very large files (> 1Gb)
        # An error occurred: 'utf-8' codec can't decode byte 0xe3 in position 1254: invalid continuation byte
        #with open(file_path, 'r', newline='', encoding='utf-8', ) as input_file:

        #with open(file_path, 'r', newline='', encoding='latin-1', ) as input_file:
        with open(file_path, 'r', newline='', encoding='ISO-8859-1', ) as input_file:
            csv_reader = csv.reader(input_file, delimiter=delimiter)
            try:
                header = next(csv_reader)  # Read the header
            except StopIteration:  # Handle if CSV is empty
                print("The CSV file is empty.")
                return

            file_count = 0
            current_size = 0
            output_file = None
            csv_writer = None

            # initialize variables with the same value because no files have been created yet
            start_time = time.time()  # Start timing file creation
            end_time = time.time()  # Start timing file creation

            for row in csv_reader:
                row_size = sum(len(str(item).encode('utf-8')) for item in row) + len(row) + len(delimiter) * (len(row) - 1)
                if current_size + row_size > max_size_bytes or csv_writer is None:
                    if output_file:
                        output_file.close()

                    if csv_writer is not None:  # Notify complete creation time for each file
                        end_time = time.time()  # Record time after closing file
                        print(f"File {output_file_path} created in {end_time - start_time:.2f} seconds.")

                    file_count += 1
                    current_size = 0
                    #output_file_path = f"{base_name}_part{file_count}{ext}"
                    seq_index = calc_seq_index(file_seq_format,file_count)

                    output_file_name = base_name.replace("{SPLITINDEX}", seq_index) 

                    #output_file_path = f"{base_name}{seq_index}{ext}"
                    output_file_path = f"{output_file_name}{ext}"
                    

                    output_files.append(output_file_path)

                    start_time = time.time()  # Start timing file creation
                    output_file = open(output_file_path, 'w', newline='', encoding='utf-8')
                    csv_writer = csv.writer(output_file, delimiter=delimiter)

                    #if header:  # Write the header if it exists
                    #    csv_writer.writerow(header)
                    csv_writer.writerow(header)

                csv_writer.writerow(row)
                current_size += row_size

            if output_file:
                output_file.close()
                end_time = time.time()  # Record time after closing file
                print(f"File {output_file_path} created in {end_time - start_time:.2f} seconds.")

        if zip_output:
            zip_base_name = zip_file_name if zip_file_name else original_base_name

            #Remove the string "{SPLITINDEX}" from the file name
            zip_base_name = zip_base_name.replace("{SPLITINDEX}", "")
            
            zip_file_path = os.path.join(dest_path if dest_path else '', f"{zip_base_name}.zip")
            with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for output_file_path in output_files:
                    zipf.write(output_file_path, os.path.basename(output_file_path))
                    os.remove(output_file_path)
            print(f"All smaller files compressed into archive: {zip_file_path}")

            if archive:
                # Copy zip file to the archive folder
                final_archive_file_path = os.path.join(archive_path, os.path.basename(zip_file_path))
                shutil.copy(zip_file_path, final_archive_file_path)
                print(f"ZIP file moved to destination folder: {final_archive_file_path}")

            if dest_path:
                # Move zip file to a different destination folder
                final_zip_file_path = os.path.join(dest_path, os.path.basename(zip_file_path))
                shutil.move(zip_file_path, final_zip_file_path)
                print(f"ZIP file moved to destination folder: {final_zip_file_path}")
        else:
            if archive:
                # Copy files to the archive folder
                for output_file_path in output_files:
                    shutil.copy(output_file_path, os.path.join(archive_path, os.path.basename(output_file_path)))
                print(f"Smaller files successfully copied to destination folder: {archive_path}")

            if dest_path:
                # Move files to a different destination folder
                for output_file_path in output_files:
                    shutil.move(output_file_path, os.path.join(dest_path, os.path.basename(output_file_path)))
                print(f"Smaller files successfully moved to destination folder: {dest_path}")

        # Remove the original large file if the splitting was successful
        os.remove(file_path)
        print(f"Original file {file_path} has been removed.")

    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    parser = argparse.ArgumentParser(description='Split a large CSV file into smaller files.')
    parser.add_argument('file_path', type=str, help='Path to the input CSV file')
    parser.add_argument('delimiter', type=str, help='Field delimiter used in the CSV file')
    parser.add_argument('max_size', type=float, help='Maximum size for each smaller file')
    parser.add_argument('unit', type=str, choices=['KB', 'MB', 'GB'], help='Unit for file size (KB, MB, or GB)')
    parser.add_argument('--file_seq_format', type=str, help='Format used to append a sequence number the output files ("_#", "_000#", "_part#", etc)')
    parser.add_argument('--base_file_name', type=str, help='Base name for the output files')
    parser.add_argument('--zip', action='store_true', help='Compress the output files into a ZIP archive')
    parser.add_argument('--zip_file_name', type=str, help='Name for the output ZIP file (without extension)')
    parser.add_argument('--dest_path', type=str, help='Destination folder to save the final ZIP file or smaller files')
    parser.add_argument('--archive', action='store_true', help='Make a 2nd Copy of the output file(s) and place it in the archive folder')
    parser.add_argument('--archive_path', type=str, help='Path to archive folder where a 2nd Copy of the output file(s) will be placed')

    args = parser.parse_args()

    split_csv_file(
        args.file_path,
        args.delimiter,
        args.max_size,
        unit=args.unit,
        file_seq_format=args.file_seq_format,
        base_file_name=args.base_file_name,
        zip_output=args.zip,
        zip_file_name=args.zip_file_name,
        dest_path=args.dest_path,
        archive=args.archive,
        archive_path=args.archive_path
    )

if __name__ == '__main__':
    main()
