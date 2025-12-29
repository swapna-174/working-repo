import os
import sys

def convert_size_to_bytes(size_str):
    """Convert a size string like '2MB' to bytes."""
    units = {'KB': 1024, 'MB': 1024 ** 2, 'GB': 1024 ** 3}
    
    try:
        size_str = size_str.upper()
        for unit in units:
            if size_str.endswith(unit):
                return int(size_str[:-len(unit)].strip()) * units[unit]
        raise ValueError(f"Invalid size format: {size_str}")
    except (ValueError, KeyError) as e:
        print(f"Error parsing size: {e}")
        sys.exit(1)

def split_file(file_path, max_size_str, delimiter=",", destination_dir="."):
    """Split file into smaller files, each not exceeding max_size_str, preserving headers."""
    max_size = convert_size_to_bytes(max_size_str)
    
    if not os.path.exists(file_path):
        print("The specified file does not exist.")
        sys.exit(1)

    base_folder_path = os.path.dirname(file_path)      # /path/to/my
    base_file_name_with_ext = os.path.basename(file_path) # file.txt
    base_file_name, base_file_ext = os.path.splitext(base_file_name_with_ext)  # file, .txt

    print("base_folder_path = ", base_folder_path)
    print("base_file_name = ", base_file_name)
    print("base_file_ext = ", base_file_ext)

    try:
        os.makedirs(destination_dir, exist_ok=True)
    except OSError as e:
        print(f"Error creating directory {destination_dir}: {e}")
        sys.exit(1)

    try:
        with open(file_path, 'r', encoding='ISO-8859-1') as infile:
            header = infile.readline()  # Read and store the header
            header_size = len(header.encode('ISO-8859-1'))
            
            file_count = 1  # Start sequence from 1
            current_output_file_name = f"{destination_dir}/{base_file_name}_{file_count}{base_file_ext}"
            current_output_file = open(current_output_file_name, 'w', encoding='ISO-8859-1')
            current_output_file.write(header)
            current_size = header_size
            
            for line in infile:
                line_size = len(line.encode('ISO-8859-1'))
                
                if current_size + line_size > max_size:
                    current_output_file.close()
                    file_count += 1
                    current_output_file_name = f"{destination_dir}/{base_file_name}_{file_count}{base_file_ext}"
                    current_output_file = open(current_output_file_name, 'w', encoding='ISO-8859-1')
                    current_output_file.write(header)
                    current_size = header_size

                current_output_file.write(line)
                current_size += line_size
            
            current_output_file.close()
            print(f"Files successfully written to: {destination_dir}")

    except Exception as e:
        print(f"Error processing the file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python split_huge_file.py <file_path> <max_size> <delimiter> <destination_folder>")
        print("Example: python split_huge_file.py large_file.csv 50MB ',' './output'")
        print("Example: python 'C:\\tmp\\split_huge_file.py' 'C:\\tmp\\TEST.log' 1KB '|' 'C:\\tmp'")
        sys.exit(1)
    
    file_path = sys.argv[1]
    max_size_str = sys.argv[2]
    delimiter = sys.argv[3]
    destination_dir = sys.argv[4]

    split_file(file_path, max_size_str, delimiter, destination_dir)
