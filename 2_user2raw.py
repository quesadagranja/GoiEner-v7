# -----------------------------------------------------------------------------------
# Script Name: 2_user2raw.py
# Author: Carlos Quesada Granja
# Affiliation: Universidad de Deusto
# Website: www.quesadagranja.com
# Year: 2025
# -----------------------------------------------------------------------------------

import os
import json
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor
from glob import glob
from datetime import datetime, timedelta

def load_config(config_path):
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config

def process_line(line, file_type_map):
    parts = line.strip().split(';')
    file_type = parts[1]
    if file_type not in file_type_map:
        return None  # Unrecognized file type

    dt_col, fl_col, in_col, out_col, dcm_col = file_type_map[file_type]

    # Ensure the necessary columns are present
    if in_col >= len(parts) or out_col >= len(parts):
        return None
    
    dt = parts[dt_col]
    fl = parts[fl_col]
    row = [dt, fl, file_type, parts[in_col], parts[out_col]]
    if dcm_col is not None:
        if dcm_col < len(parts):
            row.append(parts[dcm_col] if parts[dcm_col] else "")
        else:
            row.append("")  # Add empty string for missing DCM column
    else:
        row.append("")  # Add empty string for P5D where DCM is not present
    return row

def adjust_datetime(row):
    dt_str, fl, file_type = row[0], int(row[1]), row[2]
    
    try:
        if file_type in ['P1', 'P1D', 'F1']:
            dt = datetime.strptime(dt_str, "%Y/%m/%d %H:%M:%S")
        else:
            dt = datetime.strptime(dt_str, "%Y/%m/%d %H:%M")
    except ValueError as e:
        raise ValueError(f"Failed to parse datetime '{dt_str}' for file type '{file_type}': {e}")
    
    if fl == 1:
        dt -= timedelta(hours=1)
    row[0] = dt.strftime("%Y/%m/%d %H:%M")
    return row

def process_file(file_path, raw_dir):
    try:
        file_name = os.path.basename(file_path)
        print(f"Processing file: {file_name}")

        file_type_map = {
            'A5D': (3, 4, 5, 6, 11),
            'B5D': (3, 4, 5, 6, 11),
            'F5D': (3, 4, 5, 6, 11),
            'P5D': (3, 4, 5, 6, None),
            'RF5D': (3, 4, 5, 6, 11),
            'F1': (4, 5, 6, 7, 14),
            'P1': (4, 5, 6, 8, 22),
            'P1D': (4, 5, 6, 8, 22)
        }

        # Read the file as raw text lines
        with open(file_path, 'r') as f:
            lines = f.readlines()

        # Process each line individually
        processed_lines = [process_line(line, file_type_map) for line in lines]
        processed_lines = [line for line in processed_lines if line is not None]

        # Adjust the datetime based on the flag
        adjusted_lines = [adjust_datetime(line) for line in processed_lines]

        # Create a DataFrame from adjusted lines and remove duplicate rows
        df = pd.DataFrame(adjusted_lines).drop_duplicates()

        # Group by DT and FL
        grouped = df.groupby([0, 1])

        output_data = []

        for (dt, fl), group in grouped:
            row = [dt, fl]
            num_entries = len(group)  # Count the number of entries in this group
            row.append(num_entries)  # Add the number of initial entries as the third field
            for _, entry in group.iterrows():
                row.extend(entry[2:].tolist())
            output_data.append(row)

        # Sort by DT
        output_data.sort(key=lambda x: datetime.strptime(x[0], "%Y/%m/%d %H:%M"))

        # Pad rows to ensure each has 50 fields
        padded_output_data = [row + [''] * (50 - len(row)) for row in output_data]

        # Write the processed data to the corresponding raw file
        raw_file_path = os.path.join(raw_dir, file_name)
        with open(raw_file_path, 'w') as f:
            for row in padded_output_data:
                f.write(';'.join(map(str, row)) + '\n')

        return f"Processed {file_name}"
    except Exception as e:
        print(f"Error processing file {file_name}: {e}")
        return f"Failed to process {file_name}: {e}"

def setup_logging(id2raw_log):
    logging.basicConfig(
        filename=id2raw_log,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.json')

    config = load_config(config_path)
    id_files_pattern = os.path.join(config['id_dir'], '*.csv')
    raw_dir = config['raw_dir']
    id2raw_log = os.path.join(script_dir, config['id2raw_log'])

    # Set up logging
    setup_logging(id2raw_log)

    # Create the output directory if it doesn't exist
    os.makedirs(raw_dir, exist_ok=True)

    id_files = glob(id_files_pattern)

    num_workers = max(1, os.cpu_count() - 1)  # Asegura al menos 1 worker
    # Process files in parallel using ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
      futures = [executor.submit(process_file, file, raw_dir) for file in id_files]
      results = [future.result() for future in futures]  # Obtiene resultados una vez completados


    for result in results:
        logging.info(result)
        print(result)  # Print result to standard output for immediate feedback

if __name__ == "__main__":
    main()
