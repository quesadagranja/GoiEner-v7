# GoiEner v7

This repository contains a set of Python scripts designed to process SIMEL (Sistema de Medidas Eléctricas) files. The pipeline is composed of four main stages that transform raw SIMEL data into imputed hourly consumption time series. Below is an overview of each stage and the corresponding script.

---

## Pipeline Overview

1. **SIMEL to User Files**  
   - **Script:** `1_simel2user.py`  
   - **Function:** Splits SIMEL files into individual user files.
   - **Process:**  
     - Loads configuration from `config.json`.
     - Scans for SIMEL files that match a specified naming pattern.
     - Reads each file as a CSV (with `;` as the delimiter) and adds metadata columns (the original file name and a file prefix).
     - Groups the data by the user ID (assumed to be in the third column) and writes each group to a separate CSV file in a designated directory.
     - Uses file locking to manage concurrent write operations and utilizes parallel processing via `ProcessPoolExecutor` for performance.
   - **Key Libraries:** `pandas`, `filelock`, `concurrent.futures`, `logging`.

2. **User to Raw Files**  
   - **Script:** `2_user2raw.py`  
   - **Function:** Processes individual user files to generate an intermediate raw file containing only the relevant information.
   - **Process:**  
     - Loads configuration and retrieves all user CSV files.
     - Reads each file line-by-line, processes each line based on a predefined file type mapping, and adjusts datetime values (e.g., subtracting one hour if indicated by a flag).
     - Groups processed data by datetime and flag, sorts the data chronologically, and pads each row to ensure a fixed width (50 fields).
     - Writes the cleaned and structured data to new raw CSV files.
     - Leverages parallel processing to handle multiple files concurrently.
   - **Key Libraries:** `pandas`, `datetime`, `concurrent.futures`, `logging`.

3. **Raw to Consumption Time Series**  
   - **Script:** `3_raw2goi.py`  
   - **Function:** Transforms the intermediate raw files into raw consumption time series.
   - **Process:**  
     - Reads the raw files into DataFrames.
     - Processes each row by applying specific rules based on the file type and number of entries. For instance, if only one entry exists, it is used directly; otherwise, the script calculates the consumption value (kWh) by aggregating values from different file types (e.g., `P5D`, `F5D`, `P1D`, `A5D`).
     - Aggregates the final results into a DataFrame with columns for datetime (`dt`), flag (`fl`), and calculated consumption (`kWh`).
     - Outputs the processed data as CSV files in the designated directory.
     - Maintains detailed logging and statistics in a special log file.
   - **Key Libraries:** `pandas`, `concurrent.futures`, `logging`.

4. **Imputation of Missing Values**  
   - **Script:** `4_goi2imp.py`  
   - **Function:** Imputes missing consumption values in the generated time series.
   - **Process:**  
     - Reads the consumption CSV files and detects duplicate timestamps or those with conflicting consumption values.
     - Removes timestamps with conflicting data and drops duplicate rows when appropriate.
     - Reindexes the data to ensure a complete hourly time series.
     - Applies a Daylight Saving Time (DST) check using `pytz` (configured for the Europe/Madrid timezone) to set the correct flag.
     - For missing kWh values, searches for historical data by comparing the same day of the week and hour from previous and following weeks, using these values to impute the gap. If historical data is unavailable, the script defaults to using the overall mean consumption.
     - Writes the imputed data to new CSV files and logs detailed processing statistics.
   - **Key Libraries:** `pandas`, `datetime`, `pytz`, `concurrent.futures`, `multiprocessing`, `logging`.

---

## Configuration

All scripts share a common configuration file (`config.json`) which must include the following keys with appropriate paths:

- **Input/Output Directories:**
  - `simel_dir`: Directory containing the raw SIMEL files.
  - `id_dir`: Directory where individual user files will be stored.
  - `raw_dir`: Directory for the intermediate raw files.
  - `goiener_dir`: Directory for the consumption time series files.
  - `imputation_dir`: Directory where the imputed files will be saved.

- **Log File Paths:**
  - `simel2id_log`: Log file for the SIMEL to user processing.
  - `id2raw_log`: Log file for the user to raw processing.
  - `raw2goiener_log`: Log file for the raw to consumption processing.
  - `goi7_log`: Special log file for detailed consumption statistics.
  - `goi72imp_log`: Log file for the imputation process.
  - `imputed_log`: Log file for imputation statistics.

Ensure the paths specified in `config.json` exist or that the scripts have permission to create them.

---

## Requirements

- **Python Version:** Python 3.x
- **Required Python Packages:**
  - `pandas`
  - `filelock`
  - `pytz`
  - Other standard libraries: `json`, `os`, `re`, `logging`, `datetime`, `multiprocessing`, `concurrent.futures`, etc.

---

## How to Run the Pipeline

Run the scripts sequentially to process the SIMEL data end-to-end:

1. **Step 1:** Split SIMEL files into user files.
   ```bash
   python 1_simel2user.py
   ```
2. **Step 2:** Process user files into intermediate raw files.
   ```bash
   python 2_user2raw.py
   ```
3. **Step 3:** Convert raw files into consumption time series.
   ```bash
   python 3_raw2goi.py
   ```
4. **Step 4:** Impute missing consumption values.
   ```bash
   python 4_goi2imp.py
   ```

Each script logs its progress and errors to its respective log file, making it easier to troubleshoot any issues that arise during processing.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## Author

**Carlos Quesada Granja**  
Universidad de Deusto  
[www.quesadagranja.com](http://www.quesadagranja.com)
