# -----------------------------------------------------------------------------------
# Script Name: 1_simel2user.py
# Author: Carlos Quesada Granja
# Affiliation: Universidad de Deusto
# Website: www.quesadagranja.com
# Year: 2025
# -----------------------------------------------------------------------------------

import os
import re
import json
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor
from glob import glob
from filelock import FileLock

def load_config(config_path):
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config

def process_file(file_path, id_dir):
    try:
        file_name = os.path.basename(file_path)
        file_prefix = file_name.split('_')[0]
        
        # Read the file into a DataFrame
        df = pd.read_csv(file_path, sep=';', header=None)
        
        # Add the new columns
        df.insert(0, 'original_file', file_name)
        df.insert(1, 'file_prefix', file_prefix)
        
        # Group by the ID (assuming the ID is in the first column)
        grouped = df.groupby(df.columns[2])  # Adjusting to group by the third column which is the original ID column
        
        # Write each group to the corresponding ID file
        for id_value, group in grouped:
            id_file_path = os.path.join(id_dir, f"{id_value}.csv")
            lock_file_path = f"{id_file_path}.lock"
            with FileLock(lock_file_path):
                if os.path.exists(id_file_path):
                    group.to_csv(id_file_path, sep=';', mode='a', header=False, index=False)
                else:
                    group.to_csv(id_file_path, sep=';', mode='w', header=False, index=False)
                
        return f"Processed {file_name}"
    except Exception as e:
        return f"Failed to process {file_name}: {e}"

def setup_logging(simel2id_log):
    logging.basicConfig(
        filename=simel2id_log,
        level=logging.DEBUG,  # Cambiar a DEBUG para más información
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger().addHandler(logging.StreamHandler())  # Añadir salida a la consola


def main():
    print("Iniciando script...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.json')

    print(f"Cargando configuración desde {config_path}...")
    config = load_config(config_path)
    
    simel_files_pattern = os.path.join(config['simel_dir'], '*')
    id_dir = config['id_dir']
    simel2id_log = os.path.join(script_dir, config['simel2id_log'])
    
    print(f"Configuración cargada. simel_dir: {config['simel_dir']}, id_dir: {id_dir}")
    
    setup_logging(simel2id_log)

    os.makedirs(id_dir, exist_ok=True)
    
    print(f"Buscando archivos en {simel_files_pattern}...")
    all_files = glob(simel_files_pattern)
    
    print(f"Se encontraron {len(all_files)} archivos en la carpeta.")
    
    pattern = re.compile(r'^(A5D|B5D|F5D|P5D|RF5D|F1|P1|P1D)_.*\.\d+$')
    simel_files = [f for f in all_files if pattern.match(os.path.basename(f))]
    
    print(f"Se encontraron {len(simel_files)} archivos que cumplen el patrón.")

    if not simel_files:
        print("No hay archivos que coincidan con el patrón. Saliendo...")
        return
    
    print("Procesando archivos con ProcessPoolExecutor...")
    
    num_workers = max(1, os.cpu_count() - 1)  # Evita que el número de workers sea 0
    
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        results = list(executor.map(process_file, simel_files, [id_dir]*len(simel_files)))

    print("Procesamiento finalizado. Guardando logs...")

    for result in results:
        logging.info(result)
    
    print("Script terminado correctamente.")



if __name__ == "__main__":
    main()
