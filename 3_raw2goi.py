# -----------------------------------------------------------------------------------
# Script Name: 3_raw2goi.py
# Author: Carlos Quesada Granja
# Affiliation: Universidad de Deusto
# Website: www.quesadagranja.com
# Year: 2025
# -----------------------------------------------------------------------------------

import os
import json
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from glob import glob

def load_config(config_path):
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config

def process_file(file_path, output_dir):
    try:
        file_name = os.path.basename(file_path)
        logging.info(f"Processing file: {file_name}")
        
        df = pd.read_csv(file_path, header=None, delimiter=';', low_memory=False)
        logging.info(f"Read {len(df)} rows from {file_name}")

        processed_data = []
        max_entries = 0
        unique_count = 0
        p5d_wins = 0
        p5d_mean = 0
        f5d_wins = 0
        f5d_min = 0
        f5d_mean = 0
        p1d_wins = 0
        p1d_min = 0
        p1d_mean = 0
        a5d_wins = 0
        a5d_mean = 0
        equal_in = 0
        skipped = 0
        total_rows = len(df)

        for idx, row in df.iterrows():
            try:
                dt = row[0]
                fl = row[1]
                entries = row[2]
                max_entries = max(max_entries, entries)

                for i in range(entries):
                    entry_type = row[3 + i * 4]
                    if entry_type in ['A5D', 'B5D', 'F5D', 'P5D', 'RF5D']:
                        row[4 + i * 4] /= 1000

                kWh = None
                
                if entries == 1:
                    kWh = row[4]
                    unique_count += 1
                else:
                    p5d_ins = []
                    f5d_ins = []
                    f5d_dcmin = float('inf')
                    f5d_in_min = None
                    p1d_ins = []
                    p1d_dcmin = float('inf')
                    p1d_in_min = None
                    a5d_ins = []
                    all_ins = []

                    for i in range(entries):
                        entry_type = row[3 + i * 4]
                        entry_in = row[4 + i * 4]
                        entry_dcm = row[6 + i * 4]
                        all_ins.append(entry_in)

                        if entry_type == 'P5D':
                            p5d_ins.append(entry_in)
                        elif entry_type == 'F5D':
                            f5d_ins.append(entry_in)
                            if entry_dcm < f5d_dcmin:
                                f5d_dcmin = entry_dcm
                                f5d_in_min = entry_in
                        elif entry_type == 'P1D':
                            p1d_ins.append(entry_in)
                            if entry_dcm < p1d_dcmin:
                                p1d_dcmin = entry_dcm
                                p1d_in_min = entry_in
                        elif entry_type == 'A5D':
                            a5d_ins.append(entry_in)
                    
                    if p5d_ins:
                        if len(p5d_ins) == 1:
                            kWh = p5d_ins[0]
                            p5d_wins += 1
                        else:
                            kWh = sum(p5d_ins) / len(p5d_ins)
                            p5d_mean += 1
                    elif f5d_ins:
                        if len(f5d_ins) == 1:
                            kWh = f5d_ins[0]
                            f5d_wins += 1
                        else:
                            min_f5d_ins = [entry_in for entry_in in f5d_ins if entry_in == f5d_in_min]
                            if len(min_f5d_ins) == 1:
                                kWh = f5d_in_min
                                f5d_min += 1
                            else:
                                kWh = sum(min_f5d_ins) / len(min_f5d_ins)
                                f5d_mean += 1
                    elif p1d_ins:
                        if len(p1d_ins) == 1:
                            kWh = p1d_ins[0]
                            p1d_wins += 1
                        else:
                            min_p1d_ins = [entry_in for entry_in in p1d_ins if entry_in == p1d_in_min]
                            if len(min_p1d_ins) == 1:
                                kWh = p1d_in_min
                                p1d_min += 1
                            else:
                                kWh = sum(min_p1d_ins) / len(min_p1d_ins)
                                p1d_mean += 1
                    elif a5d_ins:
                        if len(a5d_ins) == 1:
                            kWh = a5d_ins[0]
                            a5d_wins += 1
                        else:
                            kWh = sum(a5d_ins) / len(a5d_ins)
                            a5d_mean += 1
                    else:
                        if all(x == all_ins[0] for x in all_ins):
                            kWh = all_ins[0]
                            equal_in += 1
                        else:
                            skipped += 1
                            continue

                if kWh is not None:
                    processed_data.append([dt, fl, kWh])
            except Exception as row_error:
                logging.error(f"Error processing row {idx} in {file_name}: {row_error}")

        output_df = pd.DataFrame(processed_data, columns=['dt', 'fl', 'kWh'])
        output_file_path = os.path.join(output_dir, file_name)
        output_df.to_csv(output_file_path, index=False, sep=',')

        logging.info(f"Successfully processed file: {file_name}")
        return (file_name, max_entries, total_rows, unique_count, p5d_wins, p5d_mean,
                f5d_wins, f5d_min, f5d_mean, p1d_wins, p1d_min, p1d_mean,
                a5d_wins, a5d_mean, equal_in, skipped)
    except Exception as e:
        logging.error(f"Failed to process {file_path}: {e}")
        return (os.path.basename(file_path), None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)

def setup_logging(log_file_path):
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def main():
    import os
    import json
    import logging
    from glob import glob
    from concurrent.futures import ProcessPoolExecutor, as_completed

    # Cargar configuración
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, 'config.json')
    config = load_config(config_path)

    input_pattern = os.path.join(config['raw_dir'], '*.csv')
    output_dir = config['goiener_dir']
    log_file_path = config['raw2goiener_log']
    special_log_file = config['goi7_log']

    # Configurar logging y asegurarse que exista el directorio de salida
    setup_logging(log_file_path)
    os.makedirs(output_dir, exist_ok=True)

    # Obtener lista de archivos y abortar si está vacía
    input_files = glob(input_pattern)
    if not input_files:
        logging.warning("No se encontraron archivos para procesar.")
        return

    num_workers = max(1, os.cpu_count() - 1)

    # Abrir el log especial en modo append y escribir la cabecera si el archivo está vacío
    with open(special_log_file, 'a') as spec_log:
        if os.stat(special_log_file).st_size == 0:
            spec_log.write("fname,max_entries,rows,unique,p5d_wins,p5d_mean,f5d_wins,f5d_min,f5d_mean,"
                           "p1d_wins,p1d_min,p1d_mean,a5d_wins,a5d_mean,equal_in,skipped\n")
        # Procesar archivos en paralelo y escribir cada resultado a medida que se obtiene
        with ProcessPoolExecutor(max_workers=num_workers,
                                 initializer=setup_logging,
                                 initargs=(log_file_path,)) as executor:
            futures = {executor.submit(process_file, file, output_dir): file for file in input_files}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    # Escribir sólo si el resultado es válido (max_entries distinto de None)
                    if result[1] is not None:
                        spec_log.write("{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format(*result))
                        spec_log.flush()
                        os.fsync(spec_log.fileno())
                except Exception as e:
                    logging.error("Error al procesar {}: {}".format(futures[future], e))

if __name__ == "__main__":
    main()
