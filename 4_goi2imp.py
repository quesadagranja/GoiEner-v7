# -----------------------------------------------------------------------------------
# Script Name: 4_goi2imp.py
# Author: Carlos Quesada Granja
# Affiliation: Universidad de Deusto
# Website: www.quesadagranja.com
# Year: 2025
# -----------------------------------------------------------------------------------

import os
import pandas as pd
from datetime import datetime, timedelta
import pytz
from concurrent.futures import ProcessPoolExecutor
import json
from multiprocessing import cpu_count, Manager

# Cargar configuración desde config.json
with open('config.json', 'r') as file:
    config = json.load(file)

# Obtener la ruta del log CSV desde config.json
log_csv = config["goi72imp_log"]

# Crear el directorio del log si no existe
os.makedirs(os.path.dirname(log_csv), exist_ok=True)

# Definir la función para convertir UTC a CET
def transform_utc_to_cet(utc_dt):
    utc_zone = pytz.utc
    cet_zone = pytz.timezone('Europe/Madrid')
    utc_dt = utc_zone.localize(utc_dt)
    cet_dt = utc_dt.astimezone(cet_zone)
    return cet_dt

# Definir la función para verificar horario de verano
def check_dst(utc_dt):
    cet_dt = transform_utc_to_cet(utc_dt - timedelta(hours=1))
    return 1 if cet_dt.dst() != timedelta(0) else 0

# Función para procesar archivos CSV e imputar valores
def impute_values(file_path, output_folder, stats_list):
    try:
        df = pd.read_csv(file_path, parse_dates=['dt'])

        # Contar duplicados antes de eliminarlos
        rep_count = df.duplicated(subset=['dt'], keep=False).sum()

        # Identificar timestamps con valores diferentes de kWh
        duplicate_groups = df.groupby('dt')['kWh'].nunique()
        conflict_timestamps = duplicate_groups[duplicate_groups > 1].index

        # Eliminar completamente los timestamps con valores de kWh distintos
        df = df[~df['dt'].isin(conflict_timestamps)]

        # Eliminar duplicados si tienen el mismo valor en kWh
        df = df.drop_duplicates(subset=['dt', 'kWh'], keep='first')

        # Establecer índice en 'dt'
        df.set_index('dt', inplace=True)

        # Generar el rango completo de fechas
        full_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq='h')

        # Reindexar para asegurarse de que no falten timestamps
        df = df.reindex(full_index)

        df['fl'] = df.index.map(check_dst)
        df['imp'] = df.apply(lambda row: 0 if pd.notna(row['kWh']) else 1, axis=1)

        # Función para imputar valores faltantes
        def impute_kwh(row):
            if pd.notna(row['kWh']):
                return row['kWh']
            
            target_dt = row.name
            day_of_week = target_dt.weekday()
            hour_of_day = target_dt.hour

            past_values = []
            future_values = []

            past_dt = target_dt - timedelta(weeks=1)
            future_dt = target_dt + timedelta(weeks=1)

            while past_dt >= df.index.min() or future_dt <= df.index.max():
                if past_dt >= df.index.min() and past_dt.weekday() == day_of_week and past_dt.hour == hour_of_day and pd.notna(df.loc[past_dt, 'kWh']):
                    past_values.append(df.loc[past_dt, 'kWh'])
                
                if future_dt <= df.index.max() and future_dt.weekday() == day_of_week and future_dt.hour == hour_of_day and pd.notna(df.loc[future_dt, 'kWh']):
                    future_values.append(df.loc[future_dt, 'kWh'])

                if len(past_values) >= 1 and len(future_values) >= 1:
                    break
                
                past_dt -= timedelta(weeks=1)
                future_dt += timedelta(weeks=1)

            if len(past_values) > 0 and len(future_values) > 0:
                return round((past_values[0] + future_values[0]) / 2, 3)
            elif len(past_values) > 0:
                return past_values[0]
            elif len(future_values) > 0:
                return future_values[0]
            else:
                return round(df['kWh'].mean(), 3)

        df['kWh'] = df.apply(impute_kwh, axis=1)

        # Guardar el archivo corregido
        output_file = os.path.join(output_folder, os.path.basename(file_path))
        df.reset_index().to_csv(output_file, index=False)

        # Guardar en el CSV inmediatamente después de cada archivo procesado
        stats_df = pd.DataFrame([{
            'dt': datetime.utcnow().isoformat(),
            'fname': os.path.basename(file_path),
            'rep': rep_count,
            'samples': len(df),
            'imp': sum(df['imp'] > 0)
        }])

        stats_df.to_csv(log_csv, mode='a', header=not os.path.exists(log_csv), index=False)

    except Exception as e:
        # Registrar el error en la terminal para depuración
        print(f"Error procesando {file_path}: {e}")
    
        # Si hay un error, asegurarse de que file_path sigue siendo accesible
        error_fname = os.path.basename(file_path) if file_path else "unknown_file"
    
        stats_df = pd.DataFrame([{
            'dt': None,
            'fname': error_fname,
            'rep': -1,  # Indica que hubo un error en este archivo
            'samples': 0,
            'imp': 0
        }])
    
        stats_df.to_csv(log_csv, mode='a', header=not os.path.exists(log_csv), index=False)

# Función para procesar múltiples archivos en paralelo
def process_files(config_path):
    with open(config_path, 'r') as file:
        config = json.load(file)
    
    input_folder = config['goiener_dir']
    output_folder = config['imputation_dir']
    stats_log_path = config['imputed_log']
    
    os.makedirs(output_folder, exist_ok=True)

    files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith('.csv')]

    with Manager() as manager:
        stats_list = manager.list()

        num_workers = max(1, cpu_count() - 1)  # Usa todos los núcleos menos uno
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            executor.map(impute_values, files, [output_folder]*len(files), [stats_list]*len(files))

        # Guardar estadísticas en un CSV
        stats_df = pd.DataFrame(list(stats_list))
        stats_df.to_csv(stats_log_path, index=False)

# Ejecutar el procesamiento
config_path = 'config.json'
process_files(config_path)
