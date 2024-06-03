import zipfile
import os
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy import text
import hashlib

# Configuración de la base de datos
server = 'FAMILIAESCOBARS\SQLEXPRESS'
database = 'Astronomia_2'
driver = 'ODBC Driver 17 for SQL Server'
connection_string = f"mssql+pyodbc://{server}/{database}?trusted_connection=yes&driver={driver.replace(' ', '+')}"
engine = create_engine(connection_string)

# Configuración de las rutas para conectar la carpeta origen y carpeta para extracción de archivos. Se sugiere usar rutas completas para evitar problemas
zip_directory_path = r'C:\\Users\\guillermo\\Desktop\\Archivos Astronomicos'
extract_dir = r'C:\\Extracciones'

# Columnas únicas para cada tabla
unique_keys = {
    'detection_summary': 'image_name',
    'estimated_magnitude': 'image_name',
    'GAIA_DR3!JOHNSON_I': 'image_name',
    'GAIA_DR3!JOHNSON_R': 'image_name',
    'GAIA_DR3!JOHNSON_V': 'image_name',
    'image_content': 'source_file',
    'image_metadata': 'image_name',
    'image_seq': 'file_name',
    'jpl_orbit_residuals_summary': '_id',
    'light_curve': 'image_name',
    'orbit_residual_dec': 'image',
    'orbit_residual_discarded_dec': 'image',
    'orbit_residual_discarded_ra': 'image',
    'orbit_residual_ra': 'image',
    'photometric_models_summary': 'image_name',
    'time_diff_magnitude_corrected': 'image',
    # Tablas sin identificador único conocido
    'gaia_known_sources': None,
    'gaia_multiple_sources': None,
    'gaia_unknown_sources': None,
    'image_duplicated': None,
    'lomb_scargle_periodogram': None,
    'solution_seq': None,
    'solution_summary': None,
    'summary': None
}

# Función para extracción de archivos ZIP
def extract_zip_files(zip_directory_path, extract_dir):
    for file in os.listdir(zip_directory_path):
        if file.endswith('.zip'):
            zip_path = os.path.join(zip_directory_path, file)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for member in zip_ref.infolist():
                    extracted_path = os.path.join(extract_dir, member.filename.replace('/', '\\'))
                    if len(extracted_path) >= 260:
                        # Reducir y garantizar un único nombre usando un hash
                        hash_part = hashlib.sha256(member.filename.encode()).hexdigest()[:8]
                        extracted_path = '\\'.join([part[:10] for part in extracted_path.split('\\')]) + hash_part
                    os.makedirs(os.path.dirname(extracted_path), exist_ok=True)
                    if not member.is_dir():
                        with open(extracted_path, 'wb') as output_file:
                            output_file.write(zip_ref.read(member))
                        print("Extraído:", extracted_path)

# Función para dar manejo a las columnas duplicadas en los CSV
def renombrar_columnas_duplicadas(df):
    cols = pd.Series(df.columns).map(lambda x: x.lower())
    for dup in cols[cols.duplicated()].unique():
        cols[cols == dup] = [f"{dup}_{i}" for i in range(sum(cols == dup))]
    df.columns = cols
    return df

# Función para leer archivos CSV y convertirlos en DataFrames
def leer_archivos_csv(extract_dir, carpeta_especifica):
    dataframes = {} #Diccionario para almacenar los DataFrames individuales
    dataframe_consolidado = pd.DataFrame()  # DataFrame para consolidar archivos específicos
    print(f"Buscando archivos CSV en: {extract_dir}")
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                try:
                    df = pd.read_csv(file_path, delimiter='\t') # Ajusta  el delimitador '\t' para tabuladores
                    df = renombrar_columnas_duplicadas(df)  # Aplica la función para Renombrar columnas duplicadas
                    df.columns = [col[:128] if len(col) > 128 else col for col in df.columns]  # Truncar nombres de columnas largos
                    
                    # Cambio de nombres de columnas específicas antes de la inserción en la base de datos
#                    df.rename(columns={
#                        'balanced_normalized_flux': 'flux_per_second',
#                        'balanced_estimated_absolute_magnitude': 'estimated_absolute_magnitude',
#                        'normalized_flux(counts/s)': 'flux_per_second(counts/s)'
#                    }, inplace=True)
                    
                    filename_without_extension = os.path.splitext(os.path.basename(file_path))[0] # Elimina la extensión .csv del nombre del archivo antes de añadirlo a la columna 'source_file'
                    if carpeta_especifica in root and subcarpeta_especifica in root:
                        df['source_file'] = filename_without_extension  # Añade el nombre del archivo sin la extensión .csv
                        dataframe_consolidado = pd.concat([dataframe_consolidado, df], ignore_index=True)
                    else:
                        dataframes[file_path] = df
 
                    print(f"Archivo {file_path} leído exitosamente. Columnas: {df.columns.tolist()}")
                except Exception as e:
                    print(f"Error al leer el archivo {file_path}: {e}")
    
    return dataframes, dataframe_consolidado

# Función que inserta un identificador por fila para las tablas que no tienen columnas únicas
def calcular_hash_de_fila(row):
    return hashlib.sha256(str(tuple(row)).encode('utf-8')).hexdigest()

# Función para validación de datos y manejo de registros duplicados
def verificar_y_insertar_datos(engine, df, table_name):
    unique_col = unique_keys.get(table_name)
    with engine.connect() as conn:
        inspector = inspect(engine)
        table_exists = inspector.has_table(table_name)
        
        # Crear la tabla si no existe, asumiendo que la primera carga se hace sin duplicados
        if not table_exists:
            df.to_sql(table_name, conn, index=False, if_exists='replace')
            print(f"Tabla {table_name} creada y todos los datos insertados porque no existía.")
            return
        
        if unique_col and unique_col in df.columns:
            # Verificar duplicados usando la columna única
            existing_data = pd.read_sql_table(table_name, conn, columns=[unique_col])
            df = df[~df[unique_col].isin(existing_data[unique_col])]
        else:
            # Asegurarse de que la columna row_hash exista
            if 'row_hash' not in [col['name'] for col in inspector.get_columns(table_name)]:
                conn.execute(text(f"ALTER TABLE {table_name} ADD row_hash VARCHAR(64)"))
            df['row_hash'] = df.apply(calcular_hash_de_fila, axis=1)
            existing_hashes = pd.read_sql(f"SELECT row_hash FROM {table_name}", conn)['row_hash']
            df = df[~df['row_hash'].isin(existing_hashes)]
            df.drop(columns=['row_hash'], inplace=True)  # Eliminar la columna de hash antes de insertar

        if not df.empty:
            df.to_sql(table_name, conn, if_exists='append', index=False)
            print(f"Datos nuevos insertados correctamente en la tabla {table_name}.")
        else:
            print(f"No se encontraron datos nuevos para insertar en la tabla {table_name}.")

def crear_tablas_y_insertar_datos(engine, dataframes, dataframe_consolidado, image_content):
    # Insertar dataframes generales
    for file_path, df in dataframes.items():
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        verificar_y_insertar_datos(engine, df, table_name)
    
    # Insertar el dataframe consolidado
    if not dataframe_consolidado.empty:
        verificar_y_insertar_datos(engine, dataframe_consolidado, image_content)
    else:
        print(f"No hay datos para insertar en la tabla consolidada {image_content}.")


# Código para ejecutar el proceso completo de extracción de archivos ZIP, lectura y proceamiento de archivos CSV y creación de tablas e inserción de datos en la BD
carpeta_especifica = '0_source_detection'
subcarpeta_especifica = "image_content"
nombre_tabla_consolidada = 'image_content'

extract_zip_files(zip_directory_path, extract_dir)
dataframes, dataframe_consolidado = leer_archivos_csv(extract_dir, "0_source_detection")
crear_tablas_y_insertar_datos(engine, dataframes, dataframe_consolidado, nombre_tabla_consolidada)
