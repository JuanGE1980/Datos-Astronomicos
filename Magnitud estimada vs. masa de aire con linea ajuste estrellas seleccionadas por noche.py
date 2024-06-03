import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Crear el motor con SQLAlchemy (asegúrate de que tienes instalado el driver correcto)
engine = create_engine('mssql+pyodbc://FAMILIAESCOBARS\\SQLEXPRESS/Astronomia_2?driver=ODBC+Driver+17+for+SQL+Server')

# Consulta SQL para obtener todos los datos de dos estrellas específicas
query = """
SELECT gaia_id AS estrella, estimated_magnitude, m2_est_air_mass, observing_time, telescope
FROM Estrellas
WHERE gaia_id IN (6767484242078561280, 6796437136312506624, 3661362244342230528, 4432582343794560, 4073445394156869632, 4433200819082624, 7361848823481600, 
                  3661356815503307008, 7372947018995328, 6767495890027591808, 3661366401870324608, 12286290822898176, 6783489459181109248) 
ORDER BY observing_time, telescope, estrella;
"""
#AND CAST(observing_time AS DATE) = '01/10/2019' (4073347194031694080, 4073453636154923392) , 11962037971159168, 12251931084499968, 12248769988622464, 6796435830643150720  
#(6783487363237069952, 6796434864272818176, 11891772306304640) - (6783489111287543040, 12053675394401920,6796435693203396352, 12043058235299968) - (12053675394401920, 12053915912567424, 12066388497586816, 12060787860237184, 12255573216808960)

# Cargar los datos en un DataFrame usando SQLAlchemy
df = pd.read_sql_query(query, engine)

# Convertir la columna 'observing_time' a tipo datetime utilizando errors='coerce'
df['observing_time'] = pd.to_datetime(df['observing_time'], errors='coerce')
df = df.dropna(subset=['observing_time'])

# Extraer la fecha de 'observing_time'
df['date'] = df['observing_time'].dt.date

# Definir el número de estrellas a graficar (en este caso, siempre 2)
n_estrellas = 10

# Generar colores únicos para cada estrella
colors = plt.cm.viridis(np.linspace(0, 1, n_estrellas))

# Iterar sobre cada fecha única y crear un gráfico para cada noche
for fecha in df['date'].unique():
    plt.figure(figsize=(10, 6))
    has_labels = False  # Variable para verificar si hay etiquetas
    datos_fecha = df[df['date'] == fecha]
    
    for idx, estrella in enumerate(datos_fecha['estrella'].unique()):
        datos_estrella = datos_fecha[datos_fecha['estrella'] == estrella]
        x = datos_estrella['m2_est_air_mass']
        y = datos_estrella['estimated_magnitude']
        
        # Verificar si hay suficientes datos para realizar un ajuste
        if len(x) > 1 and x.var() > 0 and y.var() > 0:
            print(f'Estrella ID: {estrella} - Cantidad de observaciones: {len(x)}')
            
            # Ajustar una línea a los datos
            coeffs = np.polyfit(x, y, 1)
            line_fit = np.polyval(coeffs, np.sort(x))
            
            # Graficar los datos y la línea de ajuste
            plt.scatter(x, y, alpha=0.7, s=30, color=colors[idx], label=f'({coeffs[0]:.4f})')
            plt.plot(np.sort(x), line_fit, color=colors[idx])
            has_labels = True

    plt.xlabel('Masa de Aire')
    plt.ylabel('Magnitud Estimada')
    plt.title(f'Magnitud Estimada vs. Masa de Aire para el {fecha}')
    plt.grid(True)
    plt.ylim(10, 20)
    plt.yticks(np.arange(10, 20, 1))
    if has_labels:
        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize='small')
    plt.tight_layout()
    plt.show()


# Función para graficar todos los datos combinados para todas las noches
def graficar_todas_noches(df):
    plt.figure(figsize=(10, 6))
    has_labels = False  # Variable para verificar si hay etiquetas
    
    for idx, estrella in enumerate(df['estrella'].unique()):
        datos_estrella = df[df['estrella'] == estrella]
        x = datos_estrella['m2_est_air_mass']
        y = datos_estrella['estimated_magnitude']
        
        # Verificar si hay suficientes datos para realizar un ajuste
        if len(x) > 1 and x.var() > 0 and y.var() > 0:
            # Ajustar una línea a los datos
            coeffs = np.polyfit(x, y, 1)
            line_fit = np.polyval(coeffs, np.sort(x))
            
            # Graficar los datos y la línea de ajuste
            plt.scatter(x, y, alpha=0.7, s=30, color=colors[idx], label=f'({coeffs[0]:.4f})')
            plt.plot(np.sort(x), line_fit, color=colors[idx])
            has_labels = True

    plt.xlabel('Masa de Aire')
    plt.ylabel('Magnitud Estimada')
    plt.title('Magnitud Estimada vs. Masa de Aire')
    plt.grid(True)
    plt.ylim(10, 20)
    plt.yticks(np.arange(10, 20, 1))
    if has_labels:
        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize='small')
    plt.tight_layout()
    plt.show()

# Graficar todos los datos combinados para todas las noches
graficar_todas_noches(df)