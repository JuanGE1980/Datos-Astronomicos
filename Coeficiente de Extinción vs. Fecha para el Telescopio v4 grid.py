from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter

# Crear el motor con SQLAlchemy
engine = create_engine(r'mssql+pyodbc://FAMILIAESCOBARS\SQLEXPRESS/Astronomia_2?driver=ODBC+Driver+17+for+SQL+Server')

# Consulta SQL para obtener todos los datos
query = """
SELECT gaia_id AS estrella, estimated_magnitude, m2_est_air_mass, observing_time, telescope
FROM Estrellas
ORDER BY observing_time, telescope, estrella;
"""

# Cargar los datos en un DataFrame usando SQLAlchemy
df = pd.read_sql_query(query, engine)

# Convertir 'observing_time' a tipo datetime utilizando el formato ISO8601
df['observing_time'] = pd.to_datetime(df['observing_time'], format='ISO8601')

# Obtener la lista única de telescopios
telescopios = df['telescope'].unique()

# Configurar los subplots
cols = 2  # Número de columnas de subplots
rows = 3  # Número fijo de filas
fig, axs = plt.subplots(rows, cols, figsize=(10, 4* rows), constrained_layout=True)
axs = axs.flatten() if rows > 1 else [axs]

# Índice para asignar los gráficos a los subplots
plot_index = 0

# Para cada telescopio, calcular la variación en coeficientes de extinción a lo largo del tiempo y graficar
for i, telescopio in enumerate(telescopios):
    df_telescopio = df[df['telescope'] == telescopio]
    fechas = df_telescopio['observing_time'].dt.date.unique()
    extincion_por_noche = []

    for fecha in fechas:
        datos_noche = df_telescopio[df_telescopio['observing_time'].dt.date == fecha]
        coeficientes_extincion = []

        for estrella in datos_noche['estrella'].unique():
            datos_estrella = datos_noche[datos_noche['estrella'] == estrella]
            if len(datos_estrella) > 1 and datos_estrella[['m2_est_air_mass', 'estimated_magnitude']].notnull().all().all() and datos_estrella['m2_est_air_mass'].var() > 0:
                coeffs = np.polyfit(datos_estrella['m2_est_air_mass'], datos_estrella['estimated_magnitude'], 1)
                coeficiente_extincion = coeffs[0]
                if -1 <= coeficiente_extincion <= 1:
                    coeficientes_extincion.append(coeficiente_extincion)

        if coeficientes_extincion:
            promedio_extincion = np.mean(coeficientes_extincion)
            extincion_por_noche.append((fecha, promedio_extincion))

    # Preparar datos para plot
    if extincion_por_noche:
        fechas, extinciones = zip(*extincion_por_noche)
        axs[plot_index].plot(fechas, extinciones, marker='*', linestyle='', color='blue')
        axs[plot_index].set_title(f'Coeficiente de Extinción vs. Fecha para el Telescopio {telescopio}', fontsize=8)
        axs[plot_index].set_xlabel('Fecha', fontsize=6)
        axs[plot_index].set_ylabel('Coeficiente de Extinción', fontsize=6)
        axs[plot_index].grid(True)
        axs[plot_index].xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
        axs[plot_index].tick_params(axis='x', rotation=0, labelsize=6)
        axs[i].tick_params(axis='y', labelsize=6)
        plot_index += 1  # Incrementar índice para el próximo subplot


# Ocultar subplots vacíos si hay menos telescopios que subplots
for j in range(plot_index, len(axs)):
    axs[j].axis('off')  # Ocultar los ejes del subplot vacío

# Ajustar el espacio entre subplots
plt.subplots_adjust(hspace=1.5, wspace=0.5)

# Ajustar el layout y mostrar el gráfico
plt.tight_layout(pad=2.0, h_pad=2.0, w_pad=2.0)  # Ajusta los paddings aquí
plt.show()

# Mostrar los coeficientes de extinción promedio para cada noche
for fecha, coef in extincion_por_noche:
    print(f"Fecha: {fecha}, Promedio coef. extinción: {coef:.3f}")