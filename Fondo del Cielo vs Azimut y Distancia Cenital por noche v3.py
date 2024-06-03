from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime

# Crear el motor con SQLAlchemy
engine = create_engine('mssql+pyodbc://FAMILIAESCOBARS\\SQLEXPRESS/Astronomia_2?driver=ODBC+Driver+17+for+SQL+Server')

# Consulta SQL para obtener los datos
query = """
SELECT gaia_id AS estrellas, background, m2_est_air_mass, observing_date, telescope, [m2_est_azimut(deg)], [m2_est_zenith(deg)]
FROM Estrellas
ORDER BY observing_date, telescope, estrellas;
"""

# Cargar los datos en un DataFrame usando SQLAlchemy
df = pd.read_sql_query(query, engine)

# Agrupar por telescopio
telescopios = df['telescope'].unique()

# Configurar el tamaño y la cantidad de subplots
num_telescopios = len(telescopios)
cols = 2  # Número de columnas de subplots
rows = (num_telescopios // cols) + (num_telescopios % cols > 0)

fig1, axs1 = plt.subplots(rows, cols, figsize=(10, 4 * rows))
fig2, axs2 = plt.subplots(rows, cols, figsize=(10, 4 * rows))
axs1 = axs1.flatten()
axs2 = axs2.flatten()

for i, telescopio in enumerate(telescopios):
    datos_telescopio = df[df['telescope'] == telescopio]
    noches = datos_telescopio['observing_date'].unique()
    
    # Gráfico 1: Fondo del cielo vs. Azimut para cada noche
    for noche in noches:
        datos_noche = datos_telescopio[datos_telescopio['observing_date'] == noche]
        axs1[i].scatter(datos_noche['m2_est_azimut(deg)'], datos_noche['background'], alpha=0.5, s=1, color='green')

    axs1[i].set_xlabel('Azimut (Grados)', fontsize=8)
    axs1[i].set_ylabel('Fondo del Cielo (Cuentas)', fontsize=8)
    axs1[i].set_title(f'Fondo del Cielo vs. Azimut\nTelescopio: {telescopio}', fontsize=8, pad=10)
    axs1[i].tick_params(axis='both', which='major', labelsize=7)
    axs1[i].grid(True)

    # Gráfico 2: Fondo del cielo vs. Distancia Cenital para cada noche
    for noche in noches:
        datos_noche = datos_telescopio[datos_telescopio['observing_date'] == noche]
        axs2[i].scatter(datos_noche['m2_est_zenith(deg)'], datos_noche['background'], alpha=0.5, s=1, color='red')

    axs2[i].set_xlabel('Distancia Cenital (Grados)', fontsize=8)
    axs2[i].set_ylabel('Fondo del Cielo (Cuentas)', fontsize=8)
    axs2[i].set_title(f'Fondo del Cielo vs. Distancia Cenital\nTelescopio: {telescopio}', fontsize=8, pad=10)
    axs2[i].tick_params(axis='both', which='major', labelsize=7)
    axs2[i].grid(True)

# Ocultar cualquier subplot vacío si hay
for j in range(i + 1, len(axs1)):
    fig1.delaxes(axs1[j])
    fig2.delaxes(axs2[j])

# Ajustar el espacio entre subplots
fig1.tight_layout(pad=1.5)
fig2.tight_layout(pad=1.5)

# Añadir más espacio vertical entre subplots
fig1.subplots_adjust(hspace=0.5)
fig2.subplots_adjust(hspace=0.5)

plt.show()


