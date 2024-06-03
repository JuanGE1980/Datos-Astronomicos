import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# Crear el motor con SQLAlchemy
engine = create_engine('mssql+pyodbc://FAMILIAESCOBARS\\SQLEXPRESS/Astronomia_2?driver=ODBC+Driver+17+for+SQL+Server')

# Consulta SQL
query = """
SELECT gaia_id AS estrella, estimated_magnitude, m2_est_air_mass, observing_time, telescope
FROM Estrellas
ORDER BY observing_time, telescope, estrella;
"""

# Cargar los datos en un DataFrame usando SQLAlchemy
df = pd.read_sql_query(query, engine)

# Crear una columna adicional 'observing_date' para almacenar solo la fecha
df['observing_date'] = df['observing_time'].str.split('T').str[0]

# Lista para almacenar los coeficientes de extinción promedio por noche
extincion_noche = []

# Lista para almacenar los histogramas
histogram_data = []

# Iterar sobre cada fecha
for fecha in df['observing_date'].unique():
    datos_noche = df[df['observing_date'] == fecha]
    coeficientes_extincion = []

    for estrella in datos_noche['estrella'].unique():
        datos_estrella = datos_noche[datos_noche['estrella'] == estrella]
        if len(datos_estrella) > 1:
            # Verificar si los datos no contienen valores nulos y tienen suficiente variabilidad
            if datos_estrella[['m2_est_air_mass', 'estimated_magnitude']].notnull().all().all():
                if datos_estrella['m2_est_air_mass'].var() > 0 and datos_estrella['estimated_magnitude'].var() > 0:
                    try:
                        coeffs = np.polyfit(datos_estrella['m2_est_air_mass'], datos_estrella['estimated_magnitude'], 1)
                        coeficiente_extincion = coeffs[0]
                        # Filtrar coeficientes para mantener solo los que están entre -1 y 1
                        if -2 <= coeficiente_extincion <= 2:
                            coeficientes_extincion.append(coeficiente_extincion)
                    except np.linalg.LinAlgError:
                        pass  # Omitir errores de ajuste sin imprimir mensaje
                else:
                    pass  # Omitir mensajes de variabilidad insuficiente
            else:
                pass  # Omitir mensajes de datos no numéricos o valores nulos

    # Calcular y graficar solo si hay coeficientes válidos
    if coeficientes_extincion:
        coeficientes_validos = [coef for coef in coeficientes_extincion if -2 <= coef <= 2]
        if coeficientes_validos:
            promedio_extincion = np.mean(coeficientes_validos)
            extincion_noche.append((fecha, promedio_extincion))
            histogram_data.append((fecha, coeficientes_validos, promedio_extincion))

# Configurar el tamaño y la cantidad de subplots
num_histograms = len(histogram_data)
cols = 5  # Número de columnas de subplots
rows = (num_histograms // cols) + (num_histograms % cols > 0)

fig, axs = plt.subplots(rows, cols, figsize=(18, 3 * rows))  # Aumentar el tamaño de la figura
axs = axs.flatten()

# Graficar cada histograma en su subplot correspondiente
for i, (fecha, coeficientes_validos, promedio_extincion) in enumerate(histogram_data):
    axs[i].hist(coeficientes_validos, bins=10, alpha=0.7, color='blue', edgecolor='black')
    axs[i].axvline(x=promedio_extincion, color='red', linestyle='--', label=f'Promedio: {promedio_extincion:.3f}')
    axs[i].set_xlabel('Coeficiente de Extinción', fontsize=6)
    axs[i].set_ylabel('Frecuencia', fontsize=6)
    axs[i].set_title(f'Distribución de Coeficientes de Extinción\n{fecha}', fontsize=7)
    axs[i].grid(True)
    axs[i].legend(fontsize=4)
    axs[i].tick_params(axis='both', which='major', labelsize=3)

# Ocultar cualquier subplot vacío si hay
for j in range(i + 1, len(axs)):
    fig.delaxes(axs[j])

# Ajustar el espacio entre subplots
plt.subplots_adjust(hspace=1.5, wspace=0.5)

plt.show()

# Mostrar los coeficientes de extinción promedio para cada noche
for fecha, coef in extincion_noche:
    print(f"Fecha: {fecha}, Promedio coef. extinción: {coef:.3f}")
