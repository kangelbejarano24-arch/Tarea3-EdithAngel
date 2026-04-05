# Procesamiento en Batch

# Descripción

Este proyecto realiza un análisis de datos en batch utilizando Apache Spark sobre un conjunto de datos de accidentes de tránsito en Colombia.

El dataset incluye información como:
- Marca del vehículo
- Tipo de vehículo
- Edad del vehículo
- Fecha del accidente
- Gravedad del accidente

El objetivo es identificar patrones y factores asociados a los accidentes.

# Tecnologías utilizadas

- Apache Spark
- Python (PySpark)
- Hadoop HDFS

## Procesamiento realizado

# 1. Carga de datos
Se carga el dataset desde HDFS utilizando Spark.

# 2. Limpieza de datos
- Eliminación de valores nulos
- Eliminación de duplicados

# 3. Transformación
- Creación de la columna "año_accidente"

# 4. Análisis exploratorio (EDA)
- Vehículos con más de 10 años
- Vehículos ordenados por edad
- Cantidad de accidentes por tipo de vehículo
- Cantidad de accidentes por gravedad
- Marcas más involucradas

# Ejecución

1. Asegurarse de tener Hadoop y Spark ejecutándose
2. Cargar el dataset en HDFS:
   hdfs dfs -put /home/hadoop/anem-hgsc.csv /Tarea3EdithAngel

3. Ejecutar el programa:
   python3 tarea3_edith_angel.py

# Resultados

El análisis permitió identificar:
- Las motocicletas como el tipo de vehículo con más accidentes
- Mayor cantidad de accidentes con heridos
- Marcas como Yamaha y Honda con alta participación

# Autor

Edith karina Angel Bejarano
