# Procesamiento en batch

## Descripción

Este proyecto realiza un análisis de datos en batch utilizando Apache Spark sobre un conjunto de datos de vehículos involucrados en accidentes de tránsito en el municipio de Tuluá.

El objetivo es identificar patrones en los accidentes.

## Dataset

Fuente: Datos Abiertos de Colombia

Archivo: anem-hgsc.csv

Variables principales:
- Marca del vehículo
- Modelo del vehículo
- Tipo de vehículo
- Edad del vehículo
- Fecha del accidente
- Gravedad del accidente
- Departamento del accidente
- Municipio del accidente
- Autoridad de tránsito

## Tecnologías utilizadas

- Apache Spark
- Python (PySpark)
- Hadoop HDFS

## Procesamiento realizado

### 1. Carga de datos

Se cargan los datos desde HDFS utilizando Spark DataFrames.

### 2. Limpieza de datos

- Eliminación de valores nulos
- Eliminación de registros duplicados

### 3. Transformación de datos

* Creación de la columna `año_accidente` a partir de la fecha

### 4. Análisis exploratorio (EDA)

- Vehículos con más de 10 años
- Cantidad de accidentes por tipo
- Cantidad por gravedad
- Marcas más involucradas

## Ejecución

1. Subir dataset a HDFS:

```
hdfs dfs -put /home/hadoop/anem-hgsc.csv /Tarea3EdithAngel
```

2. Ejecutar el programa:

```
python3 tarea3_edith_angel.py
```

## Resultados principales

- Las motocicletas presentan mayor número de accidentes
- La mayoría de los accidentes son con heridos
- Marcas como Yamaha, Honda y Chevrolet son las más frecuentes

## Autor

Edith Karina Angel Bejarano
