# Procesamiento en Batch

## Descripción del proceso

El procesamiento en batch se realizó utilizando Apache Spark, cargando un dataset de accidentes de tránsito desde HDFS.

## Etapas del procesamiento

### Carga de datos

Se cargó el archivo CSV desde HDFS usando Spark DataFrames.

### Limpieza de datos

Se eliminaron valores nulos y registros duplicados para garantizar la calidad de los datos.

### Transformación

Se creó una nueva columna llamada `año_accidente` para facilitar el análisis temporal.

### Análisis exploratorio (EDA)

Se realizaron consultas como:

* Filtrado de vehículos antiguos
* Conteo por tipo de vehículo
* Análisis de gravedad
* Ranking de marcas

## Resultados

Se identificó que:

* Las motocicletas son el tipo de vehículo con más accidentes
* La mayoría de los accidentes son con heridos
* Algunas marcas aparecen con mayor frecuencia

## Conclusión

El uso de Spark permitió procesar eficientemente el dataset y obtener información relevante para el análisis de accidentes de tránsito.
