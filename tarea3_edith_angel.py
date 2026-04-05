# Importación de librerías necesarias
from pyspark.sql import SparkSession, functions as F

# Creación de la sesión de Spark
spark = SparkSession.builder.appName('Tarea').getOrCreate()

# Ruta del archivo almacenado en HDFS
file_path = 'hdfs://localhost:9000/Tarea3EdithAngel/anem-hgsc.csv'

# Lectura del archivo CVS y creación del DataFrame
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

# Limpieza de datos
print("Limpieza de datos")
df = df.dropna()
df = df.dropDuplicates()

# Transformación de datos
print("Transformación de datos")
df = df.withColumn("anio_accidente", F.year(F.col("fecha_accidente")))

# Exploración de datos

# Visualizar la estructura del dataset
print("Esquema")
df.printSchema()

# Mostrar algunos registros para conocer el contenido
print("Primeros datos")
df.show(5)

# Obtener estadísticas generales del conjunto de datos
print("Estadísticas")
df.summary().show()

# Análisis de datos

# Vehículos con más de 10 años
print("Vehículos con edad mayor a 10 años\n")
vehiculos_antiguos = df.filter(F.col('edad_vehiculo') > 10).select('marca_vehiculo','tipo_vehiculo','edad_vehiculo')
vehiculos_antiguos.show()

# Vehículos ordenados por edad
print("Vehículos ordenados de mayor a menor edad\n")
sorted_df = df.sort(F.col("edad_vehiculo").desc()).select('marca_vehiculo','tipo_vehiculo','edad_vehiculo') 
sorted_df.show()

# Cantidad de accidentes por tipo de vehículo
print("Cantidad de accidentes por tipo de vehículo\n")
df.groupBy("tipo_vehiculo").count().orderBy("count", ascending=False).show()

# Cantidad de accidentes según la gravedad
print("Cantidad de accidentes por gravedad\n")
df.groupBy("gravedad_accidente").count().show()

# Marcas de vehículos más involucradas en accidentes
print("Accidentes por marca de vehículo\n")
df.groupBy("marca_vehiculo").count().orderBy("count", ascending=False).show(10)

# Finalización del programa
input("Presione Enter para finalizar y cerrar Spark...")
spark.stop()
