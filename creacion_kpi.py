## Con este script analizamos los datos y creamos los KPIs

# Imports
import os
os.system("pip install pyspark")  # Orden del terminal para comprobar que tenemos instalado pyspark 
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

# SPARK
spark=SparkSession.builder.appName('Pyspark').getOrCreate()
spark

# Lectura de datos con SPARK y tabla con los datos de Cards
cards = spark.read.load("datos\cards.csv",format="csv", sep="|", inferSchema="true", header="true")
cards.show()

# Conversión de datos del CSV a Integer
cards = cards.withColumn("IMPORTE", cards["IMPORTE"].cast(IntegerType()))
data_df = cards.withColumn("NUM_OP", cards["NUM_OP"].cast(IntegerType()))

# Muestra tablas con filtros propios (métodos filter o withColumn)
cards.filter(cards.IMPORTE * cards.NUM_OP > 710).show()
cards.withColumn('TOTAL', (cards.IMPORTE * cards.NUM_OP)).show()

# Agrupa por CP de los Clientes. No muestra resultado.
cards.groupBy('CP_CLIENTE')

# Vista del DataFrame para las consultas de los KPIs
cards.createTempView("bbdd")
cards.show()

# Muestra la lista de sectores
sectores = spark.sql("SELECT SECTOR FROM bbdd")
sectores.show()

# Consulta posiblemente recurrente
# spark.sql("SELECT SECTOR FROM bbdd GROUP BY SECTOR")

# KPI 1
# Lista de sectores de más vendidos a menos
kpi1 = spark.sql("SELECT SECTOR, SUM(NUM_OP) FROM bbdd GROUP BY SECTOR ORDER BY SUM(NUM_OP) DESC")
kpi1.show()