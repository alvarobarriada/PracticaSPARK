import os
os.system("pip install pyspark")
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

spark=SparkSession.builder.appName('Pyspark').getOrCreate()

spark

print("TABLA DE CARDS\n")
cards = spark.read.load("datos\cards.csv",format="csv", sep="|", inferSchema="true", header="true")
cards.show()


cards = cards.withColumn("IMPORTE", cards["IMPORTE"].cast(IntegerType()))
data_df = cards.withColumn("NUM_OP", cards["NUM_OP"].cast(IntegerType()))


cards.filter(cards.IMPORTE * cards.NUM_OP > 710).show()

print("TABLA DE CARDS\n")
cards.withColumn('TOTAL', (cards.IMPORTE * cards.NUM_OP)).show()

cards.groupBy('CP_CLIENTE')

# Intento de consulta SQL

# Creo una copia del DataFrame
cards.createTempView("bbdd")

cards2 = spark.sql("SELECT SECTOR FROM bbdd")
cards2.show()

cards.show()

# KPI 1
## Lista de sectores (de más vendidos a menos)
# spark.sql("SELECT SECTOR FROM bbdd GROUP BY SECTOR")

kpi1 = spark.sql("SELECT SECTOR, SUM(NUM_OP) FROM bbdd GROUP BY SECTOR ORDER BY SUM(NUM_OP) DESC")
kpi1.show()