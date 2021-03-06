## Con este script analizamos los datos y creamos los KPIs

# Imports
import os
os.system("pip install pyspark")  # Orden del terminal para comprobar que tenemos instalado pyspark 
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import month

# SPARK
spark=SparkSession.builder.appName('Pyspark').getOrCreate()
spark

# Lectura de datos con SPARK y tabla con los datos de Cards
struct = StructType([ \
    StructField("CP_CLIENTE",StringType(),False), \
    StructField("CP_COMERCIO",StringType(),False), \
    StructField("SECTOR",StringType(),False), \
    StructField("DIA", DateType(), False), \
    StructField("FRANJA_HORARIA", StringType(), False), \
    StructField("IMPORTE", DoubleType(), False), \
    StructField("NUM_OP", IntegerType(), False) \
  ])

cards = spark.read.load("datos\cards.csv",format="csv", sep="|", schema=struct, header="true")
#cards.show()

struct2 = StructType([ \
    StructField("FECHA",DateType(),False), \
    StructField("DIA", IntegerType(),False), \
    StructField("TMax",DoubleType(),False), \
    StructField("HTMax", StringType(), False), \
    StructField("TMin", DoubleType(), False), \
    StructField("HTMin", StringType(), False), \
    StructField("TMed", DoubleType(), False), \
    StructField("HumMax", DoubleType(), False), \
    StructField("HumMin", DoubleType(), False), \
    StructField("HumMed", DoubleType(), False), \
    StructField("VelViento", DoubleType(), False), \
    StructField("DirViento", DoubleType(), False), \
    StructField("Rad", DoubleType(), False), \
    StructField("Precip", DoubleType(), False), \
    StructField("ETo", DoubleType(), False) \
])

weather = spark.read.load("datos\weather.csv",format="csv", sep=";", schema=struct2, header="true")
#weather.show()

weather.createTempView("bbdd2")

# Conversión de datos del CSV a Integer
#cards = cards.withColumn("IMPORTE", cards["IMPORTE"].cast(DoubleType()))
#data_df = cards.withColumn("NUM_OP", cards["NUM_OP"].cast(IntegerType()))

# Muestra tablas con filtros propios (métodos filter o withColumn)
#cards.filter(cards.IMPORTE * cards.NUM_OP > 710).show()
#cards.withColumn('TOTAL', (cards.IMPORTE * cards.NUM_OP)).show()

# Agrupa por CP de los Clientes. No muestra resultado.
cards.groupBy('CP_CLIENTE')

# Vista del DataFrame para las consultas de los KPIs
cards.createTempView("bbdd")
#cards.show()
'''
# Muestra la lista de sectores
sectores = spark.sql("SELECT SECTOR FROM bbdd")
#sectores.show()

# Consulta posiblemente recurrente
# spark.sql("SELECT SECTOR FROM bbdd GROUP BY SECTOR")

# KPI 1
# Lista de sectores de más vendidos a menos
kpi1 = spark.sql("SELECT SECTOR, SUM(NUM_OP) FROM bbdd GROUP BY SECTOR ORDER BY SUM(NUM_OP) DESC")
kpi1.show()

# KPI2
# Franja horaria en la que se realizan más operaciones
kpi2 = spark.sql("SELECT FRANJA_HORARIA, SUM(NUM_OP) FROM bbdd GROUP BY FRANJA_HORARIA ORDER BY FRANJA_HORARIA ASC")
kpi2.show()

# KPI3
# CP de clientes que compran más
kpi3 = spark.sql("SELECT CP_CLIENTE, SUM(NUM_OP) FROM bbdd GROUP BY CP_CLIENTE ORDER BY SUM(NUM_OP) DESC")
kpi3.show()

# KPI4
# CP de clientes que compran más por sector
kpi4 = spark.sql("SELECT SECTOR, SUM(NUM_OP), CP_CLIENTE FROM bbdd GROUP BY CP_CLIENTE, SECTOR ORDER BY SUM(NUM_OP) DESC")
kpi4.show()


kpi5 = spark.sql("SELECT sum(IMPORTE), SECTOR FROM bbdd GROUP BY SECTOR ORDER BY sum(IMPORTE) ASC")
kpi5.show()

kpi6 = spark.sql("SELECT sum(IMPORTE), SECTOR, month(DIA), DIA, CP_CLIENTE, CP_COMERCIO FROM bbdd GROUP BY SECTOR, CP_CLIENTE, CP_COMERCIO, month(DIA), DIA ORDER BY sum(IMPORTE) DESC")
kpi6.show()
'''

# Intento de KPI Doble
# Qué temperatura media había los días en que se hicieron más operaciones
kpi11 = spark.sql("SELECT bbdd.NUM_OP, bbdd.DIA, bbdd2.TMed FROM bbdd INNER JOIN bbdd2 ON bbdd.DIA = bbdd2.FECHA GROUP BY bbdd.DIA, NUM_OP, bbdd2.TMed ORDER BY NUM_OP DESC")
kpi11.show()