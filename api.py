from flask import Flask, jsonify
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas
import pyspark.pandas as ps

spark=SparkSession.builder.appName('Pyspark').getOrCreate()

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
cards.createTempView("bbdd")


app = Flask(__name__)

@app.route('/ping')
def ping():
    return 'Pong!'

@app.route('/kpi1')
def kpi1():
    kpi1 = spark.sql("SELECT SECTOR, SUM(NUM_OP) FROM bbdd GROUP BY SECTOR ORDER BY SUM(NUM_OP) DESC")
    data = ps.DataFrame(kpi1).to_json(orient='records')
    return jsonify(data)

@app.route('/kpi2')
def kpi2():
    kpi2 = spark.sql("SELECT FRANJA_HORARIA, SUM(NUM_OP) FROM bbdd GROUP BY FRANJA_HORARIA ORDER BY FRANJA_HORARIA ASC")
    data = ps.DataFrame(kpi2)
    return jsonify(data.to_json(orient='records'))

@app.route('/kpi3')
def kpi3():
    kpi3 = spark.sql("SELECT CP_CLIENTE, SUM(NUM_OP) FROM bbdd GROUP BY CP_CLIENTE ORDER BY SUM(NUM_OP) DESC")
    data = ps.DataFrame(kpi3)
    return jsonify(data.to_json(orient='records'))

@app.route('/kpi4')
def kpi4():
    kpi4 = spark.sql("SELECT SECTOR, SUM(NUM_OP), CP_CLIENTE FROM bbdd GROUP BY CP_CLIENTE, SECTOR ORDER BY SUM(NUM_OP) DESC")
    data = ps.DataFrame(kpi4)
    return jsonify(data.to_json(orient='records'))

@app.route('/kpi5')
def kpi5():
    kpi5 = spark.sql("SELECT sum(IMPORTE), SECTOR FROM bbdd GROUP BY SECTOR ORDER BY sum(IMPORTE) ASC")
    data = ps.DataFrame(kpi5)
    return jsonify(data.to_json(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)