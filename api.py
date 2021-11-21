from flask import Flask, jsonify
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
import pandas
import pyspark.pandas as ps

spark=SparkSession.builder.appName('Pyspark').getOrCreate()

cards = spark.read.load("datos\cards.csv",format="csv", sep="|", inferSchema="true", header="true")
cards.schema
cards = cards.withColumn("IMPORTE", cards["IMPORTE"].cast(DoubleType()))
cards = cards.withColumn("NUM_OP", cards["NUM_OP"].cast(IntegerType()))
cards.createTempView("bbdd")




app = Flask(__name__)

@app.route('/ping')
def ping():
    return 'Pong!'

@app.route('/kpi1')
def kpi1():
    kpi1 = spark.sql("SELECT SECTOR, SUM(NUM_OP) FROM bbdd GROUP BY SECTOR ORDER BY SUM(NUM_OP) DESC")
    data = ps.DataFrame(kpi1)
    return jsonify(data.to_json(orient='records'))


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

if __name__ == '__main__':
    app.run(debug=True)