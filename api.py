from flask import Flask, jsonify, request
from flask_cors import CORS
from numpy import record
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import month
import pandas
import pyspark.pandas as ps
import requests

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


app = Flask(__name__)
CORS(app)

@app.route('/kpi1')
def kpi1():
    kpi1 = spark.sql("SELECT SECTOR, SUM(NUM_OP) AS NUMERO_OPERACIONES, CP_CLIENTE, CP_COMERCIO FROM bbdd GROUP BY SECTOR, CP_CLIENTE, CP_COMERCIO ORDER BY SUM(NUM_OP) DESC")
    data = ps.DataFrame(kpi1).to_json(orient='records')
    return jsonify(data)

@app.route('/kpi2')
def kpi2():
    kpi2 = spark.sql("SELECT FRANJA_HORARIA, SUM(NUM_OP) AS NUMERO_OPERACIONES, CP_CLIENTE, CP_COMERCIO, SECTOR FROM bbdd GROUP BY FRANJA_HORARIA, CP_CLIENTE, CP_COMERCIO, SECTOR ORDER BY FRANJA_HORARIA ASC")
    data = ps.DataFrame(kpi2)
    return jsonify(data.to_json(orient='records'))

@app.route('/kpi3')
def kpi3():
    kpi3 = spark.sql("SELECT CP_CLIENTE, SUM(NUM_OP) AS NUMERO_OPERACIONES FROM bbdd GROUP BY CP_CLIENTE ORDER BY SUM(NUM_OP) DESC")
    data = ps.DataFrame(kpi3)
    return jsonify(data.to_json(orient='records'))

@app.route('/kpi4')
def kpi4():
    kpi4 = spark.sql("SELECT SECTOR, SUM(NUM_OP) AS NUMERO_OPERACIONES, CP_CLIENTE FROM bbdd GROUP BY CP_CLIENTE, SECTOR ORDER BY SUM(NUM_OP) DESC")
    data = ps.DataFrame(kpi4)
    return jsonify(data.to_json(orient='records'))

@app.route('/kpi5')
def kpi5():
    kpi5 = spark.sql("SELECT sum(IMPORTE) AS TOTAL, SECTOR, CP_CLIENTE, CP_COMERCIO FROM bbdd GROUP BY SECTOR, CP_CLIENTE, CP_COMERCIO ORDER BY sum(IMPORTE) ASC")
    data = ps.DataFrame(kpi5)
    return jsonify(data.to_json(orient='records'))

@app.route('/kpi6')
def kpi6():
    kpi6 = spark.sql("SELECT sum(IMPORTE) AS TOTAL, SECTOR, month(DIA) AS MESES, CP_CLIENTE, CP_COMERCIO FROM bbdd GROUP BY SECTOR, CP_CLIENTE, CP_COMERCIO, month(DIA) ORDER BY sum(IMPORTE) DESC")
    data = ps.DataFrame(kpi6)
    data['MESES'] = data['MESES'].replace(['1'],'Enero')
    data['MESES'] = data['MESES'].replace(['2'],'Febrero')
    data['MESES'] = data['MESES'].replace(['3'],'Marzo')
    data['MESES'] = data['MESES'].replace(['4'],'Abril')
    data['MESES'] = data['MESES'].replace(['5'],'Mayo')
    data['MESES'] = data['MESES'].replace(['6'],'Junio')
    data['MESES'] = data['MESES'].replace(['7'],'Julio')
    data['MESES'] = data['MESES'].replace(['8'],'Agosto')
    data['MESES'] = data['MESES'].replace(['9'],'Septiembre')
    data['MESES'] = data['MESES'].replace(['10'],'Octubre')
    data['MESES'] = data['MESES'].replace(['11'],'Noviembre')
    data['MESES'] = data['MESES'].replace(['12'],'Diciembre')

    return jsonify(data.to_json(orient='records'))

@app.route('/kpi7', methods=['GET'])
def kpi7():
    horas = request.args.get('horas')
    kpi7 = spark.sql("SELECT FRANJA_HORARIA, SUM(NUM_OP) AS NUMERO_OPERACIONES, SECTOR, CP_CLIENTE, CP_COMERCIO FROM bbdd WHERE FRANJA_HORARIA = '{}' GROUP BY FRANJA_HORARIA, CP_CLIENTE, CP_COMERCIO, SECTOR".format(horas))
    data = ps.DataFrame(kpi7)
    return jsonify(data.to_json(orient='records'))

@app.route('/kpi8', methods=['GET'])
def kpi8():
    horas = request.args.get('horas')
    kpi8 = spark.sql("SELECT FRANJA_HORARIA, sum(IMPORTE) AS TOTAL, SECTOR, month(DIA) AS MESES, CP_CLIENTE, CP_COMERCIO FROM bbdd WHERE FRANJA_HORARIA = '{}' GROUP BY FRANJA_HORARIA, CP_CLIENTE, CP_COMERCIO, SECTOR, month(DIA)".format(horas))
    data = ps.DataFrame(kpi8)
    data['MESES'] = data['MESES'].replace(['1'],'Enero')
    data['MESES'] = data['MESES'].replace(['2'],'Febrero')
    data['MESES'] = data['MESES'].replace(['3'],'Marzo')
    data['MESES'] = data['MESES'].replace(['4'],'Abril')
    data['MESES'] = data['MESES'].replace(['5'],'Mayo')
    data['MESES'] = data['MESES'].replace(['6'],'Junio')
    data['MESES'] = data['MESES'].replace(['7'],'Julio')
    data['MESES'] = data['MESES'].replace(['8'],'Agosto')
    data['MESES'] = data['MESES'].replace(['9'],'Septiembre')
    data['MESES'] = data['MESES'].replace(['10'],'Octubre')
    data['MESES'] = data['MESES'].replace(['11'],'Noviembre')
    data['MESES'] = data['MESES'].replace(['12'],'Diciembre')
    return jsonify(data.to_json(orient='records'))

@app.route('/kpi9')
def kpi9():
    kpi9 = spark.sql("SELECT CP_CLIENTE, SUM(NUM_OP) AS NUMERO_OPERACIONES, CP_COMERCIO, SECTOR FROM bbdd GROUP BY CP_CLIENTE, CP_COMERCIO, SECTOR ORDER BY SUM(NUM_OP) DESC")
    data = ps.DataFrame(kpi9)
    return jsonify(data.to_json(orient='records'))

@app.route('/kpi10', methods=['GET'])
def kpi10():
    cp = request.args.get('cp')
    kpi10 = spark.sql("SELECT CP_CLIENTE, CP_COMERCIO, SECTOR, SUM(NUM_OP) AS NUMERO_OPERACIONES FROM bbdd WHERE CP_CLIENTE = '{}' GROUP BY CP_CLIENTE, CP_COMERCIO, SECTOR".format(cp))
    data = ps.DataFrame(kpi10).to_json(orient='records')
    return jsonify(data)

@app.route('/kpi11')
def kpi11():
    kpi11 = spark.sql("SELECT sum(bbdd.IMPORTE) AS TOTAL, bbdd.SECTOR, month(bbdd.DIA) AS MESES, bbdd.CP_COMERCIO, avg(bbdd2.TMed) AS TEMPERATURA FROM bbdd INNER JOIN bbdd2 ON month(bbdd.DIA) = month(bbdd2.FECHA) GROUP BY SECTOR,CP_COMERCIO, month(bbdd.DIA)")
    data = ps.DataFrame(kpi11)
    data['MESES'] = data['MESES'].replace(['1'],'Enero')
    data['MESES'] = data['MESES'].replace(['2'],'Febrero')
    data['MESES'] = data['MESES'].replace(['3'],'Marzo')
    data['MESES'] = data['MESES'].replace(['4'],'Abril')
    data['MESES'] = data['MESES'].replace(['5'],'Mayo')
    data['MESES'] = data['MESES'].replace(['6'],'Junio')
    data['MESES'] = data['MESES'].replace(['7'],'Julio')
    data['MESES'] = data['MESES'].replace(['8'],'Agosto')
    data['MESES'] = data['MESES'].replace(['9'],'Septiembre')
    data['MESES'] = data['MESES'].replace(['10'],'Octubre')
    data['MESES'] = data['MESES'].replace(['11'],'Noviembre')
    data['MESES'] = data['MESES'].replace(['12'],'Diciembre')
    return jsonify(data.to_json(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)