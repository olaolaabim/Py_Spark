from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
from pyspark.sql import Row
import json
import pyspark
import requests
from json import dumps
import time
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import Row
from pyspark import HiveContext

spark = SparkSession.builder\
        .config("spark.jars", "C:\\Program Files (x86)\\MySQL\\Connector_J_8.0\\mysql-connector-j-8.0.31.jar") \
        .master("local") \
        .appName("Stock") \
        .enableHiveSupport() \
        .getOrCreate()


stocksDF = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/stocks") \
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("dbtable", "stock_symbs") \
    .option("user", "root")\
    .option("password", "root").load()

new_data = stocksDF

ls = ['AAPL', 'AMZN', 'BABA', 'GOOG', 'MSFT']
response_data = requests.get('http://api.marketstack.com/v1/intraday?access_key=8329ee4b4ca94e0cb45377b1cc1843e5&symbols=MSFT,AAPL,BABA,AMZN')
data = response_data.text
data = json.loads(data)
data_api = data['data']
print(data_api)
#data_api = [{'open': 256.95, 'high': 256.95, 'low': 256.95, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-14T02:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 255.935, 'high': 255.935, 'low': 255.125, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-14T01:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 255.635, 'high': 255.7, 'low': 255.35, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-14T00:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 254.62, 'high': 255.075, 'low': 254.53, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-13T23:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 257.955, 'high': 257.985, 'low': 257.77, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-13T22:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 259.97, 'high': 260.315, 'low': 259.93, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-13T21:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 255.59, 'high': 263.91, 'low': 253.155, 'last': 255.385, 'close': 252.51, 'volume': 536711.0, 'date': '2022-12-13T20:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 255.59, 'high': 263.91, 'low': 253.155, 'last': 255.46, 'close': 252.51, 'volume': 487905.0, 'date': '2022-12-13T19:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 255.59, 'high': 263.91, 'low': 253.155, 'last': 254.62, 'close': 252.51, 'volume': 443473.0, 'date': '2022-12-13T18:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 255.59, 'high': 263.91, 'low': 255.12, 'last': 259.97, 'close': 252.51, 'volume': 250071.0, 'date': '2022-12-13T16:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 255.59, 'high': 255.61, 'low': 255.59, 'last': 255.61, 'close': 252.51, 'volume': 44.0, 'date': '2022-12-13T13:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 252.52, 'high': 252.52, 'low': 252.52, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-13T02:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 251.075, 'high': 251.195, 'low': 251.03, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-13T01:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 250.375, 'high': 250.51, 'low': 250.37, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-13T00:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 250.215, 'high': 250.615, 'low': 250.165, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-12T23:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 250.35, 'high': 250.38, 'low': 250.08, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-12T22:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 249.765, 'high': 250.035, 'low': 249.765, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-12T21:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 246.74, 'high': 252.1, 'low': 246.02, 'last': 251.09, 'close': 245.42, 'volume': 421282.0, 'date': '2022-12-12T20:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 246.74, 'high': 252.1, 'low': 246.02, 'last': 250.5, 'close': 245.42, 'volume': 378621.0, 'date': '2022-12-12T19:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 246.74, 'high': 252.1, 'low': 246.02, 'last': 250.26, 'close': 245.42, 'volume': 296452.0, 'date': '2022-12-12T18:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 246.74, 'high': 252.1, 'low': 246.02, 'last': 249.765, 'close': 245.42, 'volume': 191247.0, 'date': '2022-12-12T16:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 246.74, 'high': 252.1, 'low': 246.02, 'last': 251.22, 'close': 245.42, 'volume': 107506.0, 'date': '2022-12-12T15:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 245.39, 'high': 245.39, 'low': 245.39, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-10T02:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 246.87, 'high': 246.945, 'low': 246.73, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-10T01:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 246.965, 'high': 247.02, 'low': 246.825, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-10T00:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 247.375, 'high': 247.375, 'low': 247.18, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-09T23:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 247.43, 'high': 247.49, 'low': 247.355, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-09T22:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 247.845, 'high': 247.845, 'low': 247.505, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-09T21:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 248.69, 'high': 248.77, 'low': 244.21, 'last': 246.78, 'close': 247.4, 'volume': 239494.0, 'date': '2022-12-09T20:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 248.69, 'high': 248.77, 'low': 244.21, 'last': 247.34, 'close': 247.4, 'volume': 186086.0, 'date': '2022-12-09T18:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 248.69, 'high': 248.77, 'low': 244.21, 'last': 247.84, 'close': 247.4, 'volume': 102517.0, 'date': '2022-12-09T16:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 248.69, 'high': 248.77, 'low': 244.21, 'last': 245.86, 'close': 247.4, 'volume': 38884.0, 'date': '2022-12-09T15:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 247.43, 'high': 247.43, 'low': 247.43, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-09T02:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 246.8, 'high': 246.815, 'low': 246.55, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-09T01:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 248.165, 'high': 248.165, 'low': 247.94, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-09T00:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 247.54, 'high': 247.635, 'low': 247.47, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-08T23:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 247.295, 'high': 247.295, 'low': 247.035, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-08T22:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 246.945, 'high': 247.17, 'low': 246.905, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-08T21:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 244.7, 'high': 248.73, 'low': 243.065, 'last': 246.8, 'close': 244.37, 'volume': 297311.0, 'date': '2022-12-08T20:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 244.7, 'high': 248.73, 'low': 243.065, 'last': 248.09, 'close': 244.37, 'volume': 254001.0, 'date': '2022-12-08T19:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 244.7, 'high': 247.745, 'low': 243.065, 'last': 247.57, 'close': 244.37, 'volume': 203745.0, 'date': '2022-12-08T18:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 244.7, 'high': 246.07, 'low': 243.065, 'last': 246.07, 'close': 244.37, 'volume': 59396.0, 'date': '2022-12-08T15:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 244.315, 'high': 244.315, 'low': 244.315, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-08T02:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 244.25, 'high': 244.37, 'low': 244.18, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-08T01:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 244.695, 'high': 244.74, 'low': 244.67, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-08T00:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 245.04, 'high': 245.235, 'low': 244.99, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-07T23:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 244.14, 'high': 244.165, 'low': 243.995, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-07T22:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 245.695, 'high': 245.995, 'low': 245.65, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-07T21:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 242.54, 'high': 246.15, 'low': 242.205, 'last': 244.25, 'close': 245.12, 'volume': 262929.0, 'date': '2022-12-07T20:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 242.54, 'high': 246.15, 'low': 242.205, 'last': 244.67, 'close': 245.12, 'volume': 220990.0, 'date': '2022-12-07T19:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 242.54, 'high': 246.15, 'low': 242.205, 'last': 245.125, 'close': 245.12, 'volume': 196518.0, 'date': '2022-12-07T18:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 242.54, 'high': 245.875, 'low': 242.205, 'last': 245.72, 'close': 245.12, 'volume': 105284.0, 'date': '2022-12-07T16:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 242.54, 'high': 245.63, 'low': 242.52, 'last': 242.7, 'close': 245.12, 'volume': 42121.0, 'date': '2022-12-07T15:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 245.1, 'high': 245.1, 'low': 245.1, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-07T02:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 244.445, 'high': 244.815, 'low': 244.43, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-07T01:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 245.305, 'high': 245.34, 'low': 245.19, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-07T00:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 245.35, 'high': 245.65, 'low': 245.265, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-06T23:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 246.52, 'high': 246.56, 'low': 246.405, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-06T22:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 247.115, 'high': 247.21, 'low': 246.94, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-06T21:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 250.22, 'high': 251.805, 'low': 243.975, 'last': 244.47, 'close': 250.2, 'volume': 339283.0, 'date': '2022-12-06T20:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 250.22, 'high': 251.805, 'low': 244.415, 'last': 245.28, 'close': 250.2, 'volume': 295246.0, 'date': '2022-12-06T19:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 250.22, 'high': 251.805, 'low': 246.24, 'last': 246.46, 'close': 250.2, 'volume': 179969.0, 'date': '2022-12-06T17:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 250.22, 'high': 251.805, 'low': 246.41, 'last': 247.115, 'close': 250.2, 'volume': 117916.0, 'date': '2022-12-06T16:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 250.22, 'high': 251.805, 'low': 248.16, 'last': 248.16, 'close': 250.2, 'volume': 66538.0, 'date': '2022-12-06T15:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 250.22, 'high': 250.99, 'low': 250.09, 'last': 250.99, 'close': 250.2, 'volume': 1215.0, 'date': '2022-12-06T14:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 250.28, 'high': 250.28, 'low': 250.28, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-06T02:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 248.735, 'high': 248.965, 'low': 248.715, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-06T01:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 248.705, 'high': 248.755, 'low': 248.46, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-06T00:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 249.185, 'high': 249.32, 'low': 249.115, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-05T23:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 250.83, 'high': 250.96, 'low': 250.82, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-05T22:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 252.755, 'high': 252.92, 'low': 252.74, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-05T21:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 253.78, 'high': 253.98, 'low': 248.065, 'last': 248.83, 'close': 255.02, 'volume': 353659.0, 'date': '2022-12-05T20:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 253.78, 'high': 253.98, 'low': 248.675, 'last': 248.685, 'close': 255.02, 'volume': 294130.0, 'date': '2022-12-05T19:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 253.78, 'high': 253.98, 'low': 249.15, 'last': 249.195, 'close': 255.02, 'volume': 234878.0, 'date': '2022-12-05T18:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 253.78, 'high': 253.98, 'low': 250.47, 'last': 250.86, 'close': 255.02, 'volume': 145528.0, 'date': '2022-12-05T17:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 253.78, 'high': 253.98, 'low': 251.22, 'last': 252.85, 'close': 255.02, 'volume': 96833.0, 'date': '2022-12-05T16:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 253.78, 'high': 253.98, 'low': 251.56, 'last': 251.56, 'close': 255.02, 'volume': 50321.0, 'date': '2022-12-05T15:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 255.05, 'high': 255.05, 'low': 255.05, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-03T02:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 254.4, 'high': 254.545, 'low': 254.26, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-03T01:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 253.455, 'high': 253.46, 'low': 253.34, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-03T00:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 253.02, 'high': 253.25, 'low': 252.83, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-02T23:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 252.84, 'high': 252.84, 'low': 252.635, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-02T22:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 251.62, 'high': 251.78, 'low': 251.135, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-02T21:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 254.24, 'high': 254.52, 'low': 248.37, 'last': 254.42, 'close': 254.69, 'volume': 348916.0, 'date': '2022-12-02T20:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 254.24, 'high': 254.24, 'low': 248.37, 'last': 252.93, 'close': 254.69, 'volume': 263214.0, 'date': '2022-12-02T18:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 254.24, 'high': 254.24, 'low': 248.37, 'last': 252.82, 'close': 254.69, 'volume': 212146.0, 'date': '2022-12-02T17:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 254.24, 'high': 254.24, 'low': 248.37, 'last': 251.62, 'close': 254.69, 'volume': 148449.0, 'date': '2022-12-02T16:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 254.24, 'high': 254.24, 'low': 248.37, 'last': 250.6, 'close': 254.69, 'volume': 66468.0, 'date': '2022-12-02T15:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 254.735, 'high': 254.735, 'low': 254.735, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-02T02:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 255.835, 'high': 255.845, 'low': 255.72, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-02T01:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 254.42, 'high': 254.5, 'low': 254.28, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-02T00:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 254.31, 'high': 254.49, 'low': 254.31, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-01T23:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 253.19, 'high': 253.22, 'low': 253.005, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-01T22:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 253.975, 'high': 254.08, 'low': 253.745, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-01T21:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 255.05, 'high': 255.59, 'low': 253.92, 'last': None, 'close': None, 'volume': None, 'date': '2022-12-01T20:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 254.13, 'high': 256.11, 'low': 250.92, 'last': 254.43, 'close': 255.14, 'volume': 396186.0, 'date': '2022-12-01T19:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 254.13, 'high': 256.11, 'low': 250.92, 'last': 254.335, 'close': 255.14, 'volume': 338123.0, 'date': '2022-12-01T18:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 254.13, 'high': 256.11, 'low': 250.92, 'last': 253.19, 'close': 255.14, 'volume': 286792.0, 'date': '2022-12-01T17:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 254.13, 'high': 256.11, 'low': 250.92, 'last': 254.07, 'close': 255.14, 'volume': 243388.0, 'date': '2022-12-01T16:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}, {'open': 254.13, 'high': 256.11, 'low': 253.295, 'last': 255.29, 'close': 255.14, 'volume': 109178.0, 'date': '2022-12-01T15:00:00+0000', 'symbol': 'MSFT', 'exchange': 'IEXG'}]
########cleaning the api##########
df_api= spark.createDataFrame(data_api)
split_col = split(df_api.date, 'T',)
data_api_df1 = df_api.withColumn("new_date", split_col.getItem(0))\
            .withColumn("new_time", split_col.getItem(1))\
            .drop("date")
#Cleaning the date column
from pyspark.sql.functions import substring, length, col, expr, lit
#cleaning the from pyspark.sql.functions import substring, length, col, expr
data_api_df2 = data_api_df1.withColumn("new_time",expr("substring(new_time, 1, 8)"))
d = data_api_df2.withColumn("dividend", lit(0.0))
d2= d.withColumn("split_factor", lit(1.0))
d3=d2.drop(col("last"))
data_api_df2= d3.na.fill(value=0)
#data_api_df2= data_api_df2.select(["open, high, low, close, volume, split_factor, dividend, symbol, exchange,  new_date, new_time"])
data_api_df2.show()


###########cleaning the db data#########
split_col = split(new_data.date, 'T',)
data_df3 = new_data.withColumn("new_date", split_col.getItem(0))\
    .withColumn("new_time", split_col.getItem(1))\
    .drop("date")
#Cleaning the date column
from pyspark.sql.functions import substring, length, col, expr
#cleaning the from pyspark.sql.functions import substring, length, col, expr
stock_db_df = data_df3.withColumn("new_time",expr("substring(new_time, 1, 8)"))


stock_df_api = data_api_df2.limit(20)
stock_df_db = stock_db_df

stock_df_api.show()
stock_df_db.show()
# Create a Database stocks
spark.sql("CREATE DATABASE IF NOT EXISTS stocks")

# Create a Table naming as sampleTable under stocks database.
spark.sql("CREATE TABLE IF NOT EXISTS stocks.prices (item_id Int, open Double, high Double, low Double, close Double, volume Double, split_factor Double, dividend Double, symbol String, exchange String,  new_date Varchar(1024), new_time Varchar(1024))")
stock_df_db.createOrReplaceTempView("eod_prices")

#create api table
spark.sql("CREATE TABLE IF NOT EXISTS stocks.apidata (open Double, exchange String, high Double, low Double, close Double, symbol String, volume Double, new_date Varchar(1024), new_time Varchar(1024), dividend Double, split_factor Double)")
data_api_df2.createOrReplaceTempView("apidata")

# Insert into sampleTable using the sampleView.
spark.sql("INSERT INTO TABLE stocks.prices  SELECT * FROM eod_prices")
#insert api data
spark.sql("INSERT INTO TABLE stocks.apidata SELECT * FROM apidata")
print('Hello')

#real processing
open_real_data = spark.sql("(Select symbol, new_date, open from ((Select open, high, low, close, volume, split_factor, dividend, symbol, exchange,  new_date, new_time from stocks.prices) UNION (select open, high, low, close, volume, split_factor, dividend, symbol, exchange,  new_date, new_time from stocks.apidata)) order by new_date DESC)")
open_real_data = open_real_data.toPandas()

real_volume = spark.sql("select symbol, volume, new_date from stocks.apidata ")
real_volume = real_volume.toPandas()

#Batch Processing
import pandas as pd
#The company that paid dividend in the last one year
pd = spark.sql("select symbol, MAX(dividend) as dividend from stocks.prices WHERE dividend > 0 Group by symbol" )
paid_dividend = pd.toPandas()

#The company that paid dividend more 1 dollar in the last one year
dividend_one = spark.sql("select symbol, MAX(dividend) as dividend from stocks.prices WHERE dividend > 1 GROUP by symbol " )
dividend_one = dividend_one.toPandas()

#which company has the highest trade volume average within the 1 year period
volume_one = spark.sql("select symbol, MAX(ds.avg_volume) as max_volume from (select symbol, AVG(volume) as avg_volume from stocks.prices GROUP BY symbol) as ds GROUP BY symbol ORDER BY max_volume DESC" )
volume_one = volume_one.toPandas()

import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from plotly.subplots import make_subplots
# 7 rows

fig = make_subplots(rows=3, cols=1, subplot_titles=('Companies that paid dividend', 'Companies that paid more than 1 dollar', 'highest volume average'))

fig1 = px.histogram( paid_dividend,  x ='symbol', y= 'dividend', title = 'companies that paid more than 1 dollar')
fig2 = px.line(dividend_one, x='symbol' , y = 'dividend', title='did greater than 1 dollar')
fig3 = px.histogram(volume_one, x = 'symbol', y= 'max_volume', title= 'highest volume average')




fig.add_trace(fig1['data'][0], row=1, col=1)
fig.add_trace(fig2['data'][0], row=2, col=1)
fig.add_trace(fig3['data'][0], row=3, col=1)

fig['layout']['xaxis']['title']='symbol'
fig['layout']['yaxis']['title']='dividend'
fig['layout']['xaxis2']['title']='symbol'
fig['layout']['yaxis2']['title']='dividend'
fig['layout']['xaxis3']['title']='symbol'
fig['layout']['yaxis3']['title']='max_volume'
fig.show()




#real data
fig = px.line(open_real_data, x = 'new_date', y = 'open', color= 'symbol', title='Change in Stock Prices (One Year)', labels={
                     "new_date": "date",
                     "open": "price (usd)"})
fig.show()

print(real_volume)

fig200 = px.histogram(real_volume, x = 'new_date', y = 'volume', color = 'symbol', title='Change in volume (daily)', labels={
                     "new_date": "date",
                     "volume": "unit"})
fig200.show()

#batch procesing
#The company that paid dividend in the last one year
#fig1 = px.histogram( paid_dividend,  x ='symbol', y= 'dividend', title = 'companies that paid more than 1 dollar')
#fig1.show()

#The company that paid dividend more 1 dollar in the last one year
#dividend_one = spark.sql("select symbol, MAX(dividend) as dividend from stocks.prices WHERE dividend > 1 GROUP by symbol " )
#dividend_one = dividend_one.toPandas()
#fig2 = px.line(dividend_one, x='symbol' , y = 'dividend', title='did greater than 1 dollar')
#fig2.show()

#which company has the highest trade volume average within the 1 year period
#volume_one = spark.sql("select symbol, MAX(ds.avg_volume) as max_volume from (select symbol, AVG(volume) as avg_volume from stocks.prices GROUP BY symbol) as ds GROUP BY symbol ORDER BY max_volume DESC" )
#volume_one = volume_one.toPandas()
#fig3 = px.histogram(volume_one, x = 'symbol', y= 'max_volume', title= 'highest volume average')
#fig3.show()



#which date did any company have the highest volume
date_volume = spark.sql("Select stocks.prices.symbol, stocks.prices.volume, stocks.prices.new_date from stocks.prices INNER JOIN (select symbol, MAX(volume) as max_volume from stocks.prices GROUP BY symbol) as ds ON stocks.prices.volume = ds.max_volume Limit 21")
date_volume = date_volume.toPandas()
fig4 = px.bar(date_volume, x ='new_date', y= 'volume', color='symbol', title='highest volume date' )
fig4.show()

#which exchange traded more companies?
ex_comp= spark.sql("select exchange, (count(DISTINCT(symbol))) as comp_count from stocks.prices GROUP BY exchange")
ex_comp = ex_comp.toPandas()
fig5 = px.pie(ex_comp, names= 'exchange' , values= 'comp_count', title = 'which exchange traded more companies')
fig5.show()

