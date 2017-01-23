# -*- coding: utf-8 -*-
# __author__ = 'mxins@qq.com'
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as f
from pyspark.sql.types import *
to_int = f.udf(lambda game_id: int(game_id) + 1, IntegerType())
spark = SparkSession\
    .builder\
    .appName("Python Spark SQL data source example")\
    .getOrCreate()
path = 'game_name_dim'
fields = [StructField('game_id', IntegerType(), True), StructField('gamedb', StringType(), True),
          StructField('game_name', StringType(), True), StructField('versions', StringType(), True),
          StructField('timezone_diff', StringType(), True), StructField('is_enable', StringType(), True),
          StructField('platform', StringType(), True), StructField('orderid', StringType(), True),
          StructField('terminal_type', StringType(), True)]
schema = StructType(fields)
game = spark.read.csv(path, schema)
game.createOrReplaceTempView('tb_game')
print(game.take(1))
spark.sql('SELECT game_id, COUNT(*) FROM tb_game GROUP BY game_id').show()
sql = "SELECT game_id, gamedb, game_name, versions, timezone_diff, platform, " \
      "first_value(game_id) OVER (PARTITION BY game_name, timezone_diff " \
      "ORDER BY game_id DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) " \
      "first_game_id FROM tb_game WHERE game_name='DefenseofLegends'"
rdf = spark.sql(sql)
rdf.show()

sql = "SELECT game_id, gamedb, game_name, versions, timezone_diff, platform, " \
      "first_value(game_id) OVER (PARTITION BY game_name , timezone_diff " \
      "ORDER BY game_id DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) " \
      "first_game_id, to_int(game_id) g_1 FROM tb_game WHERE game_name='DefenseofLegends'"
sc = spark.sparkContext
ctx = SQLContext.getOrCreate(sc)
ctx.registerFunction('to_int', lambda game_id: int(game_id) + 1, IntegerType())
rdf = spark.sql(sql)
rdf.show()