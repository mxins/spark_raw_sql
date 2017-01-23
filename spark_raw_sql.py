# -*- coding: utf-8 -*-
# __author__ = 'mxins@qq.com'
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as f
from pyspark.sql import Window
from pyspark.sql.types import *
import sys
'''
spark-2.0.1-bin-hadoop2.7/bin/spark-submit spark_raw_sql.py
'''
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


def spark_sql_raw():
    print(game.count())
    game.createOrReplaceTempView('tb_game')
    sql = "SELECT game_id, gamedb, game_name, versions, timezone_diff, platform, " \
          "first_value(game_id) OVER (PARTITION BY game_name, timezone_diff " \
          "ORDER BY game_id DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) " \
          "first_game_id, add_1(game_id) game_1 FROM tb_game WHERE game_name='DefenseofLegends'"
    sc = spark.sparkContext
    ctx = SQLContext.getOrCreate(sc)
    ctx.registerFunction('add_1', lambda game_id: game_id + 1, IntegerType())
    rdf = spark.sql(sql)
    return rdf


def spark_sql():
    print(game.take(2))
    add_1 = f.udf(lambda game_id: game_id + 1, IntegerType())
    window = Window.partitionBy('game_name', 'timezone_diff')\
        .orderBy(game['game_id'].desc())\
        .rowsBetween(-sys.maxsize, sys.maxsize)
    rdf = game.filter(game.game_name == 'DefenseofLegends')\
        .select('game_id', 'gamedb', 'game_name', 'versions', 'timezone_diff', 'platform',
                f.first('game_id').over(window).alias('first_game_id'),
                add_1('game_id').alias('game_1'))
    return rdf

df1 = spark_sql_raw()
df1.show()
df2 = spark_sql()
df2.show()
