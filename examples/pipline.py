import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import examples.transformations as transformations
from pyspark_chainer import Chain

os.environ['JAVA_HOME'] = os.environ.get('JAVA_HOME') \
    or '/usr/local/opt/openjdk@8'
os.environ['JAVA_OPTS'] = '-Dio.netty.tryReflectionSetAccessible=true'

spark = SparkSession.builder \
    .master('local[2]') \
    .appName('pyspark-pipline-example') \
    .getOrCreate()

data = [dict(name='Tony', age=22, gender='male', pet='dog'),
        dict(name='Ashley', age=23, gender='female', pet='cat'),
        dict(name='Anna', age=28, gender='female', pet='dog'),
        dict(name='Cristin', age=21, gender='female', pet='cat'),
        dict(name='Andrew', age=25, gender='male', pet='dog'),
        dict(name='Jhon', age=22, gender='male', pet='cat'),
        dict(name='Stan', age=27, gender='male', pet='dog'),
        dict(name='Lory', age=22, gender='female', pet='dog'),
        dict(name='Kate', age=22, gender='female', pet='cat'),
        dict(name='Jane', age=25, gender='female', pet='cat'),
        dict(name='Adrian', age=27, gender='male', pet='dog'),
        dict(name='Kris', age=25, gender='male', pet='cat')]


df = spark.sparkContext.parallelize(data).toDF()

# TODO make more complex example
Chain(df, transformations) \
    .select('*') \
    .rank_by_age_over_gender() \
    .count_by_x_over_gender('age') \
    .count_by_x_over_gender('pet') \
    .where(col('age_rank') <= 4) \
    .show()
