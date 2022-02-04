from pyspark.sql.functions import count, row_number
from pyspark.sql.window import Window


def rank_by_age_over_gender(df):
    w = Window.partitionBy('gender').orderBy('age')
    return df.withColumn('age_rank', row_number().over(w))


def count_by_x_over_gender(df, field):
    w = Window.partitionBy('gender', field)
    return df.withColumn(f'{field}_count', count('name').over(w))
