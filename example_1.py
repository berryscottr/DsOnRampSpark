from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Python Spark create RDD example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    df = spark.sparkContext.parallelize([(1, 2, 3, 'a b c'),
                                         (4, 5, 6, 'd e f'),
                                         (7, 8, 9, 'g h i')]) \
        .toDF(['col1', 'col2', 'col3', 'col4'])

    df.show()
