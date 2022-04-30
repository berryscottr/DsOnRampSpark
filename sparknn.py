from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, stddev


def normalize(df, column):
    # average = df.agg(mean(df[column]).alias("mean")).collect()[0]["mean"]
    # std_dev = df.agg(stddev(df[column]).alias("stddev")).collect()[0]["stddev"]
    # return df.select((df[column] - average) / std_dev)

    df[column] = (df[column] - df[column].mean()) / df[column].stddev()


if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .enableHiveSupport()
             .appName("Spark KNN Iris")
             .getOrCreate())

    # load data to RDD
    iris = spark.read.csv("iris.csv", header=False, inferSchema=True)
    # normalize data
    iris = iris.select(
        *[normalize(iris.withColumn(col_name, )) for col_name in iris.columns]
    )
    # split 60/20/20
    train_set, validate_set, test_set = iris.randomSplit([0.60, 0.20, 0.20])
    spark.stop()
