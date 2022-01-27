from pyspark.sql import SparkSession
from pyspark.sql import Row

if __name__ == '__main__':
    # Create a SparkSession
    spark = (SparkSession
             .builder
             .appName("AuthorsStates")
             .getOrCreate())
    rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
    authors_df = spark.createDataFrame(rows, ["Authors", "State"])
    authors_df.show()
