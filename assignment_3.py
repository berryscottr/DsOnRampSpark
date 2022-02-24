from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .enableHiveSupport()
             .appName("Assignment3")
             .getOrCreate())

    people_json = spark.read.json("people.json")
    people_json.printSchema()
    people_json.createOrReplaceTempView("people_json")
    people_json.show()
    distinct_names = spark.sql("SELECT DISTINCT(name) FROM people_json")
    distinct_names.show()

    people_csv = spark.read.csv("people.txt")
    people_csv.show()
