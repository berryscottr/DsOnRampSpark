from pyspark.sql import SparkSession
from pyspark.sql import functions as f

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName("WordCount")
             .getOrCreate())
    sc = spark.sparkContext

    text_file = sc.textFile("assignment_2_datafile.txt")
    counts = text_file.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda x, y: x + y)

    output = counts.collect()
    for (word, count) in output:
        if len(word) >= 3:
            print("%s: %i" % (word, count))

    sc.stop()
    spark.stop()



    # df = spark.createDataFrame(rows)
    # df.show()
    #
    # df2 = df.withColumn("text_left_over", f.expr("filter(text, x -> not(length(x) < 3))"))
    # df2.show()
    #
    # # This is the actual piece of logic you are looking for.
    # df3 = df.withColumn("text_left_over", f.expr("filter(text, x -> not(length(x) < 3))")).where(f.size(f.col("text_left_over")) > 0).drop("text")
    # df3.show()from pyspark.sql import SparkSession
    # from pyspark.sql import functions as f
    #
    # if __name__ == '__main__':
    #     spark = (SparkSession
    #              .builder
    #              .appName("WordCount")
    #              .getOrCreate())
    #     sc = spark.sparkContext
    #
    #     text_file = sc.textFile("assignment_2_datafile.txt")
    #     counts = text_file.flatMap(lambda line: line.split(" ")) \
    #         .map(lambda word: (word, 1)) \
    #         .reduceByKey(lambda x, y: x + y)
    #
    #     output = counts.collect()
    #     for (word, count) in output:
    #         if len(word) >= 3:
    #             print("%s: %i" % (word, count))
    #
    #     sc.stop()
    #     spark.stop()
    #
    #
    #
    #     # df = spark.createDataFrame(rows)
    #     # df.show()
    #     #
    #     # df2 = df.withColumn("text_left_over", f.expr("filter(text, x -> not(length(x) < 3))"))
    #     # df2.show()
    #     #
    #     # # This is the actual piece of logic you are looking for.
    #     # df3 = df.withColumn("text_left_over", f.expr("filter(text, x -> not(length(x) < 3))")).where(f.size(f.col("text_left_over")) > 0).drop("text")
    #     # df3.show()