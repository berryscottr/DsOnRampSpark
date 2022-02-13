from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName("WordCount")
             .getOrCreate())
    sc = spark.sparkContext

    text_file = sc.textFile("assignment_2_datafile.txt")
    counts = text_file.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1) if len(word) > 2 else (word, 0)) \
        .reduceByKey(lambda x, y: x + y)

    output = counts.collect()
    for (word, count) in output:
        if len(word) >= 3:
            print("%s: %i" % (word, count))

    sc.stop()
    spark.stop()
