from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .enableHiveSupport()
             .appName("Assignment_4_df")
             .getOrCreate())

    # 0) Load data as SQL
    fake_data = spark.read.csv("fake_data.csv", header=True, inferSchema=True)

    # 1) Find birth country with most people
    most_births = fake_data\
        .select("Birth_Country")\
        .withColumnRenamed("Birth_Country", "Birth_Country_Most_Births")\
        .groupBy(col("Birth_Country_Most_Births"))\
        .agg(count("Birth_Country_Most_Births").alias("count"))\
        .orderBy(count("Birth_Country_Most_Births"), ascending=False)\
        .limit(1)
    most_births.show()

    # 2) Find avg income of people born in USA
    usa_avg_income = fake_data\
        .select("Income")\
        .where(col("Birth_Country") == "United States of America")\
        .agg(avg("Income").alias("USA_Average_Income"))
    usa_avg_income.show()

    # 3) Find num people with income over 100k with unapproved loan
    high_income_no_loan = fake_data\
        .select("Loan_Approved")\
        .where((col("Income") > 100000) & ~ col("Loan_Approved")) \
        .agg(count("Loan_Approved").alias("Num_High_Income_Unapproved"))
    high_income_no_loan.show()

    # 4) Find name, income, job of 10 highest income people in USA
    highest_earning_americans = fake_data\
        .select("First_Name", "Last_name", "Income", "Job")\
        .withColumnRenamed("Job", "American_Highest_Paying_Job")\
        .where(col("Birth_Country") == "United States of America")\
        .orderBy("Income", ascending=False)\
        .limit(10)
    highest_earning_americans.show()

    # 5) Find number of distinct jobs
    distinct_jobs = fake_data\
        .select("Job")\
        .agg(countDistinct("Job").alias("Num_Distinct_Jobs"))
    distinct_jobs.show()

    # 6) Find number of writers not making 100k
    modest_writers = fake_data\
        .where((col("Income") < 100000) & (col("Job") == "Writer"))\
        .agg(count("Job").alias("Modest_Writers"))
    modest_writers.show()
