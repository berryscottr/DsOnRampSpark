from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .enableHiveSupport()
             .appName("Assignment_4_sql")
             .getOrCreate())

    # 0) Load data as SQL
    fake_data = spark.read.csv("fake_data.csv", header=True, inferSchema=True)
    fake_data.createOrReplaceTempView("fake_data")

    # 1) Find birth country with most people
    most_births = spark.sql("SELECT Birth_Country AS Birth_Country_Most_Births, COUNT(*) AS count "
                            "FROM fake_data GROUP BY Birth_Country "
                            "ORDER BY COUNT(*) DESC LIMIT 1")
    most_births.show()

    # 2) Find avg income of people born in USA
    usa_avg_income = spark.sql("SELECT AVG(Income) AS USA_Average_Income "
                               "FROM fake_data WHERE Birth_Country = 'United States of America' "
                               "GROUP BY Birth_Country ")
    usa_avg_income.show()

    # 3) Find num people with income over 100k with unapproved loan
    high_income_no_loan = spark.sql("SELECT COUNT(*) AS Num_High_Income_Unapproved "
                                    "FROM fake_data WHERE Income > 100000 "
                                    "AND NOT Loan_Approved")
    high_income_no_loan.show()

    # 4) Find name, income, job of 10 highest income people in USA
    highest_earning_americans = spark.sql("SELECT First_Name, Last_name, "
                                          "Income, Job AS American_Highest_Paying_Job "
                                          "FROM fake_data WHERE Birth_Country = 'United States of America' "
                                          "GROUP BY First_Name, Last_name, Income, American_Highest_Paying_Job "
                                          "ORDER BY Income DESC LIMIT 10")
    highest_earning_americans.show()

    # 5) Find number of distinct jobs
    distinct_jobs = spark.sql("SELECT COUNT(DISTINCT Job) AS Num_Distinct_Jobs FROM fake_data")
    distinct_jobs.show()

    # 6) Find number of writers not making 100k
    modest_writers = spark.sql("SELECT COUNT(*) AS Modest_Writers "
                               "FROM fake_data WHERE Income < 100000 "
                               "AND Job = 'Writer'")
    modest_writers.show()
