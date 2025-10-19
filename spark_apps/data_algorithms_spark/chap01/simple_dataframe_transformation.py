"""
Create a dataframe and find the average and sum of hours worked by employees per department. 
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum

def main():
    # create a Dataframe using SparkSession
    spark = SparkSession.builder.appName("df-demo").getOrCreate()
    # create dummy data to use for dataframe demo
    dept_emps = [("Sales", "John", 25),("Sales", "Palesa", 32),("IT", "Shabrin", 30), ("IT","Thabang", 45),("HR", "Thabo", 27),("HR", "Sinovuyo", 30)
                ]
    # create dataframe from dummy data above
    df = spark.createDataFrame(dept_emps, ["dept", "name", "hours"])

    # group the same records of the same department together, aggregate their hours, and compute average
    dept_hours_avgs = df.groupBy("dept").agg(
        avg("hours").alias("average"),
        sum("hours").alias("total")
    )
    # show the aggregation
    dept_hours_avgs.show()

    spark.stop()


if __name__ == "__main__":
    main()

    