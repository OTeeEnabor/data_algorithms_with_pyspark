"""
ETL process for pulled U.S. Census Bureau data

Extraction - create a dataframe from the JSON document.
Transformation - filter data and keep records for seniors
                - add new column total - total of males and females
Loading - write revised DataFrame into MySQL database and verify load process.
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def main():
    spark = SparkSession.builder.master("local").appName("Census-ETL").getOrCreate()

    # Extraction
    BASE_DIR  = Path("/opt/spark/data")
    # read in the raw data for etl process
    data_path = str(BASE_DIR / "census_2010.json")
    census_df = spark.read_json(data_path)
    census_df.count()
    census_df.show(200)

    # Transformation
    # get senior records in the census_df
    seniors = census_df[census_df["age"] > 54]
    # count number of seniors
    seniors.count()
    # show
    seniors.show(200)

    seniors_final = seniors.withColumn("total", lit(seniors.males + seniors.females))
    seniors_final.show(200)

    # loading
    (seniors_final.write
     .format("jdbc")
     .option("driver", "com.mysql.jdbc.Driver")
     .mode("overwrite")
     .option("url", "jdbc:mysql://localhost/testdb")
     .option("dbtable", "seniors")
     .option("user", "root")
     .option("password", "root_password")
     .save())
    
    spark.stop()


if __name__ == "__main__":
    main()

