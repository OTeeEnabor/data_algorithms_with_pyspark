import sys
from pathlib import Path
from pyspark.sql import SparkSession


"""
- read in input data - sample.txt - with SparkSession instance
- create mew RDD 
"""
BASE_DIR  = Path("/opt/spark/data")

def main():
    # create sparksession - entry point for programming with Spark
    spark = SparkSession.builder.getOrCreate()
    # create an RDD[String] which represents all input records;
    data_path = str(BASE_DIR / "sample.txt")
    # each record becomes an RDD element
    records = spark.sparkContext.textFile(data_path )
    print(records)
    # perform transformation - convert all characters to lower case
    records_lowercase = records.map(lambda x: x.lower())
    print("transformed records")
    print(records_lowercase)
    # perform flatMap() transformation - convert each element into a sequence of target elements.
    words = records_lowercase.flatMap(lambda x: x.split(","))
    print("flat map transformation")
    print(words)

    # perform filter operation
    filtered = words.filter(lambda x:len(x)> 2)
    print("words greater than 2 characters")
    print(filtered)

    # stop session
    spark.stop()

if __name__ == "__main__":
    main()
