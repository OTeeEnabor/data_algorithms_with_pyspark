from pathlib import Path
from pyspark.sql import SparkSession


def main():
    # create spark session
    spark = SparkSession.builder.appName("transformations-intro").getOrCreate()
    # read in fasta record
    BASE_DIR = Path("/opt/spark/data")
    data_path = str(BASE_DIR / "chap01/sample_5_records/txt")

    # create rdd0 - RDD[String]
    rdd0 = spark.sparkContext.textFile(data_path)

    print("transformation 0")
    print(rdd0.collect())
    print("*" * 20)

    # transformation 2
    # rdd1 -> map() to RDD[(String, Integer)]
    def create_pair(record):
        tokens = record.split(",")
        return (tokens[0], int(tokens[1]))

    rdd1 = rdd0.map(create_pair)
    print("transformation 1")
    print(rdd1.collect())
    print("*" * 20)

    # transformation 3
    # rdd2 (RDD[(String, Integer)]) -> map() where RDD[(String, Integer)]
    # integer doubled

    def double_int(record):
        # double record int
        key, double_int = record[0], record[1] * 2
        return (key, double_int)

    rdd3 = rdd1.map(double_int)
    print("transformation 3")
    print(rdd3.collect())
    print("*" * 20)

    # transformation 4
    # Use ReduceByKey() transformation on rdd1
    # get sum of values for each key
    #
    rdd4 = rdd1.reduceByKey(lambda x, y: x + y)
    print("transformation 4")
    print(rdd4.collect())
    print("*" * 20)

    spark.stop()


if __name__ == "__main__":
    main()
