# Version 3 - DNA Base Count Solution
"""
Solution to the DNA base count problem

1. Read in FASTA data, creates an `RDD[String]` instance with each element being a FASTA record.
2. Use mapPartitions() to create hashmap of dna letter per partition
3. For each partition, aggregate and sum all the frequencies.

"""
from collections import defaultdict
from pathlib import Path
from pyspark.sql import SparkSession

def process_FASTA_partitions(iterator):
    # create dict with value type int
    hashmap = defaultdict(int)

    for fasta_record in iterator:
        if fasta_record.startswith(">"):
            hashmap["z"] += 1
        else:
            chars = fasta_record.lower()
            for c in chars:
                hashmap[c] += 1
    print(f"{hashmap=}")
    key_value_list = [(k,v) for k,v in hashmap.iteritems()]
    print(f"{key_value_list=}")
    return key_value_list
    

def main():
    # create spark session
    spark = SparkSession.builder.appName("DNA_Count_3").getOrCreate()
    # read in fasta record
    BASE_DIR  = Path("/opt/spark/data")
    data_path = str(BASE_DIR / "chap02/sample.fasta")

    # define RDD[string] from the input fasta record
    records_rdd = spark.sparkContext.textfile(data_path)

    # get the number of partitions that rdd has been distributed across
    print(records_rdd.getNumPartitions())

    # transform
    pairs_rdd = records_rdd.mapPartitions(process_FASTA_partitions)
    print("intermediate hashmap")
    print(pairs_rdd.collect())
    print("*"*20)

    # find the frequencies of dna letters
    frequencies_rdd = pairs_rdd.reduceByKey(lambda x,y : x+y)
    print("aggregate frequencies by dna letter - as list")
    print(frequencies_rdd.collect())
    print("*"*20)

    print("aggregate frequencies by dna letter - as hashmap")
    print(frequencies_rdd.collectAsMap())
    print("*"*20)


    spark.close()

if __name__ == "__main__":
    main()

