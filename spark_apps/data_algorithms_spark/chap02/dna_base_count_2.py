# Version 2 - DNA Base Count Solution
"""
Solution to the DNA base count problem

1. Read in FASTA data, creates an `RDD[String]` instance with each element being a FASTA record.
2. For every record, create a HashMap[dna_letter,frequency], then flatten the hash map with flatMap() into a list of key, value pairs.
3. For each DNA letter, aggregate and sum all the frequencies.

"""
from collections import defaultdict
from pathlib import Path
from pyspark.sql import SparkSession

def process_FASTA_as_hashmap(fasta_record):
    if fasta_record.startswith(">"):
        return [("z",1)]
    hashmap = defaultdict(int)
    chars = fasta_record.lower()
    for c in chars:
        hashmap[c] +=1
    print(f"{hashmap=}")
    key_value_list = [(k,v) for k, v in hashmap.iteritems()]
    print(f"{key_value_list}")

    return key_value_list


def main():

    spark = SparkSession.builder.appName("DNA_Count_2").getOrCreate()
    BASE_DIR  = Path("/opt/spark/data")
    data_path = str(BASE_DIR / "chap02/sample.fasta")

    records_rdd = spark.sparkContext.textfile(data_path)

    # transform
    pairs_rdd = records_rdd.flatMap(process_FASTA_as_hashmap)
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



    