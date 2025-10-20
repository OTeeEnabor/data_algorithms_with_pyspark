# Version 1 - DNA Base Count Solution
"""
Solution to the DNA base count problem

1. Read in input data (FASTA format) and create RDD[string] - each RDD element is a FASTA record.
2. Define a mapper function - for every DNA letter in FASTA record, emit a pair of (dna_letter, 1)
3. Perform an aggregation on dna_letter by summing their frequencies - (sum of 1s for each dna_letter )
"""
from pathlib import Path
from pyspark.sql import SparkSession

def process_fasta_records(fasta_record:str):
    """
    creates an iterable list
    flatten list into many elements
    """
    key_value_list = []
    if fasta_record.startswith(">"):
        key_value_list.append(("z",1))
    else:
        chars = fasta_record.lower()
        for c in chars:
            key_value_list.append((c,1))
    print(key_value_list)
    return key_value_list


def main():
    spark = SparkSession.builder.appName("DNA_Count_1").getOrCreate()
    BASE_DIR  = Path("/opt/spark/data")
    data_path = str(BASE_DIR / "chap02/sample.fasta")
    
    # create records rdd
    records_rdd = spark.read.text(data_path).rdd.map(lambda r: r[0])
    input_data = records_rdd.collect()  
    
    print("*"*20)
    print(input_data)
    print("*"*20)


    # transformation
    pairs_rdd = records_rdd.flatMap(lambda rec: process_fasta_records(rec))
    print("*"*20)
    print(pairs_rdd.collect())
    print("*"*20)

    frequencies_rdd = pairs_rdd.reduceByKey(lambda x,y: x+y)

    print("*"*20)
    print(frequencies_rdd.collect())
    print("*"*20)

    # save to file
    output_path = "/opt/spark/apps/data_algorithms_spark/chap02/version_1_sol.txt"
    frequencies_rdd.saveAsTextFile(output_path)

    spark.stop()

if __name__ == "__main__":
    main()

