# Introduction to Spark and Pyspark

Key points from this chapter

1. Spark is fast and powerful analytics engine because it uses in-memory operation, and its use of robust, distributed, fault-tolerant data abstractions - resilient distributed datasets (RDDs) and DataFrames.
2. Transformations of data in with Spark can be performed in 4 different programming languages - Java, Scala, R, and Python. This book covers Python's API for Spark - PySpark - which is popular for solving big data problems by efficiently transforming data into the desired result and format.
3. Big data can be represented using the aforementioned distributed data abstractions provided by Spark.


## Spark Data Abstractions

### Resilient Distributed Dataset (RDDs)
RDDs represent data as a collection of elements. It is an immutable set of distributed elements of type *T* - `RDD[T]`
- `RDD[Integer]` - an immutable collection of integers
- `RDD[String]` - an immutable collection of strings
- `RDD[(String, Integer)]` - an immutable collection of pairs of strings and integers. 

RDD Operations

Since RDDs are immutable and distributed, once created they cannot be altered. They can be *transformed*. RDDs support two types of operations
- transformations: transform source RDD(s) into one or more new RDDs
- actions: transform source RDD(s) into a non-RDD object such as an array or dictionary.

#### Transformations
Examples of transformation functions available in Spark
- `map()`
- `flatMap()` - 1-to-many transformation. returns a new RDD by applying a function to all elements of source RDD and then flattening the reuslts. 
- `groupByKey()`
- `reduceByKey()`
- `filter()`


`transformation: source_RDD[V] --> target_RDD[T]`

RDDs are lazily evaluated - only evaluated when an action is performed on them. 

#### Actions
An action is a RDD operation or function that produces a non-RDD value. The output of an action is a tangible value: a saved file, integer, a count of elements, list of dictionaries, a dictionary. 
`action: RDD => non-RDD value`

Examples of actions
- `collect()` - converts an RDD[T] to a list of type T
- `count()` - counts the number of elements in a given RDD
- `saveAsTextFile()` - saves RDD elements to disk


### Dataframe
Similar to how relational databases store data in a table - organized into named columns. However, a DataFrame in Spark is also immutable. DataFrames can be created from various sources such as Hive tables, structured data files, external databases, or even RDDs. SQL queries can also be executed against DataFrames. 
