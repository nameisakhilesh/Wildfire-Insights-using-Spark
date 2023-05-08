import pyspark
from pyspark.sql import SQLContext
import pyspark.sql.functions as sqlf
from pyspark.sql.types import IntegerType

# initialize spark context
sc = pyspark.SparkContext("local[*]", appName="centroids")
# initialize Spark dfs using sql
sqlContext = SQLContext(sc)

stats = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true")\
    .option("delimiter", "\t").load('/Users/akhilesh/WildfireDB_Resources/cal_zs.csv')
neighbors = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "false")\
    .option("delimiter", ",").load('/Users/akhilesh/WildfireDB_Resources/neighbors_us_dis.csv')

count = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "false")\
    .option("delimiter", ",").load('/Users/akhilesh/WildfireDB_Resources/cal_wildfire') 
neighbors = neighbors.withColumnRenamed("_c0", "fid").withColumnRenamed("_c1", "Neighbour_1")\
    .withColumnRenamed("_c2", "Neighbour_2").withColumnRenamed("_c3", "Neighbour_3")\
    .withColumnRenamed("_c4", "Neighbour_4").withColumnRenamed("_c5", "Neighbour_5")\
    .withColumnRenamed("_c6", "Neighbour_6").withColumnRenamed("_c7", "Neighbour_7")\
    .withColumnRenamed("_c8", "Neighbour_8")

# centroids = centroids.withColumn('_c0', sqlf.regexp_replace(sqlf.col('_c0'), "\\(", "")).withColumn('_c2', sqlf.regexp_replace(sqlf.col('_c2'), "POINT \\(", ""))
# centroids = centroids.withColumn('_c2', sqlf.regexp_replace(sqlf.col('_c2'), "\\)\\)", ""))
# centroids = centroids.withColumn('c_latitude', sqlf.split(sqlf.col("_c2"), ' ').getItem(0))\
#     .withColumn('c_longitude', sqlf.split(sqlf.col("_c2"), ' ').getItem(1)).drop("_c2")\
#     .withColumnRenamed("_c1", "Shape").withColumnRenamed("_c0", "fid").withColumn("fid",sqlf.col("fid").cast(IntegerType()))

# print(stats.schema)
# print(neighbors.schema)
# print(centroids.schema)

# stats_neighbors = stats.join(neighbors, on=['fid'], how='left')
# #stats_n_centroids = stats_neighbors.join(centroids, on=['fid'], how='left')
# stats_neighbors = stats_neighbors.withColumnRenamed("fid","polygon_id")
# #print(stats_neighbors.schema)
# stats_neighbors.write.option("sep", "\t").option("header","true").csv("/Users/akhilesh/WildfireDB_Resources/cal_statsnc")

print(count.count())
sc.stop()
