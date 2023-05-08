import pyspark
from pyspark.sql import SQLContext
import pyspark.sql.functions as sqlf
import datetime
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("MyPySparkApp")
conf = conf.set("spark.driver.memory", "8g")
conf = conf.set("spark.executor.memory", "8g")

# initialize spark context
#sc = pyspark.SparkContext("local[*]", appName="datam")
sc = SparkContext(conf=conf)

# initialize Spark dfs using sql
sqlContext = SQLContext(sc)

raster_data = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", "\t").load('/Users/akhilesh/WildfireDB_Resources/cal_statsnc')
#print(raster_data.schema)
fire_archivetemp = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", "\t").load('/Users/akhilesh/WildfireDB_Resources/cal_firearchiveWKT')
#fire_archive = sqlContext.read.load('/Users/akhilesh/WildfireDB_Resources/cal_firearchive', format='com.databricks.spark.csv', header='true', inferSchema='true')

fire_archive=fire_archivetemp.select("polygon_id","geometry","brightness","scan","track","acq_date","acq_time","satellite","instrument","confidence","version","bright_t31","frp","daynight","type")
#print(fire_archive.schema)
fire_archive.show(2)

# fire_archive = fire_archive.drop(sqlf.col("drop"))

# # use time interval in days, if there are two points in the same day same polygon, take average
fire_archive = fire_archive.withColumn("acq_date", sqlf.to_date(sqlf.unix_timestamp(sqlf.col("acq_date"), 'yyyy-mm-dd').cast("timestamp")))
fire_archive = fire_archive.groupBy("acq_date", "polygon_id").agg(sqlf.avg("frp"))

# # use time periods 2012 through 2017
end_date = datetime.datetime(2023, 1, 1).date()
fire_archive = fire_archive.filter(sqlf.col("acq_date") < end_date)

fire_archive = fire_archive.withColumn("next_acq_date", sqlf.date_add(sqlf.col("acq_date"), 1))
fire_archive = fire_archive.withColumnRenamed("avg(frp)", "frp")
print(fire_archive.schema)
fire_raster_columns = ["polygon_id", "acq_date", "frp", "next_acq_date", "Neighbour_1"]
fire_raster1 = fire_archive.join(raster_data, on=['polygon_id'], how='left').select(fire_raster_columns).withColumnRenamed("Neighbour_1", "Neighbour")
fire_raster_columns = ["polygon_id", "acq_date", "frp", "next_acq_date", "Neighbour_2"]
fire_raster2 = fire_archive.join(raster_data, on=['polygon_id'], how='left').select(fire_raster_columns).withColumnRenamed("Neighbour_2", "Neighbour")
# polygon_id, acq_date,frp,next_acq_date,Neighbour
fire_raster_columns = ["polygon_id", "acq_date", "frp", "next_acq_date", "Neighbour_3"]
fire_raster3 = fire_archive.join(raster_data, on=['polygon_id'], how='left').select(fire_raster_columns).withColumnRenamed("Neighbour_3", "Neighbour")
# polygon_id, acq_date,frp,next_acq_date,Neighbour
fire_raster_columns = ["polygon_id", "acq_date", "frp", "next_acq_date", "Neighbour_4"]
fire_raster4 = fire_archive.join(raster_data, on=['polygon_id'], how='left').select(fire_raster_columns).withColumnRenamed("Neighbour_4", "Neighbour")
# polygon_id, acq_date,frp,next_acq_date,Neighbour
fire_raster_columns = ["polygon_id", "acq_date", "frp", "next_acq_date", "Neighbour_5"]
fire_raster5 = fire_archive.join(raster_data, on=['polygon_id'], how='left').select(fire_raster_columns).withColumnRenamed("Neighbour_5", "Neighbour")
# polygon_id, acq_date,frp,next_acq_date,Neighbour
fire_raster_columns = ["polygon_id", "acq_date", "frp", "next_acq_date", "Neighbour_6"]
fire_raster6 = fire_archive.join(raster_data, on=['polygon_id'], how='left').select(fire_raster_columns).withColumnRenamed("Neighbour_6", "Neighbour")
# polygon_id, acq_date,frp,next_acq_date,Neighbour
fire_raster_columns = ["polygon_id", "acq_date", "frp", "next_acq_date", "Neighbour_7"]
fire_raster7 = fire_archive.join(raster_data, on=['polygon_id'], how='left').select(fire_raster_columns).withColumnRenamed("Neighbour_7", "Neighbour")
# polygon_id, acq_date,frp,next_acq_date,Neighbour
fire_raster_columns = ["polygon_id", "acq_date", "frp", "next_acq_date", "Neighbour_8"]
fire_raster8 = fire_archive.join(raster_data, on=['polygon_id'], how='left').select(fire_raster_columns).withColumnRenamed("Neighbour_8", "Neighbour")
# polygon_id, acq_date,frp,next_acq_date,Neighbour
fire_raster = fire_raster1.union(fire_raster2).union(fire_raster3).union(fire_raster4).union(fire_raster5).union(fire_raster6).union(fire_raster7).union(fire_raster8)
fire_raster = fire_raster.na.drop()

#print(raster_data.schema)
# #print(raster_data.count())
#print(fire_archive.schema)
#fire_archive.show(2)
# #print(fire_archive.count())
# #print(fire_raster1.schema)
# #print(fire_raster.count())
# #print(fire_raster.show())

fire_archive_columns = fire_archive.columns
new_fire_archive_columns = ["Neighbour_" + field for field in fire_archive_columns]
fire_archive = fire_archive.rdd.toDF(new_fire_archive_columns)
col_drop = ["Neighbour_polygon_id", "Neighbour_acq_date", "Neighbour_next_acq_date", "next_acq_date"]
fire_frp = fire_raster.alias("firedf1").join(fire_archive.alias("firedf2"), ((sqlf.col("firedf1.Neighbour") == sqlf.col("firedf2.Neighbour_polygon_id")) &
                           (sqlf.col("firedf1.next_acq_date") == sqlf.col("firedf2.Neighbour_acq_date"))), how="left").drop(*col_drop)
fire_frp = fire_frp.na.fill(0)
neighbor_names = ["Polygon_shape", "Neighbour_1", "Neighbour_2", "Neighbour_3", "Neighbour_4", "Neighbour_5",
                   "Neighbour_6", "Neighbour_7", "Neighbour_8"]

raster_data = raster_data.drop(*neighbor_names)

fire_frp = fire_frp.join(raster_data, on=['polygon_id'], how='left')

column_names = raster_data.columns
new = ["Neighbour_" + field for field in column_names]
raster_data = raster_data.rdd.toDF(new)
fire_neighbors = fire_frp.alias("df1").join(raster_data.alias("df2"), (sqlf.col("df1.Neighbour") == sqlf.col("df2.Neighbour_polygon_id")),
                           how="left").drop(sqlf.col("Neighbour_polygon_id"))
end_date = datetime.datetime(2014, 1, 1).date()
column_year = fire_neighbors.columns
column_year2012 = [k for k in column_year if '2012' in k]
column_year2014 = [k for k in column_year if '2014' in k]
column_year2016 = [k for k in column_year if '2016' in k]
column_year2018 = [k for k in column_year if '2018' in k]
column_year2020 = [k for k in column_year if '2020' in k]
fire_neighbors2012 = fire_neighbors.filter(sqlf.col("acq_date") < end_date).drop(*column_year2014).drop(*column_year2016).drop(*column_year2018).drop(*column_year2020)
columns = fire_neighbors2012.columns
new_columns = [field.replace("2012", "") for field in columns]
fire_neighbors2012 = fire_neighbors2012.rdd.toDF(new_columns)


start_date = datetime.datetime(2013, 12, 31).date()
end_date = datetime.datetime(2016, 1, 1).date()
fire_neighbors2014 = fire_neighbors.filter(sqlf.col("acq_date") < end_date).filter(sqlf.col("acq_date") > start_date)\
     .drop(*column_year2012).drop(*column_year2016).drop(*column_year2018).drop(*column_year2020)
columns = fire_neighbors2014.columns
new_columns = [field.replace("2014", "") for field in columns]
fire_neighbors2014 = fire_neighbors2014.rdd.toDF(new_columns)


start_date = datetime.datetime(2015, 12, 31).date()
end_date = datetime.datetime(2018, 1, 1).date()
fire_neighbors2016 = fire_neighbors.filter(sqlf.col("acq_date") < end_date).filter(sqlf.col("acq_date") > start_date)\
    .drop(*column_year2012).drop(*column_year2014).drop(*column_year2018).drop(*column_year2020)
columns = fire_neighbors2016.columns
new_columns = [field.replace("2016", "") for field in columns]
fire_neighbors2016 = fire_neighbors2016.rdd.toDF(new_columns)


start_date = datetime.datetime(2017, 12, 31).date()
end_date = datetime.datetime(2020, 1, 1).date()
fire_neighbors2018 = fire_neighbors.filter(sqlf.col("acq_date") < end_date).filter(sqlf.col("acq_date") > start_date)\
    .drop(*column_year2012).drop(*column_year2014).drop(*column_year2016).drop(*column_year2020)
columns = fire_neighbors2018.columns
new_columns = [field.replace("2018", "") for field in columns]
fire_neighbors2018 = fire_neighbors2018.rdd.toDF(new_columns)

start_date = datetime.datetime(2019, 12, 31).date()
end_date = datetime.datetime(2023, 1, 1).date()
fire_neighbors2020 = fire_neighbors.filter(sqlf.col("acq_date") < end_date).filter(sqlf.col("acq_date") > start_date)\
    .drop(*column_year2012).drop(*column_year2014).drop(*column_year2016).drop(*column_year2018)
columns = fire_neighbors2020.columns
new_columns = [field.replace("2020", "") for field in columns]
fire_neighbors2020 = fire_neighbors2020.rdd.toDF(new_columns)

output = fire_neighbors2012.union(fire_neighbors2014).union(fire_neighbors2016).union(fire_neighbors2018).union(fire_neighbors2020)


#output.write.option("delimiter","\t").option("header","true").csv("/Users/akhilesh/WildfireDB_Resources/cal_firezones")
#print(output.schema)
fire_neighbors2020.write.option("delimiter","\t").option("header","true").csv("/Users/akhilesh/WildfireDB_Resources/cal_firezones2020")
# #fire_neighbors2016.write.option("delimiter","\t").csv("file:///remote_data/datasets/Raptor/SW_landfire_us/results/2016")
# #print(fire_neighbors2012.schema)
# #print(fire_neighbors2014.schema)
# #print(fire_neighbors2016.schema)
# #print(fire_neighbors.schema)
# #fire_neighbors.write.option("delimiter","\t").csv("file:///remote_data/datasets/Raptor/SW_landfire_us/results/features_US_2018")

sc.stop()
