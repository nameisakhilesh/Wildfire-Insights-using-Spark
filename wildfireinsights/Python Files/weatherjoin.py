import pyspark
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as sqlf
import datetime
from pyspark.sql.functions import col
from pyspark.sql.functions import atan2, radians, lit, cos
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, DoubleType, StringType
from pyspark.sql.functions import udf
import re
import math
from pyspark.sql.functions import countDistinct
from pyspark.storagelevel import StorageLevel


# initialize spark context
# sc = pyspark.SparkContext("local[*]", appName="datam")

# # initialize Spark dfs using sql
# sqlContext = SQLContext(sc)
conf = SparkConf().setAppName("MyPySparkApp")
conf = conf.set("spark.driver.memory", "8g")
conf = conf.set("spark.executor.memory", "8g")
conf = conf.set("spark.default.parallelism", "8")
conf = conf.set("spark.sql.shuffle.partitions", "8")

# initialize spark context
#sc = pyspark.SparkContext("local[*]", appName="datam")
sc = SparkContext(conf=conf)

# initialize Spark dfs using sql
sqlContext = SQLContext(sc)

#raster_data = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", "\t").load('/Users/akhilesh/WildfireDB_Resources/cal_statsnc')
#print(raster_data.schema)

# weather_archive22 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load('/Users/akhilesh/WildfireDB_Resources/Weather/bulk_weather_data/bulk_weather_2022.csv')
# weather_archive21 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load('/Users/akhilesh/WildfireDB_Resources/Weather/bulk_weather_data/bulk_weather_2021.csv')
# weather_archive20 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load('/Users/akhilesh/WildfireDB_Resources/Weather/bulk_weather_data/bulk_weather_2020.csv')
# weather_archive19 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load('/Users/akhilesh/WildfireDB_Resources/Weather/bulk_weather_data/bulk_weather_2019.csv')
# weather_archive18 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load('/Users/akhilesh/WildfireDB_Resources/Weather/bulk_weather_data/bulk_weather_2018.csv')
# weather_archive17 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load('/Users/akhilesh/WildfireDB_Resources/Weather/bulk_weather_data/bulk_weather_2017.csv')
# weather_archive16 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load('/Users/akhilesh/WildfireDB_Resources/Weather/bulk_weather_data/bulk_weather_2016.csv')
# weather_archive15 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load('/Users/akhilesh/WildfireDB_Resources/Weather/bulk_weather_data/bulk_weather_2015.csv')
# weather_archive14 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load('/Users/akhilesh/WildfireDB_Resources/Weather/bulk_weather_data/bulk_weather_2014.csv')
# weather_archive13 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load('/Users/akhilesh/WildfireDB_Resources/Weather/bulk_weather_data/bulk_weather_2013.csv')
# weather_archive12 = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load('/Users/akhilesh/WildfireDB_Resources/Weather/bulk_weather_data/bulk_weather_2012.csv')

# weather_archivetemp = weather_archive20.union(weather_archive21).union(weather_archive22).union(weather_archive19).union(weather_archive18).union(weather_archive17).union(weather_archive16).union(weather_archive15).union(weather_archive14).union(weather_archive13).union(weather_archive12)
# weather_archivetemp.write.option("delimiter",",").option("header","true").csv("/Users/akhilesh/WildfireDB_Resources/bulkweather")

# HEREEEEE


weather_archivetemp = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load('/Users/akhilesh/WildfireDB_Resources/bulkweather')
polygonweather = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", ",").load('/Users/akhilesh/WildfireDB_Resources/polygonweather.csv')

weather_archivetemp = weather_archivetemp.persist(StorageLevel.MEMORY_AND_DISK)
polygonweather = polygonweather.persist(StorageLevel.MEMORY_AND_DISK)
weather_archivetemp = weather_archivetemp.drop(sqlf.col("station_latitude"))
weather_archivetemp = weather_archivetemp.drop(sqlf.col("station_longitude"))


polygonweather = polygonweather.drop(sqlf.col("WKT"))
polygonweather = polygonweather.withColumnRenamed("fid","polygon_id")
polygonweather = polygonweather.withColumnRenamed("nearest_station_id","station_id")
print(polygonweather.schema)
#weather_archive.write.parquet("/Users/akhilesh/WildfireDB_Resources/cal_weatherarchive.parquet")
# num_partitions = 300
# repartitioned_df = weather_archive.repartition(num_partitions)
# repartitioned_df.write.option("delimiter",",").csv("/Users/akhilesh/WildfireDB_Resources/cal_weatherarchive1", mode="overwrite")




#HEREEE

# weather_archive = weather_archive.select("polygon_id","station_id","tavg","tmin","tmax","prcp","wdir","wspd","pres","date")

# # num_unique_polygon_ids = weather_archive.select('polygon_id').groupBy().agg(countDistinct('polygon_id')).collect()[0][0]
# # print(num_unique_polygon_ids)
# firezones = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", "\t").load('/Users/akhilesh/WildfireDB_Resources/cal_firezones')
# firezonesarchive = firezones.join(polygonweather, on="polygon_id", how="left")
# print(firezonesarchive.schema)
# firezonesarchive.write.option("delimiter","\t").option("header","true").csv("/Users/akhilesh/WildfireDB_Resources/cal_weather")
# weather_archivetemp = weather_archivetemp.withColumnRenamed('date','acq_date')
# firezonesarchive = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", "\t").load('/Users/akhilesh/WildfireDB_Resources/cal_weather')
# weather_archive = firezonesarchive.join(weather_archivetemp, on=["station_id", "acq_date"], how="left")
# print(weather_archive.schema)
# #weather_archive = weather_archive.drop(sqlf.col("station_id"))

# weather_archive.write.option("delimiter","\t").option("header","true").csv("/Users/akhilesh/WildfireDB_Resources/cal_final")

weather_archive = sqlContext.read.format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true").option("delimiter", "\t").load('/Users/akhilesh/WildfireDB_Resources/cal_final') 
#print(weather_archive.schema)
#weather_archive.write.option("delimiter",",").option("header","true").csv("/Users/akhilesh/WildfireDB_Resources/cal_weatherarchive")

# # weather_archive=weather_archive.select("polygon_id","geometry","tavg","tmin","tmax","prcp","wdir","wspd","pres","station_id","date")
# # weather_archive = weather_archive.withColumnRenamed('geometry','weatherpoint')

# weather_archive.show(2)

# joined_df = firezones.join(weather_archive, ["acq_date", "polygon_id"], "left")
# joined_df.write.option("delimiter","\t").option("header","true").csv("/Users/akhilesh/WildfireDB_Resources/cal_weather")







# joined_df = firezones.join(weather_archive, ["acq_date", "polygon_id"], "inner")
# count = joined_df.count()
# print(count)
# joined_df = joined_df.drop(sqlf.col("weatherpoint"))
#print(joined_df.schema)

# pattern = 'POLYGON \(\(([-0-9\.]+) ([\-0-9\.]+),'
# # Define a UDF to extract the longitude and latitude values using the regular expression pattern
# extract_coordinates = udf(lambda x: (float(re.findall(pattern, x)[0][0]), float(re.findall(pattern, x)[0][1])), StructType([
#     StructField("longitude", DoubleType()),
#     StructField("latitude", DoubleType())
# ]))


# HEREEEEEEE

def extract_coordinates(x):
    if x is None:
        return None
    match = re.search(r'(-?\d+\.\d+)\s(-?\d+\.\d+)', x)
    if match:
        longitude, latitude = match.groups()
        latitude = float(latitude)
        longitude = float(longitude)
        return longitude, latitude
# Register the UDF with PySpark
extract_coordinates = udf(extract_coordinates, StructType([
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType())
]))


# Add two new columns to the DataFrame to store the longitude and latitude values
weather_archive = weather_archive.withColumn('longitude_rc', extract_coordinates('geometry').getField("longitude"))
weather_archive = weather_archive.withColumn('latitude_rc', extract_coordinates('geometry').getField("latitude"))
weather_archive = weather_archive.withColumn('longitude_nc', extract_coordinates('Neighbour_geometry').getField("longitude"))
weather_archive = weather_archive.withColumn('latitude_nc', extract_coordinates('Neighbour_geometry').getField("latitude"))



# Convert the lat/long coordinates to radians
lat1_rad, long1_rad = radians(lit(weather_archive["latitude_rc"])), radians(lit(weather_archive["longitude_rc"]))
lat2_rad, long2_rad = radians(lit(weather_archive["latitude_nc"])), radians(lit(weather_archive["longitude_nc"]))

# Calculate the bearing between the two points
bearing = atan2(long2_rad - long1_rad, lat2_rad - lat1_rad).alias("bearing")

# Define the schema for the DataFrame
schema = StructType([
    StructField('bearing', DoubleType(), True)
])

newdf = weather_archive.withColumn("bearing",bearing)


def calculate_angle(wind_direction_degrees):
    if wind_direction_degrees is not None:
    # Convert wind direction from degrees to radians
        wind_direction_radians = math.radians(wind_direction_degrees)
    
    # Subtract pi/2 from wind direction in radians
        angle_radians = wind_direction_radians - math.pi/2
    
    # If the result is negative, add 2*pi to it
        if angle_radians < 0:
            angle_radians = angle_radians - 2*angle_radians
            angle_radians += math.pi
        else:
            angle_radians = math.pi - angle_radians
    # Convert to degrees
        angle_degrees = math.degrees(angle_radians)
    
        return angle_radians

angle_udf = udf(calculate_angle, DoubleType())
newdf=newdf.withColumn("angle",angle_udf(col("wdir")))



def windcomp(ws,A,B):
    if ws and B and A is not None:
        cos_AB = math.cos(A) * math.cos(B) + math.sin(A) * math.sin(B)
        #print(cos_AB,A,B,ws)
        return ws*cos_AB
windcomp_udf = udf(windcomp,DoubleType())
newdf =newdf.withColumn("wcomp",windcomp_udf(col("wspd"),col("bearing"),col("angle")))

newdf = newdf.drop('angle','bearing','latitude_nc','longitude_nc','latitude_rc','longitude_rc','station_id')
print(newdf.schema)
newdf.write.option("delimiter","\t").option("header","true").csv("/Users/akhilesh/WildfireDB_Resources/cal_wildfire")
#coscal = windcomp(5.2,-2.4398184368878875,angle)
#print(coscal)


sc.stop()