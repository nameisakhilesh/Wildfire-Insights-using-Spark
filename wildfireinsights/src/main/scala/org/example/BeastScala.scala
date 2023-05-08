package org.example

import edu.ucr.cs.bdlab.beast.{RasterReadMixinFunctions, ReadWriteMixinFunctions}
import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import edu.ucr.cs.bdlab.raptor.{RaptorJoinFeature, Statistics}
import org.apache.spark.SparkConf
import org.apache.spark.beast.{CRSServer, SparkSQLRegistration}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.io.PrintWriter
import scala.reflect.runtime.universe._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin.ReadWriteRDDFunctions
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.locationtech.jts.geom.{Envelope, GeometryFactory}

/**
 * Scala examples for Beast
 */
object BeastScala {

  def newFeature(newAttributes1: List[Any]) = ???

  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    // Start the CRSServer and store the information in SparkConf
    CRSServer.startServer(conf)
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    try {
        val raster = sparkContext.geoTiff[Array[Int]]("/Users/akhilesh/WildfireDB_Resources/california_raster")
      //val raster = sparkContext.geoTiff[Array[Int]]("/Users/akhilesh/WildfireDB_Resources/AOI/AOI_5.tif")
      //val vector = sparkContext.geojsonFile("/Users/akhilesh/WildfireDB_Resources/clip_vector.geojson")
        val vector = sparkContext.shapefile("/Users/akhilesh/WildfireDB_Resources/cal_grid_wgs83.zip")
        val rjResult: RDD[RaptorJoinFeature[Array[Int]]] = raster.raptorJoin(vector)
        val emptyStats = new Statistics
        emptyStats.setNumBands(9)
        rjResult.map(x => (x.feature, x.m))
        .aggregateByKey(emptyStats)((stats, m) => {
          stats.collect(0, 0, m).asInstanceOf[Statistics]
          }, (a, b) => a.accumulate(b).asInstanceOf[Statistics])
          .map(x => {
            var feature = x._1
            val stats = x._2
            feature = Feature.append(feature, stats.sum.mkString(","), "sum")
            feature = Feature.append(feature, stats.min.mkString(","), "min")
            feature = Feature.append(feature, stats.max.mkString(","), "max")
            feature = Feature.append(feature, stats.count.mkString(","), "count")
            feature
        }).saveAsWKTFile("/Users/akhilesh/WildfireDB_Resources/cal_zs.csv", 0)


        }
    finally{
        println("Done finally, stopping Spark Session")
        sparkSession.stop()
    }
  }
}