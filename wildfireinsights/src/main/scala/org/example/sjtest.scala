package org.example
import edu.ucr.cs.bdlab.beast.{RasterReadMixinFunctions, ReadWriteMixinFunctions}
import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import edu.ucr.cs.bdlab.raptor.{RaptorJoinFeature, Statistics}
import org.apache.spark.SparkConf
import org.apache.spark.beast.{CRSServer, SparkSQLRegistration}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object sjtest {
  def newFeature(newAttributes1: List[Any]) = ???

  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("SJFire")
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
      val polygons = sparkContext.shapefile("/Users/akhilesh/WildfireDB_Resources/cal_grid_wgs83.zip")
      val vector = sparkContext.geojsonFile("/Users/akhilesh/WildfireDB_Resources/clip_vector.geojson")

      val firepoints = sparkContext.readCSVPoint("/Users/akhilesh/WildfireDB_Resources/fire_archive_SV-C2_321628.csv", "longitude", "latitude", ',', skipHeader = true)
      // Run a spatial join operation between points and polygons (point-in-polygon) query
      println(firepoints.first())
      val sjResults: RDD[(IFeature, IFeature)] =
        polygons.spatialJoin(firepoints, ESJPredicate.Contains, ESJDistributedAlgorithm.PBSM)

      //sjResults.saveAsTextFile("/Users/akhilesh/WildfireDB_Resources/sjtest11")
      val finalResults: RDD[IFeature] = sjResults.map(pip => {
                val polygon: IFeature = pip._1
                val point: IFeature = pip._2
                val values = point.toSeq :+ polygon.getAs[String]("fid")
                val schema = StructType(point.schema :+ StructField("polygon_id", StringType))
                new Feature(values.toArray, schema)
              })
      //finalResults.saveAsCSVPoints("/Users/akhilesh/WildfireDB_Resources/cal_firearchive",0,1,';')
      finalResults.saveAsWKTFile("/Users/akhilesh/WildfireDB_Resources/cal_firearchiveWKT",0)
    }
    finally {
      println("Done finally, stopping Spark Session")
      sparkSession.stop()
    }
  }

}
