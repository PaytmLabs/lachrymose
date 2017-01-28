package com.paytm.hero.marketing


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Calendar

object Lachrymose {
  def main(args: Array[String]) {

    val ga_path = s"/workspace/midgar/prod/base/ga_sanitized/"
    val oauth_snap = "/apps/hive/warehouse/oauth.db/customer_registration_snapshot_v2"
    val output_path = s"/tmp/adam"
    val txt_output = "/tmp/adam-count" + Calendar.getInstance().get(Calendar.SECOND) + ".txt"
    val hdfs_user = "adam"
    val hdfs_connect_string = "hdfs://labshdpds2"

    val ga_dates = Array("dateday=20170126/", "dateday=20170120/", "dateday=20170121/", "dateday=20170122/", "dateday=20170123/", "dateday=20170124/", "dateday=20170125/", "dateday=20170126/")

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = SparkSession
      .builder()
      .appName("lacyrymose")
      .getOrCreate()



    import sqlContext.implicits._


    def processGAData(x: String): Unit = {
      sqlContext.read
        .option("mergeSchema", "true")
        .parquet(s"$ga_path/$x")
        .filter(($"geo_country" === "Canada") || ($"geo_country" === "United States"))
        .select("customer_id", "transaction_revenue", "geo_country")
        .groupBy("customer_id").agg(sum("transaction_revenue").as("totalRevenue"), first("geo_country").as("location"))
        .withColumn("purchaseFlag", when($"totalRevenue".gt(0), 1).otherwise(0))
        .select("customer_id", "purchaseFlag", "location")
        .coalesce(3)
        .write.mode("append").parquet(output_path)
    }


    ga_dates.foreach(x => processGAData(x))

    HDFSHelper.write(hdfs_connect_string, txt_output, ga_dates.toString.getBytes, hdfs_user)

  }
}
