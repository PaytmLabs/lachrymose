package com.paytm.hero.marketing


import java.time.Instant

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Lachrymose {
  def main(args: Array[String]) {

    val timestamp = Instant.now.getEpochSecond.toString
    val ga_temp_output_path = "/tmp/hero_marketing_"
    val ga_path = s"/workspace/midgar/prod/base/ga_sanitized/"
    val oauth_snap_path = "/apps/hive/warehouse/oauth.db/customer_registration_snapshot_v2"
    val canada = "Canada"
    val us = "United States"
    val row_counts = "/tmp/hero-counts_"
    val hdfs_user = "adam"
    val hdfs_connect_string = "hdfs://labshdpds2"
    //val ga_dates: Array[String] = Array("dateday=20170116", "dateday=20170117")
    //val ga_dates: Array[String] = Array("dateday=20170116", "dateday=20170117", "dateday=20170118", "dateday=20170119", "dateday=20170120", "dateday=20170121", "dateday=20170122", "dateday=20170123", "dateday=20170124", "dateday=20170125", "dateday=20170126")
    val ga_dates : Array[String] = HDFSHelper.getFileListFromDirectory(hdfs_connect_string, ga_path, hdfs_user)

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = SparkSession
      .builder()
      .appName("lacyrymose")
      .getOrCreate()

    //explicit schemas to help write the tests I will forget about later
    val gaTempSchema =
      StructType(
        StructField("customer_id", StringType, true) ::
          StructField("purchaseFlag", IntegerType, true) ::
          StructField("location", StringType, true) :: Nil
      )


    val finalDataSchema =
      StructType(
        StructField("customer_id", StringType, true) ::
          StructField("customer_phone", StringType, true) ::
          StructField("customer_email", StringType, true) ::
          StructField("customer_name", StringType, true) ::
          StructField("location", StringType, true) ::
          StructField("purchaseFlag", StringType, true) :: Nil
      )


    import sqlContext.implicits._

    //load and cache oauth
    val oauth = sqlContext.read.parquet(oauth_snap_path).
      select("customer_email", "customer_registrationid", "customer_phone", "customer_name").cache()

    //method processes a single GA file passed to it
    def processGAFilesByCountry(date: String, country: String): DataFrame = {
      val ga_file_filtered = sqlContext.read
        .option("mergeSchema", "true")
        .parquet(s"$ga_path/$date")
        .filter($"geo_country" === country)
        .select("customer_id", "transaction_revenue", "geo_country")
        .groupBy("customer_id").agg(sum("transaction_revenue").as("totalRevenue"), first("geo_country").as("location"))
        .withColumn("purchaseFlag", when($"totalRevenue".gt(0), 1).otherwise(0))
        .select("customer_id", "purchaseFlag", "location")
        .dropDuplicates(Seq("customer_id"))

      ga_file_filtered

    }


    def processGAData(datesToProcess: Array[String], country: String): DataFrame = {

      var aggregateGA = sqlContext.createDataFrame(sc.emptyRDD[Row], gaTempSchema)
      var ga_customer_enriched = sqlContext.createDataFrame(sc.emptyRDD[Row], finalDataSchema)

      val gaDFs: ListBuffer[DataFrame] = ListBuffer()

      datesToProcess.foreach { date =>
        val processedFile = processGAFilesByCountry(date, country)
        gaDFs.+=:(processedFile)
        println("!!!!!!! iteration in foreach !!!!!!! - " + date)
      }

      aggregateGA = gaDFs.reduceLeft((left, right) => left.union(right))

      //join with oauth to enrich with contact information
      ga_customer_enriched = oauth.join(aggregateGA, oauth("customer_registrationid") === aggregateGA("customer_id"), "inner")
        .drop("customer_registrationid")
        .filter($"customer_phone" isNotNull)
        .select("customer_id", "customer_phone", "customer_email", "customer_name", "location", "purchaseFlag")


      val ga_deduped = ga_customer_enriched.dropDuplicates(Seq("customer_id"))
      HDFSHelper.write(hdfs_connect_string, row_counts + country, ga_deduped.count().toString.getBytes, hdfs_user)

      ga_deduped
    }


    //enrichment done, don't need oauth in mem
    oauth.unpersist()

    //process GA + Ouath data for Canada & US
    //processGAData(ga_dates, canada).write.mode("overwrite").parquet(ga_temp_output_path + "can")
    //processGAData(ga_dates, us).write.mode("overwrite").parquet(ga_temp_output_path + "us")

    processGAData(ga_dates, canada).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save(ga_temp_output_path + "can")
    processGAData(ga_dates, us).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save(ga_temp_output_path + "us")


  }
}
