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
    val date_list_txt = "/tmp/adam-count" + timestamp + ".txt"
    val hdfs_user = "adam"
    val hdfs_connect_string = "hdfs://labshdpds2"
    val ga_dates: Array[String] = Array("dateday=20170116", "dateday=20170117", "dateday=20170118")
    //val ga_dates : Array[String] = HDFSHelper.getFileListFromDirectory(hdfs_connect_string, ga_path, hdfs_user)

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = SparkSession
      .builder()
      .appName("lacyrymose")
      .getOrCreate()

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
      println("!!!!!!! aggregateGA count:  " + aggregateGA.count().toString)

      //join with oauth to enrich with contact information
      ga_customer_enriched = oauth.join(aggregateGA, oauth("customer_registrationid") === aggregateGA("customer_id"), "inner")
        .drop("customer_registrationid")
        .filter($"customer_phone" isNotNull)
        .select("customer_id", "customer_phone", "customer_email", "customer_name", "location", "purchaseFlag")

      //ga_customer_enriched.dropDuplicates(Seq("customer_id"))
      ga_customer_enriched
    }


    //enrichment done, don't need oauth in mem
    oauth.unpersist()

    //process GA + Ouath data for Canada & US
    //processGAData(ga_dates, canada).coalesce(1).write.mode("overwrite").parquet(ga_temp_output_path + canada)
    //processGAData(ga_dates, us).coalesce(1).write.mode("overwrite").parquet(ga_temp_output_path + us)

    processGAData(ga_dates, canada).write.format("com.databricks.spark.csv").option("header", "true").save(ga_temp_output_path + canada + ".csv")
    processGAData(ga_dates, us).write.format("com.databricks.spark.csv").option("header", "true").save(ga_temp_output_path + us + ".csv")


    HDFSHelper.write(hdfs_connect_string, date_list_txt, ga_dates.mkString("\n").getBytes, hdfs_user)

  }
}
