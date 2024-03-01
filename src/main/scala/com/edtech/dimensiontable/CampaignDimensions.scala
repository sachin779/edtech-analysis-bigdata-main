package com.edtech.dimensiontable

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import com.edtech.Job
import com.edtech.processing.SparkSessionFactory
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SaveMode

import scala.sys.process.ProcessBuilder.Source

class CampaignDimensions extends Job {

  def execute(spark: SparkSession,source: String): Unit = {

    var outputLocation = ""
    var inputLocation = ""
    var outputLocation_writeloc = ""
    //var spark: SparkSession = _

    source match {
      case "LOCAL" =>
        inputLocation=  "C:\\Users\\Sachin\\IdeaProjects\\edtech-analysis-bigdata-main\\dataset\\user_event_details\\part-00000-8fcaf9df-99a9-417a-890f-6dd8df111e21-c000.csv"
        outputLocation= "C:\\Users\\Sachin\\IdeaProjects\\edtech-analysis-bigdata-main\\output\\dim_tables\\dim_marketing_team_1\\dim_marketing_team_1.csv"
        outputLocation_writeloc= "C:\\Users\\Sachin\\IdeaProjects\\edtech-analysis-bigdata-main\\output\\dim_tables\\dim_marketing_team_1\\"
        val spark: SparkSession = SparkSessionFactory.getOrCreateSparkSessionObject("local[*]")
  

      val campaign_details_Schema = StructType(List(StructField("campaign_id", IntegerType),
        StructField("course_campaign_name", StringType),
        StructField("user_id", IntegerType),
        StructField("user_name", StringType),
        StructField("campaign_date", DateType),
        StructField("digital_marketing_team", StringType),
        StructField("event_status", IntegerType),
        StructField("event_type", StringType),
        StructField("marketing_product", StringType),
        StructField("user_response_time", IntegerType)));
      val df1 = spark.read.format("csv")
        .schema(campaign_details_Schema)
        .option("header", true)
        .option("path", inputLocation)
        .load()


      df1.createOrReplaceTempView("userEventDetails");

      /**
       * Populating dim marketing teams details
       */
      val dimMarketingType = spark.read.format("csv")
        .option("header", "true")
        .option("path", outputLocation)
        .load()
        dimMarketingType.printSchema()
      dimMarketingType.createOrReplaceTempView("dim_marketing_team")

      def splitAndReturn(name: String) = {
        val fields = name.split("_")
        fields(1)

      }

      spark.udf.register("getTheString", splitAndReturn(_: String): String);

      val marketingCount = dimMarketingType.count();

      if (marketingCount > 0) {

        val marketingDf = spark.sql(
          """select
          (select max(marketing_key) from dim_marketing_team)+ row_number() over(order by digital_marketing_team) as marketing_key,
          UPPER(digital_marketing_team) as digital_marketing_team, UPPER(getTheString(dig_marketing_team)) as marketing_place from
           userEventDetails where UPPER(digital_marketing_team) not in(select digital_marketing_team from dim_marketing_team)
            order by digital_marketing_team""")


        marketingDf.coalesce(1).write
          .format("csv")
          .option("header","true")
          .option("path", outputLocation_writeloc)
          .mode(SaveMode.Overwrite)
          .save()

      }
      else {

        val marketingDf = spark.sql(
          """select row_number() over(order by dig_marketing_team) as marketing_key,
                                UPPER(dig_marketing_team) as digital_marketing_team ,
                                UPPER(getTheString(dig_marketing_team)) as marketing_place 
                                from
                                  (select distinct(digital_marketing_team) as dig_marketing_team from userEventDetails order by dig_marketing_team asc)""")


        marketingDf.coalesce(1).write
          .format("csv")
          .option("header","true")
          .option("path", outputLocation_writeloc)
          .mode(SaveMode.Overwrite)
          .save()

      }

      case "AWS" =>
        inputLocation=  "awsbucket"
        outputLocation= "awsbucket"
        val spark: SparkSession = SparkSessionFactory.getOrCreateSparkSessionObject("yarn")

        val bucket = "edtech_analytics_dump"
        spark.conf.set("temporaryGcsBucket", bucket)


        val campaign_details_Schema = StructType(List(StructField("campaign_id", IntegerType),
          StructField("course_campaign_name", StringType),
          StructField("user_id", IntegerType),
          StructField("user_name", StringType),
          StructField("campaign_date", DateType),
          StructField("digital_marketing_team", StringType),
          StructField("event_status", IntegerType),
          StructField("event_type", StringType),
          StructField("marketing_product", StringType),
          StructField("user_response_time", IntegerType)));
        val df1 = (spark.read.format("csv")
          .schema(campaign_details_Schema)
          .option("header", true)
          .option("path", "gs://edtech_analytics_dump/data/bq-results.csv")
          .load())


        df1.createOrReplaceTempView("userEventDetails");

        /**
         * Populating dim marketing teams details
         */
        val dimMarketingType = (spark.read.format("bigquery")
          .option("table", "dimension_tables.dim_marketing_team_1")
          .load())

        dimMarketingType.createOrReplaceTempView("dim_marketing_team")

        def splitAndReturn(name: String) = {
          val fields = name.split("_")
          fields(1)

        }

        spark.udf.register("getTheString", splitAndReturn(_: String): String);

        val marketingCount = dimMarketingType.count();

        if (marketingCount > 0) {

          val marketingDf = spark.sql(
            """select
          (select max(marketing_key) from dim_marketing_team)+ row_number() over(order by digital_marketing_team) as marketing_key,
          UPPER(digital_marketing_team) as digital_marketing_team, UPPER(getTheString(dig_marketing_team)) as marketing_place from
           userEventDetails where UPPER(digital_marketing_team) not in(select digital_marketing_team from dim_marketing_team)
            order by digital_marketing_team""")


          (marketingDf.write.format("bigquery")
            .option("table", "dimension_tables.dim_marketing_team_1")
            .mode(SaveMode.Append)
            .save())


        } else {

          val marketingDf = spark.sql(
            """select row_number() over(order by dig_marketing_team) as marketing_key,
                                UPPER(dig_marketing_team) as digital_marketing_team ,
                                UPPER(getTheString(dig_marketing_team)) as marketing_place
                                from
                                  (select distinct(digital_marketing_team) as dig_marketing_team from userEventDetails order by dig_marketing_team asc)""")


          (marketingDf.write.format("bigquery")
            .option("table", "dimension_tables.dim_marketing_team_1")
            .mode(SaveMode.Append)
            .save())

        }
    }



  }

}