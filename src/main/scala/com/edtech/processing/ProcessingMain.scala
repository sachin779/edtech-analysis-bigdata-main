package com.edtech.processing

import com.edtech.Job
import com.edtech.dimensiontable.{CampaignDimensions, CountryType, DeviceType, EventType, MarketingTeam, UserDetails, UserDimensionCountry}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

import java.util.Properties

/**
 * Entry point of the spark job
 */
object ProcessingMain  {

  var toggleFlag, source, startDate, master,recordType = ""

  def main(args:Array[String]):Unit= {

    //reading cmd line arguements

    if(args.length>1) {
      for (ar <- args) {
        println(ar)
      }
    }
    else {
      println("specify some arguements!")
    }

    toggleFlag=args(0) //test or app
    source=args(1) //AWS or LOCAL
    startDate=args(2) //start_date
    master=args(3) //loacal or yarn

    println(toggleFlag)
    println(source)
    println(master)
    //loading properties from the property file
    val props = new Properties()
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    //props.load(classOf[App].getClassLoader.getResourceAsStream("application.properties"))
    //val MASTER_CONFIG: String = props.getProperty("master_config")

    val logger = Logger.getLogger("ProcessingMain")

    val jobList: List[Job] = List(new CampaignDimensions,
      new CountryType
      /*  new DeviceType,
    new EventType,
    new MarketingTeam,
    new UserDetails,
    new UserDimensionCountry*/)


    //creating spark session object
    val spark: SparkSession = SparkSessionFactory.getOrCreateSparkSessionObject(master)

    logger.info("Spark processing started")
    jobList.foreach(x => x.execute(spark,source))
    logger.info("Spark processing completed")


  }

}
