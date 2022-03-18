package scala.spark.streaming.demo

import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._


object SparkStreamDemo extends App{
    val sparkSession = SparkSession
                       .builder()
                       .appName("Spark Streaming Demo")
                       .master("local[4]")
                       .config("spark.driver.allowMultipleContexts",true)
                       .getOrCreate()
  val path = $your_local_path

  ///////spark core app////////
/*
  import sparkSession.implicits._
  println("why printing!!!!!")
  val rddDF = sparkSession.sparkContext.textFile(path).toDF("numbers")
  //rddDF.show()
  val rddSum = rddDF
    .agg(sum("numbers")
      .alias("number_cnt"))
  rddSum.show()
*/

  ////////spark streaming app//////////
  //val conf = new SparkConf().setAppName("Spark Streaming Demo").setMaster("local[4]")

  val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(10))

  val streams = streamingContext.textFileStream(path)
  println("Reading each RDD")
  streams.foreachRDD(x => x.collect().foreach(x => println("Length: "+ x.length)))
  println("Reading each RDD Done!")

  streams.foreachRDD{
    rdd =>
      println("Rdd length: " + rdd.collect.length)
      //val spark =  SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      println("Spark Session Created!!!")

      import sparkSession.implicits._
      println("RDD content")
      rdd.foreach(println)
      val rddDF = rdd.toDF("numbers")
      //rddDF.show()
      val rddSum = rddDF
        .agg(sum("numbers")
          .alias("number_cnt"))
        .selectExpr("case when number_cnt is null then 0 else number_cnt end as number_cnt")
      rddSum.show()

  }
  streamingContext.start()
  streamingContext.awaitTermination()
}
