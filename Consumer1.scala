package Kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.databricks.spark.avro._
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.avro.Schema

object Consumer1 {

  def main(args: Array[String]) {
    import scala.util.Try
    val path="C:\\tmp\\hive\\AvroData"
    val topics =  Try(args(0).toString).getOrElse("today")
    val brokers = Try(args(1).toString).getOrElse("localhost:9092")
    val topicSet = topics.split(",").toSet
    val kakfkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val conf = new SparkConf().setAppName("cassandraSpark").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(20))
    val sc = ssc.sparkContext

    val session = SparkSession.builder.master("local[*]").appName("myapp").getOrCreate()

    // Load streaming data
    import session.implicits._
    import kafka.serializer.StringDecoder
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kakfkaParams, topicSet)
    lines.foreachRDD { rdd =>
      import org.apache.spark.sql.SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      import org.apache.avro.generic.GenericRecord
      val rdd1 = rdd.map(x=>x._2)
      val df = spark.read.json(rdd1).toDF()
      df.createOrReplaceTempView("tab")
      val x = spark.sql("select * from tab")
      x.show()
     // x.printSchema()
      x.write.mode(SaveMode.Append).orc(path)

    }
    ssc.start
    ssc.awaitTermination()

  }
}