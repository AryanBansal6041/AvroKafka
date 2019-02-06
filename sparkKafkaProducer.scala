package BasicTasks.sparkStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.kafka.clients.producer.{Producer, ProducerConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.kafka.clients.producer._
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object sparkKafkaProducer {
  def main(args: Array[String]) {
 //  val data= args(0)

    val data="C:\\Work\\datasets\\poc"  // path to take data
    val topic: String = if (args.length > 0) args(0) else "toda"
    val spark = SparkSession.builder.master("local[*]").appName("structurestreaming").config("spark.sql.streaming.checkpointLocation","file:///home/hadoop/work/datasets/checkpoint").getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

  //  val data = "/home/hadoop/Desktop/asldata/"
    //   val data = args(0)
    //  val data = "/home/hadoop/work/datasets/asldata/asl*.csv" // to read use either java scala python, or spark
    val my = sc.textFile(data)
    /* val head = my.first()
     val finalrdd = my.filter(x=>x!=head)*/


    my.foreachPartition(rdd => {
      import java.util._

      val props = new java.util.Properties()
      //  props.put("metadata.broker.list", "localhost:9092")
      //      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("bootstrap.servers", "localhost:9092")



      import kafka.producer._
      // val config = new ProducerConfig(props)
      val producer = new KafkaProducer[String, String](props)

      rdd.foreach(x => {
        println(x)
        producer.send(new ProducerRecord[String, String](topic.toString(), x.toString)) //
        Thread.sleep(5000)
        //(lti, name,age,city)
        //(lti,venu,30,hyd)
      })

    })

  }
}