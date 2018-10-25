package com.ibm.filtering.plantDataFilterEngine
 import com.mongodb.spark.config._
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.sql._

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._


import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{DataFrame,SQLContext, Row}
//import org.apache.spark.sql.types._


import org.apache.spark.SparkConf

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level    

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import org.apache.kafka.clients.producer._

import java.util.concurrent.Future
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.Properties;
import org.apache.kafka.common.serialization._
import org.apache.spark.broadcast._
import org.apache.spark.sql.types.{StringType, FloatType,TimestampType,StructField, StructType}
import javax.annotation.Nullable





object PlantDataFilter {
  
   private[this] val config = ConfigFactory.load()
  
  def main (args: Array[String]): Unit = {
    
    val logger = LoggerFactory.getLogger("Plant Data Filter Engine")
   // val config = ConfigFactory.parseResources("application.conf")
    val conf = new SparkConf().setMaster("local[*]").setAppName("PlantDataFilterEngine") 
    
    val sc = new StreamingContext(conf, Seconds(config.getLong("window.timeStamp")))
   // val kafkaConf = Map( "metadata.broker.list" -> config.getString("KAFKA_BROKERS"), "zookeeper.connect" -> config.getString("zookeeper.conect"), "group.id" -> "condition monitoring", "zookeeper.connection.timeout.ms" -> "1000")
 val kafkaConf = Map[String, Object](
  "bootstrap.servers" -> config.getString("kafka.KAFKA_BROKERS"),
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "PlantDataFilterEngine",
  "auto.offset.reset" -> "latest",
  "enable.auto.commit" -> (false: java.lang.Boolean)
  )

   // topic names which will be read
    val topics = Array(config.getString("kafka.INPUT_TOPIC"))

    // create kafka direct stream object
    val stream = KafkaUtils.createDirectStream[String, String](sc, PreferConsistent, Subscribe[String, String](topics, kafkaConf))
 
    val readConfig = ReadConfig(Map("uri" -> config.getString("mongo.MONGO_HOST_URL"), "database" -> config.getString("mongo.MONGO_DATABASE_NAME"), "collection" -> "TAG_GROUPSi"))
    val writeConfig = WriteConfig(Map("uri" -> config.getString("mongo.MONGO_HOST_URL"), "database" -> config.getString("mongo.MONGO_DATABASE_NAME"), "collection" -> "OPC_PLANT_DATA"))

    val opcDataTypes : List[String]= List("Boolean","Boolean Array","Byte","Byte Array∗","Short Array∗","BCD","BCD Array∗","Word","Word Array∗","Long","Long Array∗","LBCD Array∗","DWord","DWord Array∗","Float","Float Array∗","Double","Double Array∗","LLong","LLong Array","Qword","Qword Array","Char","String")
    

    stream.foreachRDD { rdd =>

  // Get the singleton instance of SparkSession
  
    val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
    val sqlContext = new SQLContext(rdd.sparkContext)
    import sqlContext.implicits._ 
    //val kafkaSink = rdd.sparkContext.broadcast(KafkaSink(Subscribe[String, String](topics, kafkaConf)))

  
    
    val raw = rdd.map(_.value.toString)
    
    
    val rows = raw.map(r => Row.fromSeq(r.split(",")))
    
    rows.foreach(row =>{
      val field1 = row(0).toString()
      val splitedfield1 = field1.split("!")
      val itemId = splitedfield1(1)
      val splitedItemID = itemId.split(".")
      val parameter = splitedItemID(1)
      val tagId = splitedItemID(0)
      val transformed = Row.fromSeq(Seq(itemId, tagId, parameter, row(1), row(2), row(3)))
      val transformedRDD = rdd.sparkContext.parallelize(List(transformed))
    
    
    try{
    val inputDF = sqlContext.createDataFrame(transformedRDD, Raw.struct)

    val plantTags = spark.read.mongo(readConfig)
    val tags = plantTags.select($"TAGS"("ITEM_ID"))
    val filteredIn = inputDF.filter($"ITEM_ID" isin tags)
    if (filteredIn.count() >= 1){
      
      val validated = filteredIn.where($"ITEM_QUALITY"==="GOOD" && $"ITEM_DATA_TYPE" isin opcDataTypes)
      val JSONMessage = validated.toJSON
      val ssc: StreamingContext = {
      val sparkConf = new SparkConf().setAppName("PlantDataFilter").setMaster("local[*]")
      new StreamingContext(sparkConf, Seconds(1))
      }

        ssc.checkpoint("checkpoint-directory")

        val kafkaProducer: Broadcast[MySparkKafkaProducer[Array[Byte], String]] = {
        val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", config.getString("kafka.bootstrap")
        p.setProperty("key.serializer", classOf[ByteArraySerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
  ssc.sparkContext.broadcast(MySparkKafkaProducer[Array[Byte], String](kafkaProducerConfig))
      }
        JSONMessage.rdd.foreachPartition { partitionOfRecords =>
    val metadata: Stream[Future[RecordMetadata]] = partitionOfRecords.map { record =>
      kafkaProducer.value.send(config.getString("kafka.OUTPUT_TOPIC"), record)
    }.toStream
    metadata.foreach { metadata => metadata.get() }
  }
        inputDF.saveToMongoDB(writeConfig)
        
      
    }else{
      logger.error("arg0")
    }
    
    }catch{ 
     case e: Exception => logger.info("exception caught: " + e);
    }
    })
    
    
    
    
    }
  
   }
}

object Raw {
      val ITEM_ID = StructField("ITEM_ID", StringType, nullable = false)
      val TAG_ID = StructField("TAG_ID", StringType , nullable = false)
      val PARAMETER = StructField("PARAMETER", StringType , nullable = false)
      val ITEM_VALUE = StructField("ITEM_VALUE", FloatType , nullable = false)
      //val ITEM_DATA_TYPE = StructField("ITEM_DATA_TYPE", StringType , nullable = false)
      val ITEM_QUALITY = StructField("ITEM_QUALITY", StringType, nullable = false)
      val ITEM_TIMESTAMP = StructField("ITEM_TIMESTAMP", TimestampType , nullable = false)


      val struct = StructType(Array(ITEM_ID, TAG_ID, PARAMETER, ITEM_VALUE, ITEM_QUALITY, ITEM_TIMESTAMP))
    }

class MySparkKafkaProducer[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {

  /* This is the key idea that allows us to work around running into
     NotSerializableExceptions. */
  lazy val producer = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))

  def send(topic: String, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))

}

object MySparkKafkaProducer {

  import scala.collection.JavaConversions._

  def apply[K, V](config: Map[String, Object]): MySparkKafkaProducer[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)

      sys.addShutdownHook {
        // Ensure that, on executor JVM shutdown, the Kafka producer sends
        // any buffered messages to Kafka before shutting down.
        producer.close()
      }

      producer
    }
    new MySparkKafkaProducer(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): MySparkKafkaProducer[K, V] = apply(config.toMap)

}


/*
 class KafkaSink {
   class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {
      lazy val producer = createProducer()
      def send(topic: String, value: String): Unit = producer.send(new     ProducerRecord(topic, value))
    }
    object KafkaSink {
      def apply(config: java.util.Map[String, Object]): KafkaSink = {
        val f = () => {
          val producer = new KafkaProducer[String, String](config)
          sys.addShutdownHook {
            producer.close()
          }
          producer
        }
        new KafkaSink(f)
      }
    }
}*/
