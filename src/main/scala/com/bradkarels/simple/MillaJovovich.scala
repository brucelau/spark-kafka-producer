package com.bradkarels.simple

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import kafka.producer.KeyedMessage
import kafka.producer.Producer
import kafka.producer.ProducerConfig

class SparkFuEvent(
  val year: Int,
  val month: Int,
  val day: Int,
  val hour: Int,
  val minute: Int,
  val second: Int,
  val millis: Int,
  val msg: String // What happened yo!?
)

/**
 * It seems appropriate that MillaJovovich.scala would contain object Messenger.  Nerd alert!
 */
object Messenger {
  
  /**
   * In this example we will create a few simple messages to be sent to Kafka.
   * *** Next: Read data from file system.
   * *** Then: Read data from HDFS. (?)
   * *** Then: Read data from Cassandra
   * (more or less, you could do this from any Hadoop compatible source.
   */
  def main(args: Array[String]) {
	val conf = new SparkConf().setAppName("Spark Fu Kafka Producer")
	val sc = new SparkContext(conf)
	
	val props:Properties = new Properties()
        props.put("metadata.broker.list", "localhost:9092")
        props.put("serializer.class", "kafka.serializer.StringEncoder")
 
    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    // Local ZooKeeper Start
    //[bkarels@rev27 kafka_2.10-0.8.1.1]$ bin/zookeeper-server-start.sh config/zookeeper.properties &
    
    // Local Kafka Start
    //[bkarels@rev27 kafka_2.10-0.8.1.1]$ bin/kafka-server-start.sh config/server.properties &
    //[bkarels@rev27 kafka_2.10-0.8.1.1]$ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sparkfu & 
    
    // Local cosumer to view messages:
    //[bkarels@rev27 kafka_2.10-0.8.1.1]$ bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic sparkfu --from-beginning
    
    val evt0 = new SparkFuEvent(year=2015, month=1, day=20, hour=9, minute=32, second=27, millis=666, msg="What the foo?")
    val evt1 = new SparkFuEvent(year=2015, month=1, day=20, hour=9, minute=32, second=27, millis=667, msg="What the bar?")
    val evt2 = new SparkFuEvent(year=2015, month=1, day=20, hour=9, minute=32, second=27, millis=668, msg="What the baz?")
    val evt3 = new SparkFuEvent(year=2015, month=1, day=20, hour=9, minute=32, second=27, millis=669, msg="No! No! NO! What the FU!")
	
	val evts:List[SparkFuEvent] = evt0 :: evt1 :: evt2 :: evt3 :: Nil
	
	for (evt:SparkFuEvent <- evts) {
	  producer.send(new KeyedMessage[String, String]("sparkfu", evt.msg))
	}
  }
}