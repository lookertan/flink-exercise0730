package com.tanchuang.flink.hello

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * 1.启动flink enviorment
 * 2.获取到kafka 中的流
 * 3. 对流中的单词计数
 */
object HelloFlink {
  def main(args: Array[String]): Unit = {
    val props: Properties = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.setProperty("group.id", "consumer-group")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")

     val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val topic = "userLogin"

    val kfStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),props))

    kfStream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1).print()

    env.execute()

  }
}
