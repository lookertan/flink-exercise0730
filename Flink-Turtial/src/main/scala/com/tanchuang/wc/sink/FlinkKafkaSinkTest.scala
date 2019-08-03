package com.tanchuang.wc.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer


object FlinkKafkaSinkTest{

  def main(args: Array[String]): Unit = {
    val topic = "testKafka"
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val source: DataStream[String] = env.addSource(new MySourceFunction())

    source.map(line=> {
        val Array(id,timestamp,temprature) = line.split(",")
          Sensor(id,timestamp.trim.toLong,temprature.trim.toDouble).toString
    }).addSink(new FlinkKafkaProducer[String](topic,new SimpleStringSchema(),props))





    env.execute()
  }


}

