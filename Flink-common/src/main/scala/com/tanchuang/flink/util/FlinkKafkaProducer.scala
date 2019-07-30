package com.tanchuang.flink.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.StdIn

/**
  *1.控制台 输入信息
  * 2.将信息写入 kafka集群 topic 为 userLogin
  * 3.输入exit 则退出 系统
  */
object FlinkKafkaProducer {
  def main(args: Array[String]): Unit = {
    //kafka 的集群配置
    val topic = "userLogin"
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("auto.offset.reset", "latest")

    val producer = new KafkaProducer[String, String](props)
    var flag = true
    while(flag){
      val msg = StdIn.readLine("输入信息： ")

      if (msg == "exit"){
        flag = false
      }

      val record = new ProducerRecord[String,String](topic,msg)

      producer.send(record)

    }
  producer.close()

  }

}
