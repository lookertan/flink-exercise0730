package com.tanchuang.wc.state.com.tanchuang.state.mock

import java.text.SimpleDateFormat
import java.util.{Properties, UUID}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.shaded.akka.org.jboss.netty.logging.InternalLogLevel
import org.apache.flink.shaded.netty4.io.netty.util.internal.logging.{InternalLogger, InternalLoggerFactory, Log4JLoggerFactory}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

import scala.util.Random

/**
 * 1. 主要模拟点击事件
 * 2. 产生的数据的格式内容
 * 3. 时间戳,mid,userid,url
 * 4. 数据发送给kafka
 */

object MockClickEvent {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = env.addSource(new MySourceProducerFunction())
  val topicId = "flink0804"
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    stream.print("模拟数据")
    stream.addSink(new FlinkKafkaProducer[String](topicId,new SimpleStringSchema(),props))

    env.execute()

  }
}

class MySourceProducerFunction() extends SourceFunction[String] {
  private var flag = true

  //3. 时间戳,mid,userid,url
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

    val random = new Random()
    //产生用户基数
    val userVisitLog: List[String] = (1 to 50).map(i =>
      UUID.randomUUID() + "," + i
    ).toList

    while (flag) {
      val time: Long = System.currentTimeMillis()
      val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      ctx.collect(formatter.format(time + random.nextInt(2000)) + "," + userVisitLog(random.nextInt(50)) + ", http://" + UUID.randomUUID())
      Thread.sleep(500)
    }
  }


  override def cancel(): Unit = flag = false
}