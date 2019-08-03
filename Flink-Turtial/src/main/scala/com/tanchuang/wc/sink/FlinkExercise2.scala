package com.tanchuang.wc.sink

import java.util

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
//import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import scala.util.Random

//source

/**
 * 1.自定义类从 MYSQL 中获取数据
 */
object FlinkExercise2 {
  def main(args: Array[String]): Unit = {

    //自定义类从指定数据源获取数据
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //从自定义类中获取到数据
//    val sourceStream: DataStream[String] = env.readTextFile("D:\\workspace\\flink-exercise\\Flink-Turtial\\src\\main\\resources\\sensor.txt")
    val sourceStream: DataStream[String] = env.addSource(new MySourceFunction())

    //HttpHost端口号
    val hosts = new util.ArrayList[HttpHost]()
    hosts.add(new HttpHost("hadoop102", 9200))

    //elaSink写操作
    val elaBuilder = new ElasticsearchSink.Builder[Sensor](hosts,
      //具体写数据操作
      new ElasticsearchSinkFunction[Sensor] {

        override def process(t: Sensor, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {

          println(t + " 正在写入Ela")
//case class Sensor(id: String, timeStamp: Long, temprature: Double)
          val result = new util.HashMap[String, String]()
          result.put("id", t.id.toString)
          result.put("timeStamp", t.timeStamp.toString)
          result.put("temprature", t.temprature.toString)

          val indexRequest= Requests.indexRequest().index("sensor0802").`type`("readingData").source(result)

          requestIndexer.add(indexRequest)

          println("数据写入")
        }
      })
    elaBuilder.setBulkFlushMaxActions(100)
    //将指定类进行转换过
//    sensor_10, 1547718216, 32.101067604893124
    sourceStream.map(str => {
      val Array(id, timeStamp, temprature): Array[String] = str.split(",")
      Sensor(id.trim, timeStamp.trim.toLong, temprature.trim.toDouble)
    })
      .addSink(elaBuilder.build())


    env.execute()


  }
}

class MySourceFunction() extends SourceFunction[String]() {
  private var flag = true
  private val random = new Random()

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    //传感器基准数据
    val sensorSourceData: List[(String, Double)] = 1.to(50).map(
      i => (i.toString, 60 + random.nextGaussian() * 20)
    ).toList

    while (flag) {
      //随机选择一个 root ntpdate time.nuri.net
      val i: Int = Math.abs(random.nextInt()) % 50
      val sensorData: (String, Double) = sensorSourceData(i)
      val timeStamp: Long = System.currentTimeMillis()
      ctx.collect(s"${sensorData._1},$timeStamp,${sensorData._2 + random.nextDouble()}")
      Thread.sleep(300)
    }
  }

  //手动关闭 需要指定
  override def cancel(): Unit = {
    flag = false
  }

}

case class Sensor(id: String, timeStamp: Long, temprature: Double)

class MyMap() extends RichMapFunction[Sensor, String] {
  //再调用map 方法之前会调用的方法
  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def map(value: Sensor) = {
    "转换成字符串： " + value.toString
  }

  //再调用map 方法之后调用的方法
  override def close(): Unit = super.close()
}

class MyFLatMap() extends RichFlatMapFunction[String, String] {
  override def flatMap(value: String, out: Collector[String]): Unit = {
    out.collect(value)
  }
}



