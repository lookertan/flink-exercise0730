package com.tanchuang.wc.state.com.tanchuang.state

import com.tanchuang.wc.sink.Sensor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlinkStateExercise {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = env.socketTextStream("hadoop102", 7777)

    val sensorStream: DataStream[Sensor] = stream.filter(_.nonEmpty).map(fun = msg => {
      val Array(id, timestamp, temprature): Array[String] = msg.split(",")
      Sensor(id.trim, timestamp.trim.toLong, temprature.trim.toDouble)
    })
    sensorStream.keyBy(_.id).process(new MyKeyedProcessFunction()).print("Sensor High Warning")
//    sensorStream.keyBy(_.id).print("Sensor High Warning")


    env.execute("State Coding")

  }
}



