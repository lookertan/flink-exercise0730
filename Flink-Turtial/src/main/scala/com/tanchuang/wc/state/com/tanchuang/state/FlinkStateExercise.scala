package com.tanchuang.wc.state.com.tanchuang.state

import com.tanchuang.wc.sink.Sensor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlinkStateExercise {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = env.socketTextStream("hadoop102",7777)

    val sensorStream: DataStream[Sensor] = stream.map(fun = msg => {
      val Array(id, timestamp, temprature): Array[String] = msg.split(",")
      Sensor(id.trim, timestamp.trim.toLong, temprature.trim.toDouble)
    })
    sensorStream.keyBy(_.id).process(new MyKeyedProcessFunction())



    env.execute("State Coding")

  }
}


//定时输出 如果1秒的温度时持续上升的就报警
class MyKeyedProcessFunction() extends KeyedProcessFunction[String,Sensor,String]{
  //存储温度状态
  lazy private val temperature: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("temperature",classOf[Double]))
  //存储定时器状态
  lazy private val timer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer",classOf[Long]))

  override def processElement(value: Sensor, ctx: KeyedProcessFunction[String, Sensor, String]#Context, out: Collector[String]): Unit = {
    val temp: Double = temperature.value()
    if(value.temprature > temp && timer == 0L){
      ctx.timerService().registerProcessingTimeTimer(value.timeStamp+2000)
      timer.update(value.timeStamp + 1000)
      temperature.update(value.temprature)
    }else if (value.temprature < temp){
      ctx.timerService().deleteProcessingTimeTimer(timer.value())
      temperature.update(value.temprature)
        timer.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor, String]#OnTimerContext, out: Collector[String]): Unit = {

    out.collect("连续升温报警"+ ctx.getCurrentKey)
    temperature.clear()
    timer.clear()
  }
}

