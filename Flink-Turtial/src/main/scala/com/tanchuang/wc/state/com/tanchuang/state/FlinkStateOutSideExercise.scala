package com.tanchuang.wc.state.com.tanchuang.state

import com.tanchuang.wc.sink.Sensor
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object FlinkStateOutSideExercise {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sourceStream: DataStream[String] = env.socketTextStream("hadoop102", 7777)
    //没有进行严格的过滤，正常情况脏数据很少
    sourceStream.filter(_.nonEmpty).map(msg => {
      val Array(id, timestamp, temperature): Array[String] = msg.split(",")
      Sensor(id.trim, timestamp.trim.toLong, temperature.trim.toDouble)
    })
            //      .assignAscendingTimestamps(_.timeStamp*1000)
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Sensor] {
        var ts: Long = _

        override def getCurrentWatermark: Watermark = new Watermark(ts)

        override def extractTimestamp(element: Sensor, previousElementTimestamp: Long): Long = {
          ts = element.timeStamp * 1000
          element.timeStamp * 1000
        }
      })
      .keyBy(_.id)
      .process(new MyKeyedProcessFunction)
      .print()

/*      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.milliseconds(100)) {
        override def extractTimestamp(element: Sensor): Long = {
          element.timeStamp *1000
        }
      })*/

    //    val sideOutputStream: DataStream[String] = value.getSideOutput(new OutputTag[String]("freeze point warning "))
    //    sideOutputStream.print("freeze")

    //    val coStream: ConnectedStreams[String, String] = sideOutputStream.connect(value)


    env.execute()


  }

}

class MyProcessFunction() extends ProcessFunction[Sensor, String] {
  override def processElement(value: Sensor, ctx: ProcessFunction[Sensor, String]#Context, out: Collector[String]): Unit = {
    if (value.temprature < 32.0) {
      val outputTag = new OutputTag[String]("freeze point warning ")
      ctx.output(outputTag, value.toString)
    } else {
      out.collect(value.toString)
    }
  }
}

//定时输出 如果1秒的温度时持续上升的就报警
class MyKeyedProcessFunction() extends KeyedProcessFunction[String, Sensor, String] {
  //存储温度状态
  lazy val temperature: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("temperature", Types.of[Double]))
  //存储定时器状态
  lazy val timer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", Types.of[Long]))

  override def processElement(value: Sensor, ctx: KeyedProcessFunction[String, Sensor, String]#Context, out: Collector[String]): Unit = {
    val temp: Double = temperature.value()
    val timerTime: Long = timer.value()
    if (value.temprature > temp && timerTime == 0L) {
      //      val timerTime: Long = value.timeStamp*1000+100L
      val timerTime: Long = value.timeStamp*1000 + 2000L
      //      ctx.timerService().registerEventTimeTimer(timerTime)
      ctx.timerService().registerEventTimeTimer(timerTime)
      timer.update(timerTime)
      println("1111")
      temperature.update(value.temprature)
    } else if (value.temprature <= temp) {
      //      ctx.timerService().deleteEventTimeTimer(timer.value())
      ctx.timerService().deleteEventTimeTimer(timer.value())
      temperature.update(value.temprature)
      timer.clear()
    }

    println(value + "" + ctx.timerService().currentWatermark() + ": " + ctx.timerService().currentProcessingTime())
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor, String]#OnTimerContext, out: Collector[String]): Unit = {
    println("定时时间到" + timestamp + ctx.timeDomain() + ctx.timerService().currentWatermark())
    out.collect("连续升温报警" + ctx.getCurrentKey)
    temperature.clear()
    timer.clear()
  }
}
