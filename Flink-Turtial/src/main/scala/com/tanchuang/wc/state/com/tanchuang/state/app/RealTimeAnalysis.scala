package com.tanchuang.wc.state.com.tanchuang.state.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Properties

import com.tanchuang.wc.state.com.tanchuang.state.bean.UserVisitLog
import com.tanchuang.wc.state.com.tanchuang.state.util.ConfigurationUtil
import org.apache.flink.api.common.functions.{AggregateFunction, RuntimeContext}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.aggregation.AggregationFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation

//1.需求计算 每隔 5秒 1小时内 user 出现的TOPN
object RealTimeAnalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val topic: String = ConfigurationUtil.getPropery("kafka.topic")
    val props = new Properties()
    props.setProperty("bootstrap.servers", ConfigurationUtil.getPropery("kafka.server"))
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val kfStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), props))

    kfStream.map(msg => {
      val Array(timestamp, mid, uid, url): Array[String] = msg.split(",")
      UserVisitLog(formatter.parse(timestamp.trim).getTime, mid, uid, url)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserVisitLog](Time.milliseconds(2000)) {
        override def extractTimestamp(element: UserVisitLog): Long = element.timeStamp
      })
      .keyBy(_.mid)
      .timeWindow(Time.minutes(60), Time.seconds(5))
      .aggregate(new MyAggregation(), new MyWindowProcessFunction())
      .map(outputString => {
        val Array(windowEnd, key, time): Array[String] = outputString.split(",")
        WindowKeyTime(windowEnd.trim.toLong, key.trim, time.trim.toLong)
      }
      )
      .keyBy(_.windowEnd)
        .process(new MyProcessFunction2TopN(3))
        .print()

    env.execute()


  }
}


class MyAggregation() extends AggregateFunction[UserVisitLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserVisitLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class MyWindowProcessFunction() extends ProcessWindowFunction[Long, String, String, TimeWindow] {

  override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {

    out.collect(context.window.getEnd + "," + key + "," + elements.toIterator.next)
  }
}

case class WindowKeyTime(windowEnd: Long, key: String, loginTime: Long)

class MyProcessFunction2TopN(size: Int) extends KeyedProcessFunction[Long, WindowKeyTime, String] {
  private lazy val windowKeyTimeListState: ListState[WindowKeyTime] = getRuntimeContext.getListState[WindowKeyTime](new ListStateDescriptor[WindowKeyTime]("top3", classOf[WindowKeyTime]))



  override def close(): Unit = {
    super.close()
    windowKeyTimeListState.clear()
  }

  override def processElement(value: WindowKeyTime, ctx: KeyedProcessFunction[Long, WindowKeyTime, String]#Context, out: Collector[String]): Unit = {
    //来一个元素将元素存起来
    windowKeyTimeListState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, WindowKeyTime, String]#OnTimerContext, out: Collector[String]): Unit = {
    val windowKeyTimeList: lang.Iterable[WindowKeyTime] = windowKeyTimeListState.get()
    import collection.JavaConversions._
    val times: List[WindowKeyTime] = windowKeyTimeList.toList.sortBy(-_.loginTime).take(3)
    val record = new StringBuilder()
    record.append("==================================\n")
    for (i <- times.indices) {
      record.append(s"No.${i + 1}").append(times(i)).append("\n")
    }
    record.append("==================================\n")
    out.collect(record.toString())
  }
}
/*


*/
