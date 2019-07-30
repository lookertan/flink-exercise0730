package com.tanchuang.flink.itemanalysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 热门商品分析，求每隔5分钟 每个小时内点击排名前 3的产品
 **/
object HotItemsAnaysis {


  def main(args: Array[String]): Unit = {
    //    1.获取flink 环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // 并行度
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 处理事件
    env.getConfig.setAutoWatermarkInterval(100)


    val props: Properties = new Properties()

    props.setProperty("group.id", "consumer-group")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")

    val topic = "HotItems"
    //    543462,1715,1464116,pv,1511658000
    /*    val kfStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), props))
        kfStream.map(msg => {
          val Array(userId, itemId, categoryId, behavior, timeStamp) = msg.split(",")
          UserBehavior(userId.toLong,itemId.toLong,categoryId.toInt,behavior,timeStamp.toLong)
        }).print()*/
    //获取的到源数据
    val fileStream: DataStream[String] = env.readTextFile("D:\\workspace\\flink-exercise\\Flink-HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //1. 将源数据转换成样例类操作
    fileStream.map(msg => {
      val Array(userId, itemId, categoryId, behavior, timestamp): Array[String] = msg.split(",")
      UserBehavior(userId.trim.toLong, itemId.trim.toLong, categoryId.trim.toInt, behavior.trim, timestamp.trim.toInt)
    })
      .assignAscendingTimestamps(_.timeStamp *1000)
      .filter(_.behavior == "pv") //过滤出点击的
      .keyBy(_.itemId) //商品Id进行分流
      .timeWindow(Time.minutes(60), Time.seconds(5))
      .aggregate(new MyAgg(), new windowProcessFunction())
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))
      .print()


    env.execute()
  }
}

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timeStamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

//按照 categoryId(产品id)分组后 计算每个Id的数量
class MyAgg extends AggregateFunction[UserBehavior, Long, Long]() {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//窗口关闭时的操作，包装成样例类
class windowProcessFunction() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val count = ItemViewCount(key, window.getEnd, input.iterator.next)
    out.collect(count)
  }
}

class TopNHotItems(size: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  private var itemState: ListState[ItemViewCount] = _


  //用来初始化itemState
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val itemStateDescri = new ListStateDescriptor("item-state", classOf[ItemViewCount])
    itemState = getRuntimeContext.getListState(itemStateDescri)
  }


  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每条数据都暂存入ListState
    itemState.add(value)
    //注册一个定时器，延迟触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
  }

  //核心处理流程，定时器触发时进行操作，可以认为之前的数据都已经到达
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val itemBuffer: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      itemBuffer.add(item)
    }

    val sortedItemBuffer: ListBuffer[ItemViewCount] = itemBuffer.sortBy(-_.count).take(size)

    val result: StringBuilder = new StringBuilder()

    result.append("==================================\n")
      .append("时间： ").append(new Timestamp(timestamp - 100)).append("\n") //增加时间

    for (i <- 0 until sortedItemBuffer.length) {
      result.append(s"No.${i + 1}: ")
        .append(" 商品ID= ").append(sortedItemBuffer(i).itemId)
        .append(" 浏览量= ").append(sortedItemBuffer(i).count).append("\n")
    }

    result.append("==================================\n")

    Thread.sleep(1000)
    out.collect(result.toString())

  }


}

