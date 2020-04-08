package pers.xmr.bigdata.flink

import org.apache.flink.streaming.api.scala
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * @author xmr
  * @date 2019/11/20 16:24
  * @description
  */
class DataStreamFlink {
  def main(args: Array[String]): Unit = {
    val env : scala.StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val unit: DataStream[String] = env.socketTextStream("linux01", 7777)

  }
}
