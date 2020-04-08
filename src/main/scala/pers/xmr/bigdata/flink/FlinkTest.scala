package pers.xmr.bigdata.flink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * @author xmr
  * @date 2019/11/20 16:09
  * @description
  */
class FlinkTest {
  def main(args: Array[String]): Unit = {
    // 1. env 2. source 3.transform  4. sink(用于调整state)
    val env : ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val txtDataSet : DataSet[String] = env.readTextFile("D:\\video")
    val aggSet: AggregateDataSet[(String,Int)] = txtDataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1) // 这里会提示(...)的问题,需要进行隐式转换
    aggSet.print()
  }
}
