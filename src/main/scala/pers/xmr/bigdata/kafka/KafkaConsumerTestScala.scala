package pers.xmr.bigdata.kafka
import java.util
import java.util.{Arrays, Properties}
import scala.collection.JavaConversions._

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

/**
  * @author xmr
  * @date 2019/4/28 13:55
  * @description kafka消费端程序
  */
object KafkaConsumerTestScala extends Thread{
  def main(args: Array[String]): Unit = {

    start()
  }

  override def run() {
    val properties = new Properties()
    properties.put("bootstrap.servers", "10.213.32.96:9092,10.213.32.97:9092,10.213.32.98:9092")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("group.id", "g2")
    // 创建消费端对象
    val  kafkaConsumer = new KafkaConsumer[String,String](properties)
    // 订阅主题
    kafkaConsumer.subscribe(util.Arrays.asList("posp_trade_time"))
    // 消息轮询,消费端的核心
    while (true) {
      // 持续进行轮询,返回记录列表, 传递的参数是超时时间
      val records: ConsumerRecords[String, String] = kafkaConsumer.poll(1000)
      for (record <- records) {
        System.out.println("消费者消费到数据: " + record)
      }
      // 提交最后一个返回的偏移量
      kafkaConsumer.commitAsync()
    }
  }
}

