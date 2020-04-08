package pers.xmr.bigdata.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author xmr
 * @date 2019/4/28 13:56
 * @description Kafka消费端(Java版)
 */
public class KafkaConsumerTestJava extends Thread{
    public static void main(String[] args) {
        KafkaConsumerTestJava kafkaConsumerTestJava = new KafkaConsumerTestJava();
        kafkaConsumerTestJava.start();
    }
    @Override
    public void run() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.213.32.96:9092,10.213.32.97:9092,10.213.32.98:9092");
        properties.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "g2");
        // 创建消费端对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        // 订阅主题
        kafkaConsumer.subscribe(Arrays.asList("test"));
        // 消息轮询,消费端的核心
        while (true) {
            // 持续进行轮询,返回记录列表, 传递的参数是超时时间
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            // 对获取到的记录进行处理
            for(final ConsumerRecord record: records) {
                System.out.println("消费者消费到数据: " + record);
            }
            // 提交最后一个返回的偏移量
            kafkaConsumer.commitAsync();
        }

    }
}

