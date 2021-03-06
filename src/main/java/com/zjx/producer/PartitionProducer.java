package com.zjx.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class PartitionProducer {

    public static void main(String[] args) throws Exception{
        //1.创建Kafka生产者的配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.zjx.partitioner.MyPartitioner");

        //创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //发送数据
        for(int i = 0; i < 10; i++){
            producer.send(new ProducerRecord<String, String>("second", "zjx", "zjx" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        System.out.println(recordMetadata.partition() + "---" + recordMetadata.offset());
                    }else {
                        e.printStackTrace();
                    }
                }
            }).get(); //kafka发消息本身是异步的,调用这个GET方法，可以变成 同步
        }
        //关闭资源
        producer.close();
    }
}
