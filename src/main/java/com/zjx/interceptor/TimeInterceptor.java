package com.zjx.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.Map;

public class TimeInterceptor implements ProducerInterceptor<String, String>{
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String value = System.currentTimeMillis() + "," + record.value();
        return new ProducerRecord<String, String>(record.topic(), record.partition(), record.key(), value);
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
