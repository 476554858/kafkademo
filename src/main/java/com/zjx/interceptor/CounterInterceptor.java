package com.zjx.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor<String, String> {
    int seccess = 0;
    int error = 0;
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(recordMetadata != null){
            seccess++;
        }else {
            error++;
        }
    }

    @Override
    public void close() {
        System.out.println("success:" + seccess);
        System.out.println("error:" + error);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
