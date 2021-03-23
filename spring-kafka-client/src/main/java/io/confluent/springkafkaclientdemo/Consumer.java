package io.confluent.springkafkaclientdemo;

import org.apache.kafka.clients.producer.Producer;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

// @Service
public class Consumer {

    ///@KafkaListener(topics = "t1_MK", groupId = "SpringConsumer2")
    public void consume(String message) throws IOException {
        System.out.println( String.format("#### -> Consumed message -> %s", message) );
    }

}