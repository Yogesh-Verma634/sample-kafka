package com.self.sample.kafka.samplekafka.component;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class KafkaListener {

    @org.springframework.kafka.annotation.KafkaListener(topics = "${kafka.consumer.topic}")
    public void listener(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("Message: {}", record.value());
    }
}
