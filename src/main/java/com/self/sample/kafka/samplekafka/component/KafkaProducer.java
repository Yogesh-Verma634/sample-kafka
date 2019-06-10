package com.self.sample.kafka.samplekafka.component;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.CountDownLatch;

@Log4j2
@Component
public class KafkaProducer {

    KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.consumer.topic}")
    String kafkaTopic;

    public void produceMessages(){
        CountDownLatch latch = new CountDownLatch(1);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(kafkaTopic, "1", "Hello World");
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(producerRecord);
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info("Message Emitted Success. Partition - {}, Offset - {}, Value - {}",
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable ex) {
                log.info("Message Emitted Failed. The exception is {}",
                        ex.getMessage(),
                        ex);
                latch.countDown();
            }
        });
    }
}
