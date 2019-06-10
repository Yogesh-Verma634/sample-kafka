package com.self.sample.kafka.samplekafka.configuration;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${kafka.consumer.bootstrap-servers}")
    @NotNull
    private String bootstrapServers;

    @Value("${kafka.consumer.ssl.keystore.path}")
    @NotNull
    private String sslKeystorePath;

    @Value("${kafka.consumer.ssl.keystore.password}")
    @NotNull
    private String sslKeystorePassword;

    @Value("${kafka.consumer.security-protocol}")
    @NotNull
    private String securityProtocol;

    @Value("${kafka.consumer.ssl.key-password}")
    @NotNull
    private String sslKeyPassword;

    @Value("${kafka.consumer.groupId}")
    @NotNull
    String groupId;

    @Value("${kafka.consumer.concurrency}")
    @NotNull
    private int concurrency;

    @Value("${kafka.consumer.poll-time}")
    @NotNull
    private int polltime;

    @Value("${kafka.consumer.max.poll.records}")
    @NotNull
    private String maxPollRecords;

    @Value("${kafka.consumer.max.poll.interval}")
    @NotNull
    private String maxPollIntervalInMilliSeconds;

    @Value("${kafka.consumer.auto-reset-config}")
    @NotNull
    private String autoResetConfig;

    @Value("${kafka.consumer.max.partition.fetchBytes}")
    @NotNull
    private String maxPartitionFetchBytes;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() throws IOException {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoResetConfig);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalInMilliSeconds);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);

        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, securityProtocol);
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, createTempFilePath(sslKeystorePath, "server.keystore"));
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() throws IOException {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setPollTimeout(polltime);
        factory.setBatchListener(true);
        return factory;
    }

    /**
     * KLUDGE: Create a temporary file and move the file as a stream. As, we
     * cannot retrieve a file inside a nested jar of Spring boot (Executable
     * Jar). We create a temporary file and move the server.keystore and
     * client.truststore contents as a stream.
     * <p>
     * This would be fixed once we move the keystore files to a SSL store
     * location.
     *
     * @param sslKeystorePath
     * @param prefix
     * @return
     * @throws IOException
     */
    private String createTempFilePath(String sslKeystorePath, String prefix) throws IOException {
        InputStream inputStream = KafkaConsumerConfig.class.getClassLoader().getResourceAsStream(sslKeystorePath);
        File serverKeystore = File.createTempFile(prefix, ".jks");
        try {
            FileUtils.copyInputStreamToFile(inputStream, serverKeystore);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        return serverKeystore.getPath();
    }
}
