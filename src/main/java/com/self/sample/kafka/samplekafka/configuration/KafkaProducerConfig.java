package com.self.sample.kafka.samplekafka.configuration;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    private static  final Logger logger = LoggerFactory.getLogger(KafkaProducerConfig.class);

    @Value("${kafka.producer.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.producer.ssl.keystore.path}")
    @NotNull
    private String sslKeystorePath;

    @Value("${kafka.producer.ssl.keystore.password}")
    @NotNull
    private String sslKeystorePassword;

    @Value("${kafka.producer.security-protocol}")
    @NotNull
    private String securityProtocol;

    @Value("${kafka.consumer.ssl.key-password}")
    @NotNull
    private String sslKeyPassword;

    @Value("${kafka.producer.requestTimeout}")
    @NotNull
    String requestTimeoutMs;

    @Value("${kafka.producer.retries-config}")
    @NotNull
    private String retriesConfig;

    @Value("${kafka.producer.acks}")
    @NotNull
    private String acknowledgmentType;

    @Value("${kafka.producer.batch-size}")
    @NotNull
    private int batchSize;

    @Bean
    public Map<String, Object> producerConfigs() throws IOException {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        props.put(ProducerConfig.RETRIES_CONFIG, retriesConfig);
        props.put(ProducerConfig.ACKS_CONFIG, acknowledgmentType);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);

        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                    createTempFilePath(sslKeystorePath, "server.keystore"));
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
        props.put(SslConfigs.SSL_PROTOCOL_CONFIG, securityProtocol);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        return props;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() throws IOException {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() throws IOException {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Create a temporary file and move the file as a stream. As, we
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
        InputStream inputStream = KafkaProducerConfig.class.getClassLoader().getResourceAsStream(sslKeystorePath);
        File serverKeystore = File.createTempFile(prefix, ".jks");
        try {
            FileUtils.copyInputStreamToFile(inputStream, serverKeystore);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        return serverKeystore.getPath();
    }


}
