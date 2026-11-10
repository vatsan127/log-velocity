package com.github.logvelocity.config;

import com.github.logvelocity.appender.KafkaAppenderInstaller;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.Map;

/**
 * Wires the log producer and the appender installer. Active only when {@code log-velocity.enabled=true}.
 *
 * @author vatsan127@gmail.com
 */
@AutoConfiguration
@EnableConfigurationProperties(LogVelocityProperties.class)
@ConditionalOnProperty(prefix = "log-velocity", name = "enabled", havingValue = "true")
public class LogVelocityAutoConfiguration {

    /**
     * Producer dedicated to log traffic. Developer settings override the defaults, but never
     * {@code max.block.ms}, the bootstrap servers or the serializers. Closed by the appender installer.
     */
    @Bean(name = "logVelocityProducer", destroyMethod = "")
    public KafkaProducer<String, byte[]> logVelocityProducer(LogVelocityProperties props) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.ACKS_CONFIG, "0");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        config.putAll(props.getKafka().getProducer());
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getKafka().getBootstrapServers());
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 0);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaProducer<>(config);
    }

    /**
     * Attaches the appender to the root logger and closes the producer on shutdown.
     */
    @Bean
    public KafkaAppenderInstaller kafkaAppenderInstaller(
            @Qualifier("logVelocityProducer") KafkaProducer<String, byte[]> producer,
            LogVelocityProperties props,
            Environment environment) {
        return new KafkaAppenderInstaller(producer, props, environment);
    }
}
