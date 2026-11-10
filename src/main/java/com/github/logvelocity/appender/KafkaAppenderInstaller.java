package com.github.logvelocity.appender;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import com.github.logvelocity.config.LogVelocityProperties;
import com.github.logvelocity.model.RegistryRecord;
import com.github.logvelocity.util.LogUtils;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.stream.Stream;

/**
 * Creates the {@link KafkaAppender}, attaches it to the Logback root logger, registers the instance,
 * and on shutdown detaches the appender and closes the log producer.
 *
 * @author vatsan127@gmail.com
 */
public class KafkaAppenderInstaller {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(KafkaAppenderInstaller.class);

    private static final long RETRY_DELAY_MS = 50;

    private final Producer<String, byte[]> producer;
    private final Duration shutdownTimeout;
    private final Duration warmupTimeout;
    private final String generalTopic;
    private final String errorTopic;
    private final String registryTopic;
    private final String applicationName;
    private final String instanceName;
    private final KafkaAppender appender;
    private Logger root;

    /**
     * Application name: {@code spring.application.name}, else {@code unknown}.
     * Instance name: the property, else the host name.
     *
     * @param producer    producer dedicated to log traffic; closed here at shutdown
     * @param props       topics, trace id key, instance name and timeouts
     * @param environment source of {@code spring.application.name}
     */
    public KafkaAppenderInstaller(Producer<String, byte[]> producer,
                                  LogVelocityProperties props,
                                  Environment environment) {
        this.producer = producer;
        this.shutdownTimeout = props.getKafka().getShutdownTimeout();
        this.warmupTimeout = props.getKafka().getWarmupTimeout();
        this.generalTopic = props.getKafka().getGeneralTopic();
        this.errorTopic = props.getKafka().getErrorTopic();
        this.registryTopic = props.getKafka().getRegistryTopic();
        this.applicationName = environment.getProperty("spring.application.name", "unknown");
        this.instanceName = props.getInstanceName() != null ? props.getInstanceName() : LogUtils.hostName();
        this.appender = new KafkaAppender(producer, props, applicationName, instanceName);
    }

    /**
     * Waits up to the warmup timeout for the topics' metadata, attaches the appender to the root logger
     * (Logback only) and logs that it is attached. Sends the registration message if the metadata arrived;
     * otherwise logs a warning that events are dropped until Kafka is reachable.
     * The wait blocks startup only, never a request thread.
     */
    @PostConstruct
    public void install() {
        boolean ready = warmUp();
        if (LoggerFactory.getILoggerFactory() instanceof LoggerContext context) {
            appender.setContext(context);
            appender.setName("LOG_VELOCITY_KAFKA");
            appender.start();
            root = context.getLogger(Logger.ROOT_LOGGER_NAME);
            root.addAppender(appender);
            log.info("log-velocity: Kafka appender attached (general-topic={}, error-topic={})", generalTopic, errorTopic);
        }
        if (ready) {
            register();
        } else {
            log.warn("log-velocity: topic metadata not available within {} ms, events are dropped until Kafka is reachable",
                    warmupTimeout.toMillis());
        }
    }

    /**
     * Detaches and stops the appender, then closes the producer, waiting up to the shutdown timeout
     * for buffered events.
     */
    @PreDestroy
    public void uninstall() {
        if (root != null) {
            root.detachAppender(appender);
        }
        appender.stop();
        producer.close(shutdownTimeout);
    }

    private boolean warmUp() {
        long deadline = System.nanoTime() + warmupTimeout.toNanos();
        for (String topic : Stream.of(generalTopic, errorTopic, registryTopic).filter(t -> t != null).toList()) {
            while (!hasMetadata(topic)) {
                if (System.nanoTime() >= deadline) {
                    return false;
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return true;
    }

    private boolean hasMetadata(String topic) {
        try {
            producer.partitionsFor(topic);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private void register() {
        if (registryTopic == null) {
            return;
        }
        try {
            RegistryRecord registryRecord = new RegistryRecord(instanceName, applicationName, LocalDateTime.now());
            producer.send(new ProducerRecord<>(registryTopic, instanceName,
                    LogUtils.MAPPER.writeValueAsBytes(registryRecord)));
        } catch (RuntimeException e) {
        }
    }
}
