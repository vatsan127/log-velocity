package com.github.logvelocity.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Settings bound from {@code log-velocity.*}. Kafka settings are under {@code log-velocity.kafka.*}.
 *
 * @author vatsan127@gmail.com
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "log-velocity")
public class LogVelocityProperties {

    /** Turns the starter on; {@code false} by default. */
    private boolean enabled = false;

    /** MDC key that holds the trace id; {@code trace_id} by default. */
    private String traceIdKey = "trace_id";

    /** Instance name written to each record; defaults to the host name. */
    private String instanceName;

    /** Kafka settings for log traffic. */
    private Kafka kafka = new Kafka();

    /**
     * Settings bound from {@code log-velocity.kafka.*}.
     */
    @Getter
    @Setter
    public static class Kafka {

        /** Kafka brokers used only for log traffic, separate from the application's own Kafka settings. */
        private String bootstrapServers;

        /** Topic for every event below ERROR. */
        private String generalTopic;

        /** Topic for ERROR events. */
        private String errorTopic;

        /**
         * Extra Kafka producer settings. They override the defaults ({@code acks=0}, {@code linger.ms=5})
         * but never {@code max.block.ms}, the bootstrap servers or the serializers.
         */
        private Map<String, String> producer = new HashMap<>();

        /** Longest wait for buffered events when the producer closes at shutdown; {@code 5s} by default. */
        private Duration shutdownTimeout = Duration.ofSeconds(5);

        /** Topic for the registration message sent at startup; none is sent if unset. */
        private String registryTopic;

        /** Longest wait for the topics' metadata at startup; {@code 3s} by default. */
        private Duration warmupTimeout = Duration.ofSeconds(3);
    }
}
