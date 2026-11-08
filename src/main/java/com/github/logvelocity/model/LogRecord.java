package com.github.logvelocity.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Getter;

import java.time.LocalDateTime;

/**
 * The Kafka message value: one log event, shaped like a row of the log tables.
 * Written as plain JSON with snake_case names. The structure is fixed.
 *
 * @author vatsan127@gmail.com
 */
@Getter
public class LogRecord {

    private final String threadName;
    private final String application;
    private final String instanceName;
    private final String logLevel;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private final LocalDateTime timestamp;
    private final long sequenceNumber;
    private final String traceId;
    private final String logDetails;

    /**
     * @param threadName     name of the thread that logged the event
     * @param application    application name
     * @param instanceName   instance that produced the event
     * @param logLevel       Logback level name, for example {@code ERROR}
     * @param timestamp      event time in the application's local zone, as {@code yyyy-MM-dd HH:mm:ss}
     * @param sequenceNumber count of events for the trace id on the logging thread, starting at 1
     * @param traceId        request correlation id; also the Kafka message key
     * @param logDetails     {@link LogDetails} serialized as a JSON string
     */
    public LogRecord(String threadName,
                     String application,
                     String instanceName,
                     String logLevel,
                     LocalDateTime timestamp,
                     long sequenceNumber,
                     String traceId,
                     String logDetails) {
        this.threadName = threadName;
        this.application = application;
        this.instanceName = instanceName;
        this.logLevel = logLevel;
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
        this.traceId = traceId;
        this.logDetails = logDetails;
    }
}
