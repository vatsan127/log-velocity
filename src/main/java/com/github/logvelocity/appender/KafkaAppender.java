package com.github.logvelocity.appender;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import com.github.logvelocity.config.LogVelocityProperties;
import com.github.logvelocity.model.ErrorLogDetails;
import com.github.logvelocity.model.GeneralLogDetails;
import com.github.logvelocity.model.LogDetails;
import com.github.logvelocity.model.LogRecord;
import com.github.logvelocity.util.LogUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.MDC;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

/**
 * Logback appender that sends each event to Kafka as a {@link LogRecord} in JSON.
 * <ul>
 *   <li>ERROR and above go to the error topic, everything else to the general topic.</li>
 *   <li>The Kafka key is the trace id from MDC. If MDC has none, one is generated and put in MDC, so later
 *       events on the same thread reuse it until the MDC is cleared.</li>
 *   <li>The sequence number counts the events of the current trace id on the logging thread, starting at 1.
 *       It restarts when the trace id changes, for example after the MDC is cleared.</li>
 *   <li>ERROR events carry {@link ErrorLogDetails} with the exception; others carry {@link GeneralLogDetails}.</li>
 *   <li>Sends on the logging thread and never waits for Kafka. A failed send drops the event silently.</li>
 * </ul>
 * Created in code with a dedicated producer, not through XML.
 *
 * @author vatsan127@gmail.com
 */
public class KafkaAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {

    private final Producer<String, byte[]> producer;
    private final LogVelocityProperties props;
    private final String applicationName;
    private final String instanceName;
    private final ThreadLocal<Sequence> sequence = new ThreadLocal<>();

    /**
     * @param producer        producer dedicated to log traffic
     * @param props           topics and trace id key
     * @param applicationName application name written to each record
     * @param instanceName    instance name written to each record
     */
    public KafkaAppender(Producer<String, byte[]> producer, LogVelocityProperties props,
                         String applicationName, String instanceName) {
        this.producer = producer;
        this.props = props;
        this.applicationName = applicationName;
        this.instanceName = instanceName;
    }

    /**
     * Builds the record for one event and hands it to the producer.
     */
    @Override
    protected void append(ILoggingEvent event) {
        boolean error = event.getLevel().isGreaterOrEqual(Level.ERROR);
        String topic = error ? props.getKafka().getErrorTopic() : props.getKafka().getGeneralTopic();

        String traceId = event.getMDCPropertyMap().get(props.getTraceIdKey());
        if (traceId == null) {
            traceId = UUID.randomUUID().toString();
            MDC.put(props.getTraceIdKey(), traceId);
        }

        LogDetails details = error ? errorDetails(event) : generalDetails(event);

        LogRecord logRecord = new LogRecord(
                event.getThreadName(),
                applicationName,
                instanceName,
                event.getLevel().toString(),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(event.getTimeStamp()), ZoneId.systemDefault()),
                nextSequence(traceId),
                traceId,
                LogUtils.MAPPER.writeValueAsString(details));

        producer.send(new ProducerRecord<>(topic, traceId, LogUtils.MAPPER.writeValueAsBytes(logRecord)));
    }

    private long nextSequence(String traceId) {
        Sequence current = sequence.get();
        if (current == null || !current.traceId.equals(traceId)) {
            current = new Sequence(traceId);
            sequence.set(current);
        }
        return ++current.value;
    }

    private GeneralLogDetails generalDetails(ILoggingEvent event) {
        return new GeneralLogDetails(
                LogUtils.lineNumber(event),
                event.getLoggerName(),
                event.getFormattedMessage(),
                event.getMDCPropertyMap(),
                null);
    }

    private ErrorLogDetails errorDetails(ILoggingEvent event) {
        IThrowableProxy throwable = event.getThrowableProxy();
        return new ErrorLogDetails(
                LogUtils.lineNumber(event),
                event.getLoggerName(),
                event.getFormattedMessage(),
                event.getMDCPropertyMap(),
                throwable == null ? null : throwable.getClassName(),
                throwable == null ? null : throwable.getMessage(),
                throwable == null ? null : ThrowableProxyUtil.asString(throwable));
    }

    private static final class Sequence {

        private final String traceId;
        private long value;

        private Sequence(String traceId) {
            this.traceId = traceId;
        }
    }
}
