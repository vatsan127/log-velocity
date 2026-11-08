# log-velocity

A Spring Boot starter that sends every Logback event to Kafka as plain JSON.

- ERROR events go to an error topic. All other events go to a general topic.
- The Kafka key is the trace id, so one request stays in one partition, in order.
- The logging thread never waits for Kafka.

## Requirements

| | Version |
|---|---|
| Java | 21 |
| Spring Boot | 4.1.x |
| Logging backend | Logback (the starter does nothing with another backend) |

## Usage

```xml
<dependency>
    <groupId>com.github.logvelocity</groupId>
    <artifactId>log-velocity</artifactId>
    <version>1.0.0</version>
</dependency>
```

```yaml
spring:
  application:
    name: offer-service

log-velocity:
  kafka:
    enabled: true
    bootstrap-servers: kafka.example.com:9092
    general-topic: app-logs
    error-topic: error-logs
    registry-topic: app-registry
```

## Configuration

All keys are under `log-velocity.kafka`.

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Turns the starter on |
| `bootstrap-servers` | none | Kafka brokers for log traffic only. Never reads `spring.kafka.*`. |
| `general-topic` | none | Topic for events below ERROR |
| `error-topic` | none | Topic for ERROR events |
| `registry-topic` | none | Topic for the registration message sent at startup. None is sent if unset. |
| `trace-id-key` | `trace_id` | MDC key that holds the trace id |
| `instance-name` | host name | Instance name written to each record |
| `producer.*` | `acks=0`, `linger.ms=5` | Extra producer settings. `max.block.ms`, the bootstrap servers and the serializers cannot be overridden. |
| `warmup-timeout` | `3s` | Longest wait for the topics' metadata at startup |
| `shutdown-timeout` | `5s` | Longest wait for buffered events at shutdown |

The application name comes from `spring.application.name`, or `unknown` if it is not set.

## Message format

```json
{
  "thread_name": "http-nio-8080-exec-3",
  "application": "offer-service",
  "instance_name": "offer-service-1",
  "log_level": "ERROR",
  "timestamp": "2026-11-08 20:15:52",
  "sequence_number": 19,
  "trace_id": "c4f6cf8b-f124-4dff-8ef2-67693aa7d14d",
  "log_details": "{\"line_number\":24,\"logger\":\"com.acme.OfferService\",\"log_message\":\"Offer check failed\",\"mdc\":{},\"exception_class\":\"java.lang.IllegalStateException\",\"exception_message\":\"subscriber is null\",\"stack_trace\":\"...\"}"
}
```

- `log_details` is a JSON string holding `line_number`, `logger`, `log_message` and `mdc`.
- At ERROR and above it also holds `exception_class`, `exception_message` and the full `stack_trace`, when the event has an exception.
- Fields that are `null` are left out.
- `timestamp` is the application's local date and time as `yyyy-MM-dd HH:mm:ss`, to the second, with no offset.
- The structure is fixed. Logback patterns and custom layouts are not supported.
- `ddl/clickhouse.sql` has ClickHouse tables for these messages.

Registration message, with `registry_time` in the same format as `timestamp`:

```json
{"instance_name": "offer-service-1", "application_name": "offer-service", "registry_time": "2026-11-08 20:15:52"}
```

## Behaviour

| Stage | What happens |
|---|---|
| Startup | Waits up to `warmup-timeout` for the topics' metadata, attaches the appender and logs one INFO line. Sends the registration message if the metadata arrived. Otherwise logs a WARN that events are dropped until Kafka is reachable. |
| Logging | Builds the record and sends it on the logging thread. `max.block.ms` is 0, so the send never waits. |
| Shutdown | Detaches the appender, then closes the producer, waiting up to `shutdown-timeout` |

Trace id:

- It is read from MDC under `trace-id-key`.
- If MDC has none, a random id is generated and put in MDC. Later events on the same thread reuse it until the MDC is cleared.
- `sequence_number` starts at 1 for each trace id and counts up until the id changes. On scheduled threads the id is never cleared, so the count keeps growing.

## Limitations

- A failed send drops the event silently. Nothing reports it.
- With the default `acks=0`, delivery is at most once.
- Events logged before the appender is attached are not sent.
- If Kafka is not reachable within `warmup-timeout`, the first events are dropped and the registration message is neither sent nor retried.
- A generated trace id stays on its thread. Pooled or scheduled threads can reuse one id for unrelated work unless the application clears MDC.
- `sequence_number` is counted per thread and `timestamp` is to the second, so events of one trace id from different threads have no exact order.
- `timestamp` has no zone, so readers and any database column must use the application's zone.
- `stack_trace` is sent whole, so it can be large, and it uses the operating system's line separator.
- Nothing is masked. Do not log sensitive data.

## Build

```
mvn clean install
```
