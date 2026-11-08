package com.github.logvelocity.model;

import lombok.Getter;

import java.util.Map;

/**
 * Detail of one log event, carried as a JSON string in the {@code logDetails} field of {@link LogRecord}.
 *
 * @author vatsan127@gmail.com
 */
@Getter
public abstract class LogDetails {

    private final Integer lineNumber;
    private final String logger;
    private final String logMessage;
    private final Map<String, String> mdc;

    /**
     * @param lineNumber source line of the log call; {@code null} when caller data is unavailable
     * @param logger     logger name, usually the fully qualified class name
     * @param logMessage message with its arguments already substituted
     * @param mdc        MDC of the event
     */
    protected LogDetails(Integer lineNumber,
                         String logger,
                         String logMessage,
                         Map<String, String> mdc) {
        this.lineNumber = lineNumber;
        this.logger = logger;
        this.logMessage = logMessage;
        this.mdc = mdc;
    }
}
