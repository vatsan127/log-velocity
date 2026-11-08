package com.github.logvelocity.model;

import lombok.Getter;

import java.util.Map;

/**
 * Details for events at ERROR and above. The exception fields are {@code null} when the event has no exception.
 *
 * @author vatsan127@gmail.com
 */
@Getter
public class ErrorLogDetails extends LogDetails {

    private final String exceptionClass;
    private final String exceptionMessage;
    private final String stackTrace;

    /**
     * @param exceptionClass   class name of the logged exception
     * @param exceptionMessage message of the logged exception
     * @param stackTrace       full stack trace as text
     */
    public ErrorLogDetails(Integer lineNumber,
                           String logger,
                           String logMessage,
                           Map<String, String> mdc,
                           String exceptionClass,
                           String exceptionMessage,
                           String stackTrace) {
        super(lineNumber, logger, logMessage, mdc);
        this.exceptionClass = exceptionClass;
        this.exceptionMessage = exceptionMessage;
        this.stackTrace = stackTrace;
    }
}
