package com.github.logvelocity.model;

import lombok.Getter;

import java.util.Map;

/**
 * Details for events below ERROR.
 *
 * @author vatsan127@gmail.com
 */
@Getter
public class GeneralLogDetails extends LogDetails {

    private final Map<String, Object> logParams;

    /**
     * @param logParams named parameters of the log call; the appender does not fill it yet
     */
    public GeneralLogDetails(Integer lineNumber,
                             String logger,
                             String logMessage,
                             Map<String, String> mdc,
                             Map<String, Object> logParams) {
        super(lineNumber, logger, logMessage, mdc);
        this.logParams = logParams;
    }
}
