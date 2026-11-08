package com.github.logvelocity.util;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Helpers shared by the appender and the installer.
 *
 * @author vatsan127@gmail.com
 */
public final class LogUtils {

    /** Writes JSON with snake_case names and leaves out {@code null} fields. */
    public static final JsonMapper MAPPER = JsonMapper.builder()
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .changeDefaultPropertyInclusion(v -> v.withValueInclusion(JsonInclude.Include.NON_NULL))
            .build();

    private LogUtils() {
    }

    /**
     * @return the source line of the log call, or {@code null} when caller data is unavailable
     */
    public static Integer lineNumber(ILoggingEvent event) {
        StackTraceElement[] caller = event.getCallerData();
        return caller == null || caller.length == 0 ? null : caller[0].getLineNumber();
    }

    /**
     * @return the host name, or {@code unknown} if it cannot be resolved
     */
    public static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }
}
