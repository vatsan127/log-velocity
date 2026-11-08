package com.github.logvelocity.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Getter;

import java.time.LocalDateTime;

/**
 * Registration message sent once when the application starts. Written as plain JSON with snake_case names.
 *
 * @author vatsan127@gmail.com
 */
@Getter
public class RegistryRecord {

    private final String instanceName;
    private final String applicationName;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private final LocalDateTime registryTime;

    /**
     * @param instanceName    instance that registered
     * @param applicationName application the instance belongs to
     * @param registryTime    registration time in the application's local zone, as {@code yyyy-MM-dd HH:mm:ss}
     */
    public RegistryRecord(String instanceName, String applicationName, LocalDateTime registryTime) {
        this.instanceName = instanceName;
        this.applicationName = applicationName;
        this.registryTime = registryTime;
    }
}
