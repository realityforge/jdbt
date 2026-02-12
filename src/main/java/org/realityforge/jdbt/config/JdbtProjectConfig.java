package org.realityforge.jdbt.config;

import java.util.Map;

public record JdbtProjectConfig(DefaultsConfig defaults, Map<String, DatabaseConfig> databases) {
    public JdbtProjectConfig {
        databases = Map.copyOf(databases);
    }
}
