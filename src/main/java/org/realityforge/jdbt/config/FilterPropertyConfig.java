package org.realityforge.jdbt.config;

import java.util.List;
import org.jspecify.annotations.Nullable;

public record FilterPropertyConfig(String pattern, @Nullable String defaultValue, List<String> supportedValues) {
    public FilterPropertyConfig {
        supportedValues = List.copyOf(supportedValues);
    }
}
