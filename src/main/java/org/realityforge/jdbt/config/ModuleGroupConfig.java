package org.realityforge.jdbt.config;

import java.util.List;

public record ModuleGroupConfig(String key, List<String> modules, boolean importEnabled) {
    public ModuleGroupConfig {
        modules = List.copyOf(modules);
    }
}
