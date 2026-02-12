package org.realityforge.jdbt.repository;

import java.util.List;
import java.util.Map;

public record RepositoryConfig(
        List<String> modules,
        Map<String, String> schemaOverrides,
        Map<String, List<String>> tableMap,
        Map<String, List<String>> sequenceMap) {

    public RepositoryConfig {
        modules = List.copyOf(modules);
        schemaOverrides = Map.copyOf(schemaOverrides);
        tableMap = normalize(tableMap);
        sequenceMap = normalize(sequenceMap);
    }

    private static Map<String, List<String>> normalize(final Map<String, List<String>> source) {
        return source.entrySet().stream()
                .collect(java.util.stream.Collectors.toUnmodifiableMap(
                        Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
    }
}
