package org.realityforge.jdbt.repository;

import java.util.List;
import java.util.Map;
import org.realityforge.jdbt.config.ConfigException;

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

    public String schemaNameForModule(final String moduleName) {
        final String override = schemaOverrides.get(moduleName);
        if (null != override) {
            return override;
        }
        if (modules.contains(moduleName)) {
            return moduleName;
        }
        throw new ConfigException("Unable to determine schema name for non existent module " + moduleName);
    }

    public List<String> tableOrdering(final String moduleName) {
        final List<String> tables = tableMap.get(moduleName);
        if (null == tables) {
            throw new ConfigException("No tables defined for module " + moduleName);
        }
        return tables;
    }

    public List<String> sequenceOrdering(final String moduleName) {
        final List<String> sequences = sequenceMap.get(moduleName);
        if (null == sequences) {
            throw new ConfigException("No sequences defined for module " + moduleName);
        }
        return sequences;
    }

    public List<String> orderedElementsForModule(final String moduleName) {
        final List<String> values = new java.util.ArrayList<>(tableOrdering(moduleName));
        values.addAll(sequenceOrdering(moduleName));
        return List.copyOf(values);
    }
}
