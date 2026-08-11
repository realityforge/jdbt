package org.realityforge.jdbt.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.realityforge.jdbt.config.ConfigException;

public record RepositoryConfig(
        List<String> modules,
        Map<String, String> schemaOverrides,
        Map<String, List<RepositoryTable>> tableMap,
        Map<String, List<String>> sequenceMap) {

    public RepositoryConfig {
        modules = List.copyOf(modules);
        schemaOverrides = Map.copyOf(schemaOverrides);
        tableMap = normalizeTables(tableMap);
        sequenceMap = normalizeStrings(sequenceMap);
    }

    private static Map<String, List<RepositoryTable>> normalizeTables(final Map<String, List<RepositoryTable>> source) {
        return source.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
    }

    private static Map<String, List<String>> normalizeStrings(final Map<String, List<String>> source) {
        return source.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
    }

    public String schemaNameForModule(final String moduleName) {
        final var override = schemaOverrides.get(moduleName);
        if (null != override) {
            return override;
        }
        if (modules.contains(moduleName)) {
            return moduleName;
        }
        throw new ConfigException("Unable to determine schema name for non existent module " + moduleName);
    }

    public List<RepositoryTable> tablesForModule(final String moduleName) {
        final var tables = tableMap.get(moduleName);
        if (null == tables) {
            throw new ConfigException("No tables defined for module " + moduleName);
        }
        return tables;
    }

    public List<String> tableOrdering(final String moduleName) {
        return tablesForModule(moduleName).stream().map(RepositoryTable::name).toList();
    }

    public List<String> sequenceOrdering(final String moduleName) {
        final var sequences = sequenceMap.get(moduleName);
        if (null == sequences) {
            throw new ConfigException("No sequences defined for module " + moduleName);
        }
        return sequences;
    }

    public List<String> orderedElementsForModule(final String moduleName) {
        final var values = new ArrayList<>(tableOrdering(moduleName));
        values.addAll(sequenceOrdering(moduleName));
        return List.copyOf(values);
    }
}
