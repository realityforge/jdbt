package org.realityforge.jdbt.repository;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.realityforge.jdbt.config.ConfigException;
import org.realityforge.jdbt.config.YamlMapSupport;

public final class RepositoryConfigLoader {
    public RepositoryConfig load(final String yaml, final String sourceName) {
        final var root = YamlMapSupport.parseRoot(yaml, sourceName);
        YamlMapSupport.assertKeys(root, Set.of("modules"), sourceName);

        final var modulesValue = root.get("modules");
        if (modulesValue == null) {
            return new RepositoryConfig(List.of(), Map.of(), Map.of(), Map.of());
        }

        final var entries = parseModuleEntries(modulesValue, sourceName + ".modules");
        final var modules = new ArrayList<String>(entries.size());
        final var schemaOverrides = new LinkedHashMap<String, String>();
        final var tableMap = new LinkedHashMap<String, List<String>>();
        final var sequenceMap = new LinkedHashMap<String, List<String>>();

        for (final var entry : entries) {
            if (tableMap.containsKey(entry.name)) {
                throw new ConfigException("Duplicate repository module '" + entry.name + "' in " + sourceName + '.');
            }
            modules.add(entry.name);
            tableMap.put(entry.name, entry.tables);
            sequenceMap.put(entry.name, entry.sequences);
            if (!entry.name.equals(entry.schema)) {
                schemaOverrides.put(entry.name, entry.schema);
            }
        }

        return new RepositoryConfig(modules, schemaOverrides, tableMap, sequenceMap);
    }

    private static List<ModuleEntry> parseModuleEntries(final Object modulesValue, final String path) {
        if (modulesValue instanceof List<?> list) {
            final var entries = new ArrayList<ModuleEntry>(list.size());
            for (int i = 0; i < list.size(); i++) {
                final var element = list.get(i);
                if (!(element instanceof Map<?, ?> map)) {
                    throw new ConfigException("Expected module map at " + path + '[' + i + "].");
                }
                final var asStringMap = YamlMapSupport.toStringMap(map, path + '[' + i + ']');
                if (asStringMap.size() != 1) {
                    throw new ConfigException("Expected single module entry at " + path + '[' + i + "].");
                }
                final var moduleName = asStringMap.keySet().iterator().next();
                final var moduleBody = asStringMap.values().iterator().next();
                entries.add(parseModule(moduleName, moduleBody, path + '[' + i + ']'));
            }
            return entries;
        }

        if (modulesValue instanceof Map<?, ?> map) {
            final var moduleMap = YamlMapSupport.toStringMap(map, path);
            final var entries = new ArrayList<ModuleEntry>(moduleMap.size());
            for (final var entry : moduleMap.entrySet()) {
                entries.add(parseModule(entry.getKey(), entry.getValue(), path + '.' + entry.getKey()));
            }
            return entries;
        }

        throw new ConfigException("Expected list or map for " + path + '.');
    }

    private static ModuleEntry parseModule(final String moduleName, final Object moduleBody, final String path) {
        if (!(moduleBody instanceof Map<?, ?> map)) {
            throw new ConfigException("Expected map body for module '" + moduleName + "' at " + path + '.');
        }

        final var body = YamlMapSupport.toStringMap(map, path);
        YamlMapSupport.assertKeys(body, Set.of("schema", "tables", "sequences"), path);

        final var schema = YamlMapSupport.optionalString(body, "schema", path) == null
                ? moduleName
                : YamlMapSupport.requireString(body, "schema", path);
        final var tables = YamlMapSupport.optionalStringList(body, "tables", path, List.of());
        final var sequences = YamlMapSupport.optionalStringList(body, "sequences", path, List.of());
        return new ModuleEntry(moduleName, schema, tables, sequences);
    }

    private record ModuleEntry(String name, String schema, List<String> tables, List<String> sequences) {}
}
