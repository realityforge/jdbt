package org.realityforge.jdbt.repository;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.realityforge.jdbt.config.ConfigException;

public final class RepositoryConfigMerger {
    public RepositoryConfig merge(
            final List<RepositoryConfig> preArtifacts,
            final RepositoryConfig localRepository,
            final List<RepositoryConfig> postArtifacts) {
        final var accumulator = new MergeAccumulator();
        preArtifacts.forEach(accumulator::merge);
        accumulator.merge(localRepository);
        postArtifacts.forEach(accumulator::merge);
        return accumulator.toRepositoryConfig();
    }

    private static final class MergeAccumulator {
        private final List<String> modules = new ArrayList<>();
        private final Map<String, String> schemaOverrides = new LinkedHashMap<>();
        private final Map<String, List<String>> tableMap = new LinkedHashMap<>();
        private final Map<String, List<String>> sequenceMap = new LinkedHashMap<>();

        void merge(final RepositoryConfig source) {
            for (final var module : source.modules()) {
                if (tableMap.containsKey(module)) {
                    throw new ConfigException("Attempting to merge repository with duplicate module definition '"
                            + module + "'. Existing modules: " + modules);
                }
                modules.add(module);
            }
            schemaOverrides.putAll(source.schemaOverrides());
            tableMap.putAll(source.tableMap());
            sequenceMap.putAll(source.sequenceMap());
        }

        RepositoryConfig toRepositoryConfig() {
            return new RepositoryConfig(modules, schemaOverrides, tableMap, sequenceMap);
        }
    }
}
