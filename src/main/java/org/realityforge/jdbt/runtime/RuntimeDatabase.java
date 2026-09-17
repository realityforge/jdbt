package org.realityforge.jdbt.runtime;

import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.config.FilterPropertyConfig;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.files.ArtifactContent;
import org.realityforge.jdbt.repository.RepositoryConfig;
import org.realityforge.jdbt.repository.RepositoryTable;

public record RuntimeDatabase(
        RepositoryConfig repository,
        Path resourceRoot,
        List<ArtifactContent> preDbArtifacts,
        List<ArtifactContent> postDbArtifacts,
        String indexFileName,
        List<String> upDirs,
        List<String> downDirs,
        List<String> finalizeDirs,
        List<String> preCreateDirs,
        List<String> postCreateDirs,
        String fixtureDirName,
        String datasetsDirName,
        List<String> preDatasetDirs,
        List<String> postDatasetDirs,
        List<String> datasets,
        String migrationDir,
        @Nullable String version,
        @Nullable String schemaHash,
        @Nullable String dataPath,
        @Nullable String logPath,
        boolean forceDrop,
        boolean deleteBackupHistory,
        boolean reindexOnImport,
        boolean shrinkOnImport,
        Map<String, FilterPropertyConfig> filterProperties,
        Map<String, ImportConfig> imports,
        List<String> contributionDirs) {

    public RuntimeDatabase {
        resourceRoot = resourceRoot.toAbsolutePath().normalize();
        preDbArtifacts = List.copyOf(preDbArtifacts);
        postDbArtifacts = List.copyOf(postDbArtifacts);
        upDirs = List.copyOf(upDirs);
        downDirs = List.copyOf(downDirs);
        finalizeDirs = List.copyOf(finalizeDirs);
        preCreateDirs = List.copyOf(preCreateDirs);
        postCreateDirs = List.copyOf(postCreateDirs);
        preDatasetDirs = List.copyOf(preDatasetDirs);
        postDatasetDirs = List.copyOf(postDatasetDirs);
        datasets = List.copyOf(datasets);
        filterProperties = Collections.unmodifiableMap(new LinkedHashMap<>(filterProperties));
        imports = Collections.unmodifiableMap(new LinkedHashMap<>(imports));
        contributionDirs = List.copyOf(contributionDirs);
    }

    public String schemaNameForModule(final String moduleName) {
        return repository.schemaNameForModule(moduleName);
    }

    public List<String> tableOrdering(final String moduleName) {
        return repository.tableOrdering(moduleName);
    }

    public List<RepositoryTable> tablesForModule(final String moduleName) {
        return repository.tablesForModule(moduleName);
    }

    public List<String> sequenceOrdering(final String moduleName) {
        return repository.sequenceOrdering(moduleName);
    }

    public List<String> orderedElementsForModule(final String moduleName) {
        return repository.orderedElementsForModule(moduleName);
    }
}
