package org.realityforge.jdbt.runtime;

import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.config.FilterPropertyConfig;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.config.ModuleGroupConfig;
import org.realityforge.jdbt.files.ArtifactContent;
import org.realityforge.jdbt.repository.RepositoryConfig;

public record RuntimeDatabase(
        String key,
        RepositoryConfig repository,
        List<Path> searchDirs,
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
        boolean migrationsEnabled,
        boolean migrationsAppliedAtCreate,
        String migrationsDirName,
        @Nullable String version,
        @Nullable String schemaHash,
        Map<String, FilterPropertyConfig> filterProperties,
        Map<String, ImportConfig> imports,
        Map<String, ModuleGroupConfig> moduleGroups) {

    public RuntimeDatabase {
        searchDirs = List.copyOf(searchDirs);
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
        imports = Map.copyOf(imports);
        moduleGroups = Map.copyOf(moduleGroups);
    }

    public RuntimeDatabase(
            final String key,
            final RepositoryConfig repository,
            final List<Path> searchDirs,
            final List<ArtifactContent> preDbArtifacts,
            final List<ArtifactContent> postDbArtifacts,
            final String indexFileName,
            final List<String> upDirs,
            final List<String> downDirs,
            final List<String> finalizeDirs,
            final List<String> preCreateDirs,
            final List<String> postCreateDirs,
            final String fixtureDirName,
            final String datasetsDirName,
            final List<String> preDatasetDirs,
            final List<String> postDatasetDirs,
            final List<String> datasets,
            final boolean migrationsEnabled,
            final boolean migrationsAppliedAtCreate,
            final String migrationsDirName,
            final @Nullable String version,
            final @Nullable String schemaHash,
            final Map<String, ImportConfig> imports,
            final Map<String, ModuleGroupConfig> moduleGroups) {
        this(
                key,
                repository,
                searchDirs,
                preDbArtifacts,
                postDbArtifacts,
                indexFileName,
                upDirs,
                downDirs,
                finalizeDirs,
                preCreateDirs,
                postCreateDirs,
                fixtureDirName,
                datasetsDirName,
                preDatasetDirs,
                postDatasetDirs,
                datasets,
                migrationsEnabled,
                migrationsAppliedAtCreate,
                migrationsDirName,
                version,
                schemaHash,
                Map.of(),
                imports,
                moduleGroups);
    }

    public String schemaNameForModule(final String moduleName) {
        return repository.schemaNameForModule(moduleName);
    }

    public List<String> tableOrdering(final String moduleName) {
        return repository.tableOrdering(moduleName);
    }

    public List<String> sequenceOrdering(final String moduleName) {
        return repository.sequenceOrdering(moduleName);
    }

    public List<String> orderedElementsForModule(final String moduleName) {
        return repository.orderedElementsForModule(moduleName);
    }

    public @Nullable ArtifactContent artifactById(final String id) {
        for (final var artifact : postDbArtifacts) {
            if (artifact.id().equals(id)) {
                return artifact;
            }
        }
        for (final var artifact : preDbArtifacts) {
            if (artifact.id().equals(id)) {
                return artifact;
            }
        }
        return null;
    }
}
