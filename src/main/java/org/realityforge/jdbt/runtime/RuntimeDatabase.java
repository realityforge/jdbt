package org.realityforge.jdbt.runtime;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
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
        imports = Map.copyOf(imports);
        moduleGroups = Map.copyOf(moduleGroups);
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
        for (final ArtifactContent artifact : postDbArtifacts) {
            if (artifact.id().equals(id)) {
                return artifact;
            }
        }
        for (final ArtifactContent artifact : preDbArtifacts) {
            if (artifact.id().equals(id)) {
                return artifact;
            }
        }
        return null;
    }
}
