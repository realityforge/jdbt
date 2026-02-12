package org.realityforge.jdbt.runtime;

import java.nio.file.Path;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.config.DatabaseConfig;
import org.realityforge.jdbt.config.DefaultsConfig;
import org.realityforge.jdbt.files.ArtifactContent;
import org.realityforge.jdbt.repository.RepositoryConfig;

public final class RuntimeDatabaseFactory {
    public RuntimeDatabase from(
            final DatabaseConfig database,
            final DefaultsConfig defaults,
            final RepositoryConfig repository,
            final List<ArtifactContent> preDbArtifacts,
            final List<ArtifactContent> postDbArtifacts,
            final @Nullable String schemaHash) {
        return new RuntimeDatabase(
                database.key(),
                repository,
                database.searchDirs().stream().map(Path::of).toList(),
                preDbArtifacts,
                postDbArtifacts,
                defaults.indexFileName(),
                database.upDirs(),
                database.downDirs(),
                database.finalizeDirs(),
                database.preCreateDirs(),
                database.postCreateDirs(),
                database.fixtureDirName(),
                database.datasetsDirName(),
                database.preDatasetDirs(),
                database.postDatasetDirs(),
                database.datasets(),
                database.migrations(),
                database.version(),
                schemaHash,
                database.imports(),
                database.moduleGroups());
    }
}
