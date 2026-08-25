package org.realityforge.jdbt.runtime;

import java.nio.file.Path;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.config.DatabaseConfig;
import org.realityforge.jdbt.files.ArtifactContent;
import org.realityforge.jdbt.repository.RepositoryConfig;

public final class RuntimeDatabaseFactory {
    private static final String INDEX_FILE_NAME = "index.txt";

    public RuntimeDatabase from(
            final DatabaseConfig database,
            final RepositoryConfig repository,
            final List<ArtifactContent> preDbArtifacts,
            final List<ArtifactContent> postDbArtifacts,
            final @Nullable String schemaHash,
            final Path resourceRoot) {
        return new RuntimeDatabase(
                repository,
                resourceRoot,
                preDbArtifacts,
                postDbArtifacts,
                INDEX_FILE_NAME,
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
                database.migrationDir(),
                database.version(),
                schemaHash,
                database.dataPath(),
                database.logPath(),
                database.forceDrop(),
                database.deleteBackupHistory(),
                database.reindexOnImport(),
                database.shrinkOnImport(),
                database.filterProperties(),
                database.imports());
    }
}
