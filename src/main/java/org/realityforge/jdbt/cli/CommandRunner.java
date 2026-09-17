package org.realityforge.jdbt.cli;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.db.DatabaseConnection;

interface CommandRunner {
    void validateProject();

    void status(Map<String, String> filterProperties);

    void create(DatabaseConnection target, boolean noCreate, Map<String, String> filterProperties);

    void createWithDataset(
            DatabaseConnection target, boolean noCreate, String dataset, Map<String, String> filterProperties);

    void drop(DatabaseConnection target, Map<String, String> filterProperties);

    void migrate(DatabaseConnection target, Map<String, String> filterProperties);

    void createByImport(
            @Nullable String importKey,
            DatabaseConnection target,
            DatabaseConnection source,
            @Nullable String resumeAt,
            boolean noCreate,
            @Nullable Path timingOutput,
            Map<String, String> filterProperties);

    void loadDataset(String dataset, DatabaseConnection target, Map<String, String> filterProperties);

    void packageData(Path outputFile);

    void emitStandardImports(@Nullable String importKey, @Nullable Path outputDirectory, boolean replace);

    void verifyConstraints(
            DatabaseConnection target,
            List<String> schemas,
            List<String> checkQueries,
            Map<String, String> filterProperties);

    void exportFixtures(
            DatabaseConnection target,
            Path propertiesFile,
            @Nullable String dataset,
            @Nullable Path outputDirectory,
            Map<String, String> filterProperties);

    void exportDatabaseStatistics(DatabaseConnection target, Path outputFile);
}
