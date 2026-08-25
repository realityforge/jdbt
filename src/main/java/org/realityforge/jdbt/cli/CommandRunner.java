package org.realityforge.jdbt.cli;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.db.DatabaseConnection;

interface CommandRunner {
    void validateProject();

    void status(String driver);

    void create(String driver, DatabaseConnection target, boolean noCreate, Map<String, String> filterProperties);

    void createWithDataset(
            String driver,
            DatabaseConnection target,
            boolean noCreate,
            String dataset,
            Map<String, String> filterProperties);

    void drop(String driver, DatabaseConnection target, Map<String, String> filterProperties);

    void migrate(String driver, DatabaseConnection target, Map<String, String> filterProperties);

    void databaseImport(
            String driver,
            @Nullable String importKey,
            @Nullable String moduleGroup,
            DatabaseConnection target,
            DatabaseConnection source,
            @Nullable String resumeAt,
            @Nullable Path timingOutput,
            Map<String, String> filterProperties);

    void createByImport(
            String driver,
            @Nullable String importKey,
            DatabaseConnection target,
            DatabaseConnection source,
            @Nullable String resumeAt,
            boolean noCreate,
            @Nullable Path timingOutput,
            Map<String, String> filterProperties);

    void loadDataset(String driver, String dataset, DatabaseConnection target, Map<String, String> filterProperties);

    void upModuleGroup(
            String driver, String moduleGroup, DatabaseConnection target, Map<String, String> filterProperties);

    void downModuleGroup(
            String driver, String moduleGroup, DatabaseConnection target, Map<String, String> filterProperties);

    void packageData(Path outputFile);

    void emitStandardImports(@Nullable String importKey, @Nullable Path outputDirectory, boolean replace);

    void verifyConstraints(
            String driver,
            DatabaseConnection target,
            List<String> schemas,
            List<String> checkQueries,
            Map<String, String> filterProperties);

    void exportFixtures(
            String driver,
            DatabaseConnection target,
            Path propertiesFile,
            @Nullable String dataset,
            @Nullable Path outputDirectory,
            Map<String, String> filterProperties);

    void exportDatabaseStatistics(String driver, DatabaseConnection target, Path outputFile);
}
