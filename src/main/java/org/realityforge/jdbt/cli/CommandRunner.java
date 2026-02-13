package org.realityforge.jdbt.cli;

import java.nio.file.Path;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.db.DatabaseConnection;

interface CommandRunner {
    void status(@Nullable String databaseKey, String driver);

    void create(
            @Nullable String databaseKey,
            String driver,
            DatabaseConnection target,
            boolean noCreate,
            Map<String, String> filterProperties);

    void drop(
            @Nullable String databaseKey,
            String driver,
            DatabaseConnection target,
            Map<String, String> filterProperties);

    void migrate(
            @Nullable String databaseKey,
            String driver,
            DatabaseConnection target,
            Map<String, String> filterProperties);

    void databaseImport(
            @Nullable String databaseKey,
            String driver,
            @Nullable String importKey,
            @Nullable String moduleGroup,
            DatabaseConnection target,
            DatabaseConnection source,
            @Nullable String resumeAt,
            Map<String, String> filterProperties);

    void createByImport(
            @Nullable String databaseKey,
            String driver,
            @Nullable String importKey,
            DatabaseConnection target,
            DatabaseConnection source,
            @Nullable String resumeAt,
            boolean noCreate,
            Map<String, String> filterProperties);

    void loadDataset(
            @Nullable String databaseKey,
            String driver,
            String dataset,
            DatabaseConnection target,
            Map<String, String> filterProperties);

    void upModuleGroup(
            @Nullable String databaseKey,
            String driver,
            String moduleGroup,
            DatabaseConnection target,
            Map<String, String> filterProperties);

    void downModuleGroup(
            @Nullable String databaseKey,
            String driver,
            String moduleGroup,
            DatabaseConnection target,
            Map<String, String> filterProperties);

    void packageData(@Nullable String databaseKey, Path outputFile);

    void dumpFixtures(@Nullable String databaseKey, String driver, DatabaseConnection target);
}
