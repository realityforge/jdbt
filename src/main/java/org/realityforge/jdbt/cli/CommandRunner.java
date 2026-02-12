package org.realityforge.jdbt.cli;

import java.nio.file.Path;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.db.DatabaseConnection;

public interface CommandRunner {
    void status(@Nullable String databaseKey, String driver);

    void create(@Nullable String databaseKey, String driver, DatabaseConnection target, boolean noCreate);

    void drop(@Nullable String databaseKey, String driver, DatabaseConnection target);

    void migrate(@Nullable String databaseKey, String driver, DatabaseConnection target);

    void databaseImport(
            @Nullable String databaseKey,
            String driver,
            @Nullable String importKey,
            @Nullable String moduleGroup,
            DatabaseConnection target,
            DatabaseConnection source,
            @Nullable String resumeAt);

    void createByImport(
            @Nullable String databaseKey,
            String driver,
            @Nullable String importKey,
            DatabaseConnection target,
            DatabaseConnection source,
            @Nullable String resumeAt,
            boolean noCreate);

    void loadDataset(@Nullable String databaseKey, String driver, String dataset, DatabaseConnection target);

    void upModuleGroup(@Nullable String databaseKey, String driver, String moduleGroup, DatabaseConnection target);

    void downModuleGroup(@Nullable String databaseKey, String driver, String moduleGroup, DatabaseConnection target);

    void packageData(@Nullable String databaseKey, Path outputFile);

    void dumpFixtures(@Nullable String databaseKey, String driver, DatabaseConnection target);
}
