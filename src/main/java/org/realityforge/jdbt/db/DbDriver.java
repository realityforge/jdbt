package org.realityforge.jdbt.db;

import java.util.List;
import java.util.Map;
import org.realityforge.jdbt.config.ImportConfig;

public interface DbDriver {
    void open(DatabaseConnection connection, boolean openControlDatabase);

    void close();

    void drop(DatabaseMetadata database, DatabaseConnection connection);

    void createDatabase(DatabaseMetadata database, DatabaseConnection connection);

    void createSchema(String schemaName);

    void dropSchema(String schemaName, List<String> tablesInDropOrder);

    void execute(String sql, boolean executeInControlDatabase);

    default void execute(
            final String sql, final boolean executeInControlDatabase, final SqlTimingObserver timingObserver) {
        execute(sql, executeInControlDatabase);
    }

    default void enableImportTiming() {}

    void preFixtureImport(String tableName);

    void insert(String tableName, Map<String, Object> record);

    void postFixtureImport(String tableName);

    void updateSequence(String sequenceName, long value);

    void preTableImport(DatabaseMetadata database, ImportConfig importConfig, String tableName);

    void postTableImport(DatabaseMetadata database, ImportConfig importConfig, String tableName);

    default void postTableImport(
            final DatabaseMetadata database,
            final ImportConfig importConfig,
            final String tableName,
            final ImportMaintenanceObserver observer) {
        postTableImport(database, importConfig, tableName);
    }

    void postDataModuleImport(
            DatabaseMetadata database, ImportConfig importConfig, String moduleName, List<String> tablesInOrder);

    default void postDataModuleImport(
            final DatabaseMetadata database,
            final ImportConfig importConfig,
            final String moduleName,
            final List<String> tablesInOrder,
            final ImportMaintenanceObserver observer) {
        postDataModuleImport(database, importConfig, moduleName, tablesInOrder);
    }

    void postDatabaseImport(DatabaseMetadata database, ImportConfig importConfig);

    default void postDatabaseImport(
            final DatabaseMetadata database,
            final ImportConfig importConfig,
            final ImportMaintenanceObserver observer) {
        postDatabaseImport(database, importConfig);
    }

    default boolean supportsAssertMacros() {
        return false;
    }

    default boolean supportsStandardImportScripts() {
        return false;
    }

    List<String> columnNamesForTable(String tableName);

    List<String> primaryKeyColumnNamesForTable(String tableName);

    QueryResult query(String sql);

    QueryResult verifySchemaConstraints(String schemaName);

    void setupMigrations();

    boolean shouldMigrate(String migrationName);

    void markMigrationAsRun(String migrationName);

    String generateStandardImportSql(
            String tableName, String targetDatabase, String sourceDatabase, List<String> columns);

    String generateStandardSequenceImportSql(String sequenceName, String targetDatabase, String sourceDatabase);

    String generateDefaultSequenceExportSql(String sequenceName);
}
