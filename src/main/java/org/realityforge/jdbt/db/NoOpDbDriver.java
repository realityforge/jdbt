package org.realityforge.jdbt.db;

import java.util.List;
import java.util.Map;
import org.realityforge.jdbt.config.ImportConfig;

final class NoOpDbDriver implements DbDriver {
    @Override
    public void open(final DatabaseConnection connection, final boolean openControlDatabase) {}

    @Override
    public void close() {}

    @Override
    public void drop(final DatabaseMetadata database, final DatabaseConnection connection) {}

    @Override
    public void createDatabase(final DatabaseMetadata database, final DatabaseConnection connection) {}

    @Override
    public void createSchema(final String schemaName) {}

    @Override
    public void dropSchema(final String schemaName, final List<String> tablesInDropOrder) {}

    @Override
    public void execute(final String sql, final boolean executeInControlDatabase) {}

    @Override
    public void preFixtureImport(final String tableName) {}

    @Override
    public void insert(final String tableName, final Map<String, Object> record) {}

    @Override
    public void postFixtureImport(final String tableName) {}

    @Override
    public void updateSequence(final String sequenceName, final long value) {}

    @Override
    public void preTableImport(
            final DatabaseMetadata database, final ImportConfig importConfig, final String tableName) {}

    @Override
    public void postTableImport(
            final DatabaseMetadata database, final ImportConfig importConfig, final String tableName) {}

    @Override
    public void postDataModuleImport(
            final DatabaseMetadata database,
            final ImportConfig importConfig,
            final String moduleName,
            final List<String> tablesInOrder) {}

    @Override
    public void postDatabaseImport(final DatabaseMetadata database, final ImportConfig importConfig) {}

    @Override
    public List<String> columnNamesForTable(final String tableName) {
        return List.of();
    }

    @Override
    public List<String> primaryKeyColumnNamesForTable(final String tableName) {
        return List.of();
    }

    @Override
    public QueryResult query(final String sql) {
        return new QueryResult(List.of(), List.of());
    }

    @Override
    public void setupMigrations() {}

    @Override
    public boolean shouldMigrate(final String namespace, final String migrationName) {
        return true;
    }

    @Override
    public void markMigrationAsRun(final String namespace, final String migrationName) {}

    @Override
    public String generateStandardImportSql(
            final String tableName,
            final String targetDatabase,
            final String sourceDatabase,
            final List<String> columns) {
        return "INSERT INTO ["
                + targetDatabase
                + "]."
                + tableName
                + '(' + String.join(", ", columns)
                + ")\n  SELECT "
                + String.join(", ", columns)
                + " FROM ["
                + sourceDatabase
                + "]."
                + tableName
                + "\n";
    }

    @Override
    public String generateStandardSequenceImportSql(
            final String sequenceName, final String targetDatabase, final String sourceDatabase) {
        return "DECLARE @Next VARCHAR(50);\n"
                + "SELECT @Next = CAST(current_value AS BIGINT) + 1 FROM ["
                + sourceDatabase
                + "].sys.sequences "
                + "WHERE object_id = OBJECT_ID('["
                + sourceDatabase
                + "]."
                + sequenceName
                + "');\n"
                + "SET @Next = COALESCE(@Next,'1');"
                + "EXEC('USE ["
                + targetDatabase
                + "]; ALTER SEQUENCE "
                + sequenceName
                + " RESTART WITH ' + @Next );";
    }

    @Override
    public String generateDefaultSequenceExportSql(final String sequenceName) {
        return "SELECT 1";
    }
}
