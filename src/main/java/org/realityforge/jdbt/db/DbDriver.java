package org.realityforge.jdbt.db;

import java.util.List;
import java.util.Map;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.runtime.RuntimeDatabase;

public interface DbDriver {
    void open(DatabaseConnection connection, boolean openControlDatabase);

    void close();

    void drop(RuntimeDatabase database, DatabaseConnection connection);

    void createDatabase(RuntimeDatabase database, DatabaseConnection connection);

    void createSchema(String schemaName);

    void dropSchema(String schemaName, List<String> tablesInDropOrder);

    void execute(String sql, boolean executeInControlDatabase);

    void preFixtureImport(String tableName);

    void insert(String tableName, Map<String, Object> record);

    void postFixtureImport(String tableName);

    void updateSequence(String sequenceName, long value);

    void preTableImport(ImportConfig importConfig, String tableName);

    void postTableImport(ImportConfig importConfig, String tableName);

    void postDataModuleImport(ImportConfig importConfig, String moduleName);

    void postDatabaseImport(ImportConfig importConfig);

    List<String> columnNamesForTable(String tableName);
}
