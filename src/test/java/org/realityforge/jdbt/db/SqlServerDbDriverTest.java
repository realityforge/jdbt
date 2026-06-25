package org.realityforge.jdbt.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.config.ImportConfig;

final class SqlServerDbDriverTest {
    private final DatabaseConnection config = new DatabaseConnection("127.0.0.1", 1433, "DB", "sa", "secret");

    @Test
    void openAndCloseLifecycleManagesConnections() throws Exception {
        final var target = mock(Connection.class);
        final var control = mock(Connection.class);
        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> controlDatabase ? control : target);

        driver.open(config, false);
        driver.close();

        verify(target).close();
    }

    @Test
    void executeUsesControlConnectionWhenRequested() throws Exception {
        final var target = mock(Connection.class);
        final var control = mock(Connection.class);
        final var targetStatement = mock(Statement.class);
        final var controlStatement = mock(Statement.class);
        when(target.createStatement()).thenReturn(targetStatement);
        when(control.createStatement()).thenReturn(controlStatement);
        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> controlDatabase ? control : target);
        driver.open(config, false);

        driver.execute("SELECT 1", false);
        driver.execute("SELECT 2", true);

        verify(targetStatement).execute("SELECT 1");
        verify(controlStatement).execute("SELECT 2");
    }

    @Test
    void createDatabaseUsesVersionedFilePathsAndVersionMetadata() throws Exception {
        final var control = mock(Connection.class);
        final var target = mock(Connection.class);
        final var statement = mock(Statement.class);
        when(control.createStatement()).thenReturn(statement);
        final var targetStatement = mock(Statement.class);
        when(target.createStatement()).thenReturn(targetStatement);

        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> controlDatabase ? control : target);
        driver.open(config, true);
        final var metadata =
                new DatabaseMetadata("default", "Version.1", "hash", "C:\\data", "C:\\log", false, true, true, false);

        driver.createDatabase(metadata, config);

        verify(statement)
                .execute("CREATE DATABASE [DB] ON PRIMARY (NAME = [DB_Version_1],"
                        + " FILENAME='C:\\data\\DB_Version_1.mdf') LOG ON (NAME = [DB_Version_1_LOG],"
                        + " FILENAME='C:\\log\\DB_Version_1.ldf')");
        verify(targetStatement)
                .execute("EXEC sys.sp_addextendedproperty @name = N'DatabaseSchemaVersion', @value = N'Version.1'");
    }

    @Test
    void dropDatabaseUsesRubyDefaultControlSql() throws Exception {
        final var control = mock(Connection.class);
        final var statement = mock(Statement.class);
        when(control.createStatement()).thenReturn(statement);
        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> control);
        driver.open(config, true);

        driver.drop(new DatabaseMetadata("default", "1", "hash"), config);

        verify(statement).execute("SET DEADLOCK_PRIORITY HIGH");
        verify(statement).execute("EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N'DB'");
        verify(statement)
                .execute("IF EXISTS (SELECT * FROM sys.master_files WHERE state = 0 AND db_name(database_id) = 'DB')"
                        + " DROP DATABASE [DB]");
        verify(statement, never()).execute(contains("SINGLE_USER"));
    }

    @Test
    void dropDatabaseHonorsForceDropAndBackupHistoryOptions() throws Exception {
        final var control = mock(Connection.class);
        final var statement = mock(Statement.class);
        when(control.createStatement()).thenReturn(statement);
        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> control);
        driver.open(config, true);
        final var metadata = new DatabaseMetadata("default", "1", "hash", null, null, true, false, true, false);

        driver.drop(metadata, config);

        verify(statement).execute("SET DEADLOCK_PRIORITY HIGH");
        verify(statement, never()).execute(contains("sp_delete_database_backuphistory"));
        verify(statement)
                .execute("IF EXISTS (SELECT * FROM sys.master_files WHERE state = 0 AND db_name(database_id) = 'DB')"
                        + " ALTER DATABASE [DB] SET SINGLE_USER WITH ROLLBACK IMMEDIATE");
        verify(statement)
                .execute("IF EXISTS (SELECT * FROM sys.master_files WHERE state = 0 AND db_name(database_id) = 'DB')"
                        + " DROP DATABASE [DB]");
    }

    @Test
    void insertAndColumnNamesUsePreparedStatements() throws Exception {
        final var target = mock(Connection.class);
        final var insert = mock(PreparedStatement.class);
        final var columns = mock(PreparedStatement.class);
        final var columnResult = mock(ResultSet.class);
        when(target.prepareStatement(anyString())).thenAnswer(invocation -> {
            final var sql = invocation.<String>getArgument(0);
            return sql.startsWith("INSERT INTO") ? insert : columns;
        });
        when(columns.executeQuery()).thenReturn(columnResult);
        when(columnResult.next()).thenReturn(true, true, false);
        when(columnResult.getString(1)).thenReturn("ID", "NAME");
        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> target);
        driver.open(config, false);

        final var record = new LinkedHashMap<String, Object>();
        record.put("ID", 1);
        record.put("NAME", "A");
        driver.insert("[dbo].[tbl]", record);
        final var columnNames = driver.columnNamesForTable("[dbo].[tbl]");

        verify(insert).setObject(1, 1);
        verify(insert).setObject(2, "A");
        verify(insert).executeUpdate();
        assertThat(columnNames).containsExactly("[ID]", "[NAME]");
    }

    @Test
    void primaryKeysAndQueryUseJdbcMetadata() throws Exception {
        final var target = mock(Connection.class);
        final var primaryKeys = mock(PreparedStatement.class);
        final var primaryKeyResult = mock(ResultSet.class);
        when(target.prepareStatement(contains("INFORMATION_SCHEMA.TABLE_CONSTRAINTS")))
                .thenReturn(primaryKeys);
        when(primaryKeys.executeQuery()).thenReturn(primaryKeyResult);
        when(primaryKeyResult.next()).thenReturn(true, true, false);
        when(primaryKeyResult.getString(1)).thenReturn("A", "B");

        final var statement = mock(Statement.class);
        final var queryResult = mock(ResultSet.class);
        final var metadata = mock(ResultSetMetaData.class);
        when(target.createStatement()).thenReturn(statement);
        when(statement.executeQuery("SELECT A, B FROM [Core].[tbl]")).thenReturn(queryResult);
        when(queryResult.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(2);
        when(metadata.getColumnLabel(1)).thenReturn("A");
        when(metadata.getColumnLabel(2)).thenReturn("B");
        when(queryResult.next()).thenReturn(true, false);
        when(queryResult.getObject(1)).thenReturn(1);
        when(queryResult.getObject(2)).thenReturn("two");

        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> target);
        driver.open(config, false);

        assertThat(driver.primaryKeyColumnNamesForTable("[Core].[tbl]")).containsExactly("[A]", "[B]");
        assertThat(driver.query("SELECT A, B FROM [Core].[tbl]"))
                .isEqualTo(new QueryResult(List.of("A", "B"), List.of(List.of(1, "two"))));
        assertThat(driver.generateDefaultSequenceExportSql("[Core].[seq]"))
                .isEqualTo("SELECT CAST(current_value AS BIGINT) FROM sys.sequences WHERE object_id ="
                        + " OBJECT_ID('[Core].[seq]')");
    }

    @Test
    void fixtureImportTogglesIdentityInsertWhenIdentityPresent() throws Exception {
        final var target = mock(Connection.class);
        final var identityQuery = mock(PreparedStatement.class);
        final var identityResult = mock(ResultSet.class);
        when(target.prepareStatement(
                        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(OBJECT_ID(?),"
                                + " COLUMN_NAME, 'IsIdentity') = 1"))
                .thenReturn(identityQuery);
        when(identityQuery.executeQuery()).thenReturn(identityResult);
        when(identityResult.next()).thenReturn(true, true);
        when(identityResult.getLong(1)).thenReturn(1L, 1L);

        final var statement = mock(Statement.class);
        when(target.createStatement()).thenReturn(statement);

        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> target);
        driver.open(config, false);
        driver.preFixtureImport("[dbo].[tbl]");
        driver.postFixtureImport("[dbo].[tbl]");
        final var metadata = new DatabaseMetadata("default", "1", "hash");
        driver.preTableImport(
                metadata, new ImportConfig("default", List.of(), "import", List.of(), List.of()), "[dbo].[tbl]");
        driver.postTableImport(
                metadata, new ImportConfig("default", List.of(), "import", List.of(), List.of()), "[dbo].[tbl]");

        verify(statement, times(2)).execute("SET IDENTITY_INSERT [dbo].[tbl] ON");
        verify(statement, times(2)).execute("SET IDENTITY_INSERT [dbo].[tbl] OFF");
        verify(statement).execute("DBCC DBREINDEX (N'[dbo].[tbl]', '', 0) WITH NO_INFOMSGS");
    }

    @Test
    void postImportMaintenanceHonorsReindexAndShrinkOptions() throws Exception {
        final var target = mock(Connection.class);
        final var statement = mock(Statement.class);
        when(target.createStatement()).thenReturn(statement);
        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> target);
        driver.open(config, false);
        final var importConfig = new ImportConfig("default", List.of("Core"), "import", List.of(), List.of());
        final var noMaintenance = new DatabaseMetadata("default", "1", "hash", null, null, false, true, false, false);

        driver.postDatabaseImport(noMaintenance, importConfig);
        driver.postDataModuleImport(noMaintenance, importConfig, "Core", List.of("[Core].[foo]"));

        verify(statement, never()).execute(contains("sp_updatestats"));
        verify(statement, never()).execute(contains("SHRINKDATABASE"));
        verify(statement, never()).execute(contains("DBREINDEX"));

        final var shrinkAndReindex = new DatabaseMetadata("default", "1", "hash", null, null, false, true, true, true);
        driver.postDataModuleImport(shrinkAndReindex, importConfig, "Core", List.of("[Core].[foo]", "[Core].[bar]"));
        driver.postDatabaseImport(shrinkAndReindex, importConfig);

        verify(statement)
                .execute("DECLARE @DbName VARCHAR(100); SET @DbName = DB_NAME(); DBCC SHRINKDATABASE(@DbName, 10,"
                        + " NOTRUNCATE) WITH NO_INFOMSGS");
        verify(statement)
                .execute("DECLARE @DbName VARCHAR(100); SET @DbName = DB_NAME(); DBCC SHRINKDATABASE(@DbName, 10,"
                        + " TRUNCATEONLY) WITH NO_INFOMSGS");
        verify(statement).execute("DBCC DBREINDEX (N'[Core].[foo]', '', 0) WITH NO_INFOMSGS");
        verify(statement).execute("DBCC DBREINDEX (N'[Core].[bar]', '', 0) WITH NO_INFOMSGS");
        verify(statement).execute("EXEC dbo.sp_updatestats");
        verify(statement)
                .execute("DECLARE @DbName VARCHAR(100); SET @DbName = DB_NAME(); DBCC UPDATEUSAGE(@DbName) WITH"
                        + " NO_INFOMSGS, COUNT_ROWS");
    }

    @Test
    void migrationMethodsCreateAndQueryMigrationTable() throws Exception {
        final var target = mock(Connection.class);
        final var tableExists = mock(PreparedStatement.class);
        final var tableExistsResult = mock(ResultSet.class);
        final var shouldMigrate = mock(PreparedStatement.class);
        final var shouldMigrateResult = mock(ResultSet.class);
        final var markMigration = mock(PreparedStatement.class);
        when(target.prepareStatement(anyString())).thenAnswer(invocation -> {
            final var sql = invocation.<String>getArgument(0);
            if (sql.contains("INFORMATION_SCHEMA.TABLES")) {
                return tableExists;
            }
            if (sql.contains("FROM [dbo].[tblMigration] WHERE")) {
                return shouldMigrate;
            }
            if (sql.startsWith("INSERT INTO [dbo].[tblMigration]")) {
                return markMigration;
            }
            throw new IllegalStateException("Unexpected sql " + sql);
        });
        when(tableExists.executeQuery()).thenReturn(tableExistsResult);
        when(tableExistsResult.next()).thenReturn(true, true);
        when(tableExistsResult.getLong(1)).thenReturn(1L, 1L);
        when(shouldMigrate.executeQuery()).thenReturn(shouldMigrateResult);
        when(shouldMigrateResult.next()).thenReturn(true);
        when(shouldMigrateResult.getLong(1)).thenReturn(0L);

        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> target);
        driver.open(config, false);

        assertThat(driver.shouldMigrate("default", "001_init")).isTrue();
        driver.markMigrationAsRun("default", "001_init");

        verify(markMigration).setString(1, "default");
        verify(markMigration).setString(2, "001_init");
        verify(markMigration).executeUpdate();
    }
}
