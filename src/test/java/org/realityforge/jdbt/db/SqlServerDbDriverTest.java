package org.realityforge.jdbt.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.repository.RepositoryConfig;
import org.realityforge.jdbt.runtime.RuntimeDatabase;

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
    void createAndDropDatabaseUseControlConnection() throws Exception {
        final var control = mock(Connection.class);
        final var exists = mock(PreparedStatement.class);
        final var existsResult = mock(ResultSet.class);
        when(control.prepareStatement("SELECT COUNT(*) FROM sys.databases WHERE name = ?"))
                .thenReturn(exists);
        when(exists.executeQuery()).thenReturn(existsResult);
        when(existsResult.next()).thenReturn(true, true);
        when(existsResult.getLong(1)).thenReturn(0L, 1L);

        final var statement = mock(Statement.class);
        when(control.createStatement()).thenReturn(statement);

        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> control);
        driver.open(config, true);
        final var database = runtimeDatabase();

        driver.createDatabase(database, config);
        driver.drop(database, config);

        verify(statement).execute("CREATE DATABASE [DB]");
        verify(statement).execute("ALTER DATABASE [DB] SET SINGLE_USER WITH ROLLBACK IMMEDIATE");
        verify(statement).execute("DROP DATABASE [DB]");
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
    void fixtureImportTogglesIdentityInsertWhenIdentityPresent() throws Exception {
        final var target = mock(Connection.class);
        final var identityQuery = mock(PreparedStatement.class);
        final var identityResult = mock(ResultSet.class);
        when(target.prepareStatement(
                        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(OBJECT_ID(?), COLUMN_NAME, 'IsIdentity') = 1"))
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
        driver.preTableImport(new ImportConfig("default", List.of(), "import", List.of(), List.of()), "[dbo].[tbl]");
        driver.postTableImport(new ImportConfig("default", List.of(), "import", List.of(), List.of()), "[dbo].[tbl]");

        verify(statement, times(2)).execute("SET IDENTITY_INSERT [dbo].[tbl] ON");
        verify(statement, times(2)).execute("SET IDENTITY_INSERT [dbo].[tbl] OFF");
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

    private static RuntimeDatabase runtimeDatabase() {
        return new RuntimeDatabase(
                "default",
                new RepositoryConfig(
                        List.of("MyModule"),
                        Map.of(),
                        Map.of("MyModule", List.of("[MyModule].[foo]")),
                        Map.of("MyModule", List.of())),
                List.of(Path.of(".")),
                List.of(),
                List.of(),
                "index.txt",
                List.of("."),
                List.of("down"),
                List.of("finalize"),
                List.of("db-hooks/pre"),
                List.of("db-hooks/post"),
                "fixtures",
                "datasets",
                List.of("pre"),
                List.of("post"),
                List.of("seed"),
                true,
                false,
                "migrations",
                "1",
                "hash",
                Map.of("default", new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of())),
                Map.of());
    }
}
