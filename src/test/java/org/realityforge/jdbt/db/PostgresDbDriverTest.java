package org.realityforge.jdbt.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.runtime.RuntimeExecutionException;

final class PostgresDbDriverTest {
    private final DatabaseConnection config = new DatabaseConnection("127.0.0.1", 5432, "db", "postgres", "secret");

    @Test
    void createAndDropDatabaseUseControlConnection() throws Exception {
        final var control = mock(Connection.class);
        final var statement = mock(Statement.class);
        when(control.createStatement()).thenReturn(statement);

        final var driver = new PostgresDbDriver((connection, controlDatabase) -> control);
        driver.open(config, true);
        driver.createDatabase(null, config);
        driver.drop(null, config);

        verify(statement).execute("CREATE DATABASE \"db\"");
        verify(statement).execute("DROP DATABASE IF EXISTS \"db\"");
    }

    @Test
    void createSchemaAndDropSchemaUseExpectedSql() throws Exception {
        final var target = mock(Connection.class);
        final var schemaExists = mock(PreparedStatement.class);
        final var schemaExistsResult = mock(ResultSet.class);
        when(target.prepareStatement("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?"))
                .thenReturn(schemaExists);
        when(schemaExists.executeQuery()).thenReturn(schemaExistsResult);
        when(schemaExistsResult.next()).thenReturn(true);
        when(schemaExistsResult.getLong(1)).thenReturn(0L);

        final var statement = mock(Statement.class);
        when(target.createStatement()).thenReturn(statement);

        final var driver = new PostgresDbDriver((connection, controlDatabase) -> target);
        driver.open(config, false);
        driver.createSchema("core");
        driver.dropSchema("core", List.of());

        verify(statement).execute("CREATE SCHEMA \"core\"");
        verify(statement).execute("DROP SCHEMA IF EXISTS \"core\" CASCADE");
    }

    @Test
    void insertAndColumnLookupUsePreparedStatements() throws Exception {
        final var target = mock(Connection.class);
        final var insert = mock(PreparedStatement.class);
        final var columns = mock(PreparedStatement.class);
        final var columnResults = mock(ResultSet.class);
        when(target.prepareStatement(anyString())).thenAnswer(invocation -> {
            final var sql = invocation.<String>getArgument(0);
            return sql.startsWith("INSERT INTO") ? insert : columns;
        });
        when(columns.executeQuery()).thenReturn(columnResults);
        when(columnResults.next()).thenReturn(true, true, false);
        when(columnResults.getString(1)).thenReturn("id", "name");

        final var driver = new PostgresDbDriver((connection, controlDatabase) -> target);
        driver.open(config, false);

        final var record = new LinkedHashMap<String, Object>();
        record.put("id", 1);
        record.put("name", "A");
        driver.insert("public.tbl", record);
        final var columnsResult = driver.columnNamesForTable("public.tbl");

        verify(insert).setObject(1, 1);
        verify(insert).setObject(2, "A");
        verify(insert).executeUpdate();
        assertThat(columnsResult).containsExactly("\"id\"", "\"name\"");
    }

    @Test
    void migrationMethodsUsePostgresSyntax() throws Exception {
        final var target = mock(Connection.class);
        final var tableExists = mock(PreparedStatement.class);
        final var tableExistsResult = mock(ResultSet.class);
        final var shouldMigrate = mock(PreparedStatement.class);
        final var shouldMigrateResult = mock(ResultSet.class);
        final var markMigration = mock(PreparedStatement.class);
        when(target.prepareStatement(anyString())).thenAnswer(invocation -> {
            final var sql = invocation.<String>getArgument(0);
            if (sql.contains("information_schema.tables")) {
                return tableExists;
            }
            if (sql.contains("FROM \"tblMigration\" WHERE")) {
                return shouldMigrate;
            }
            if (sql.startsWith("INSERT INTO \"tblMigration\"")) {
                return markMigration;
            }
            throw new IllegalStateException("Unexpected SQL " + sql);
        });
        when(tableExists.executeQuery()).thenReturn(tableExistsResult);
        when(tableExistsResult.next()).thenReturn(true, true);
        when(tableExistsResult.getLong(1)).thenReturn(1L, 1L);
        when(shouldMigrate.executeQuery()).thenReturn(shouldMigrateResult);
        when(shouldMigrateResult.next()).thenReturn(true);
        when(shouldMigrateResult.getLong(1)).thenReturn(0L);

        final var driver = new PostgresDbDriver((connection, controlDatabase) -> target);
        driver.open(config, false);

        assertThat(driver.shouldMigrate("default", "001")).isTrue();
        driver.markMigrationAsRun("default", "001");

        verify(markMigration).setString(1, "default");
        verify(markMigration).setString(2, "001");
        verify(markMigration).executeUpdate();
    }

    @Test
    void standardImportSqlRequiresSameDatabaseForPostgres() {
        final var driver = new PostgresDbDriver((connection, controlDatabase) -> mock(Connection.class));
        assertThatThrownBy(() -> driver.generateStandardImportSql("public.tbl", "target", "source", List.of("\"id\"")))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("across databases is not supported");

        assertThat(driver.generateStandardImportSql("public.tbl", "same", "same", List.of("\"id\"")))
                .contains("INSERT INTO public.tbl")
                .contains("SELECT \"id\"");
    }
}
