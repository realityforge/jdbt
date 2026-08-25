package org.realityforge.jdbt.db.sqlserver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.db.DatabaseConnection;
import org.realityforge.jdbt.db.DatabaseException;
import org.realityforge.jdbt.db.DatabaseMetadata;
import org.realityforge.jdbt.db.ImportMaintenanceObserver;
import org.realityforge.jdbt.db.QueryResult;
import org.realityforge.jdbt.db.SqlTimingObservation;

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
    void executeUsesTargetSessionAndTemporarilySelectsControlCatalogWhenTargetIsOpen() throws Exception {
        final var target = mock(Connection.class);
        final var control = mock(Connection.class);
        final var targetStatement = mock(Statement.class);
        when(target.getCatalog()).thenReturn("DB");
        when(target.createStatement()).thenReturn(targetStatement);
        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> controlDatabase ? control : target);
        driver.open(config, false);

        driver.execute("SELECT 1", false);
        driver.execute("SELECT 2", true);

        verify(targetStatement).execute("SELECT 1");
        final var ordered = inOrder(target, targetStatement);
        ordered.verify(target).getCatalog();
        ordered.verify(target).setCatalog("msdb");
        ordered.verify(target).createStatement();
        ordered.verify(targetStatement).execute("SELECT 2");
        ordered.verify(target).setCatalog("DB");
        verify(control, never()).createStatement();
    }

    @Test
    void executeRestoresTargetCatalogWhenControlSqlFails() throws Exception {
        final var target = mock(Connection.class);
        final var control = mock(Connection.class);
        final var statement = mock(Statement.class);
        when(target.getCatalog()).thenReturn("DB");
        when(target.createStatement()).thenReturn(statement);
        doThrow(new SQLException("boom")).when(statement).execute("FAIL");
        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> controlDatabase ? control : target);
        driver.open(config, false);

        assertThatThrownBy(() -> driver.execute("FAIL", true))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("Failed to execute SQL");

        final var ordered = inOrder(target, statement);
        ordered.verify(target).getCatalog();
        ordered.verify(target).setCatalog("msdb");
        ordered.verify(target).createStatement();
        ordered.verify(statement).execute("FAIL");
        ordered.verify(target).setCatalog("DB");
        verify(control, never()).createStatement();
    }

    @Test
    void importTimingUsesVersionedReadOnlySessionContext() throws Exception {
        final var target = mock(Connection.class);
        final var statement = mock(Statement.class);
        when(target.createStatement()).thenReturn(statement);
        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> target);
        driver.open(config, false);

        driver.enableImportTiming();

        verify(statement).execute(contains("[sys].[sp_getapplock]"));
        verify(statement).execute(contains("CREATE TABLE [dbo].[tblImportTiming]"));
        verify(statement).execute(contains("[sys].[sp_set_session_context]"));
    }

    @Test
    void timedExecutionTraversesResultsAndReadsOnlyExactProtocolRows() throws Exception {
        final var target = mock(Connection.class);
        final var statement = mock(Statement.class);
        final var arbitraryResult = mock(ResultSet.class);
        final var arbitraryMetadata = mock(ResultSetMetaData.class);
        final var timingResult = mock(ResultSet.class);
        final var timingMetadata = timingMetadata();
        final var clearStatement = mock(Statement.class);
        when(statement.execute("MULTI")).thenReturn(true);
        when(statement.getResultSet()).thenReturn(arbitraryResult, timingResult);
        when(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT)).thenReturn(false, true, false);
        when(statement.getUpdateCount()).thenReturn(2, -1);
        when(arbitraryResult.getMetaData()).thenReturn(arbitraryMetadata);
        when(arbitraryMetadata.getColumnCount()).thenReturn(1);
        when(arbitraryMetadata.getColumnLabel(1)).thenReturn("SecretValue");
        when(timingResult.getMetaData()).thenReturn(timingMetadata);
        when(timingResult.next()).thenReturn(true, true, false);
        when(timingResult.getString(1)).thenReturn("jdbt.timing.v1");
        when(timingResult.getLong(2)).thenReturn(1L, 2L);
        when(timingResult.getString(3)).thenReturn("analysis/a", "phase/root");
        when(timingResult.getString(4)).thenReturn("phase/root", "__JDBT_ACTIVE_SQL_BATCH__");
        when(timingResult.getString(5)).thenReturn("analysis_corruption_check", "phase");
        when(timingResult.getString(6)).thenReturn("succeeded");
        when(timingResult.getLong(7)).thenReturn(12L, 34L);
        when(timingResult.wasNull()).thenReturn(false);
        final var observations = new ArrayList<SqlTimingObservation>();
        final var driver = timedDriver(target, statement, clearStatement);

        driver.execute("MULTI", false, observations::add);

        assertThat(observations)
                .containsExactly(
                        new SqlTimingObservation(
                                1L, "analysis/a", "phase/root", "analysis_corruption_check", "succeeded", 12L),
                        new SqlTimingObservation(
                                2L, "phase/root", "__JDBT_ACTIVE_SQL_BATCH__", "phase", "succeeded", 34L));
        verify(arbitraryResult, never()).next();
        verify(arbitraryResult, never()).getString(1);
        verify(arbitraryResult).close();
        verify(timingResult).close();
        verify(statement, times(3)).getMoreResults(Statement.CLOSE_CURRENT_RESULT);
        verify(clearStatement).execute("TRUNCATE TABLE [DB].[dbo].[tblImportTiming]");
    }

    @Test
    void timedExecutionDrainsAndRetainsSinkAfterSqlFailure() throws Exception {
        final var target = mock(Connection.class);
        final var failingStatement = mock(Statement.class);
        final var drainStatement = mock(Statement.class);
        final var result = mock(ResultSet.class);
        final var metadata = timingMetadata();
        final var sqlFailure = new SQLException("primary database detail", "S0002", 208);
        when(failingStatement.execute("FAIL")).thenThrow(sqlFailure);
        when(drainStatement.execute(anyString())).thenReturn(true);
        when(drainStatement.getResultSet()).thenReturn(result);
        when(drainStatement.getMoreResults(Statement.CLOSE_CURRENT_RESULT)).thenReturn(false);
        when(drainStatement.getUpdateCount()).thenReturn(-1);
        when(result.getMetaData()).thenReturn(metadata);
        when(result.next()).thenReturn(true, false);
        when(result.getString(1)).thenReturn("jdbt.timing.v1");
        when(result.getLong(2)).thenReturn(1L);
        when(result.getString(3)).thenReturn("phase/final-validation");
        when(result.getString(4)).thenReturn("__JDBT_ACTIVE_SQL_BATCH__");
        when(result.getString(5)).thenReturn("phase");
        when(result.getString(6)).thenReturn("failed");
        when(result.getLong(7)).thenReturn(19L);
        when(result.wasNull()).thenReturn(false);
        final var observations = new ArrayList<SqlTimingObservation>();
        final var driver = timedDriver(target, failingStatement, drainStatement);

        final DatabaseException error;
        try {
            driver.execute("FAIL", false, observations::add);
            throw new AssertionError("Expected database failure");
        } catch (final DatabaseException expected) {
            error = expected;
        }

        assertThat(error).hasNoSuppressedExceptions();
        assertThat(error.getCause()).isSameAs(sqlFailure);
        assertThat(observations)
                .containsExactly(new SqlTimingObservation(
                        1L, "phase/final-validation", "__JDBT_ACTIVE_SQL_BATCH__", "phase", "failed", 19L));
        verify(drainStatement).execute(contains("FROM [DB].[dbo].[tblImportTiming] ORDER BY [Ordinal]"));
        verify(result).close();
    }

    @Test
    void timedExecutionKeepsSqlFailurePrimaryWhenDrainFails() throws Exception {
        final var target = mock(Connection.class);
        final var failingStatement = mock(Statement.class);
        final var drainStatement = mock(Statement.class);
        final var sqlFailure = new SQLException("primary secret", "S0002", 208);
        when(failingStatement.execute("FAIL")).thenThrow(sqlFailure);
        when(drainStatement.execute(anyString())).thenThrow(new SQLException("drain credential secret"));
        final var driver = timedDriver(target, failingStatement, drainStatement);

        final DatabaseException error;
        try {
            driver.execute("FAIL", false, ignored -> {});
            throw new AssertionError("Expected database failure");
        } catch (final DatabaseException expected) {
            error = expected;
        }

        assertThat(error.getCause()).isSameAs(sqlFailure);
        assertThat(sqlFailure.getErrorCode()).isEqualTo(208);
        assertThat(sqlFailure.getSQLState()).isEqualTo("S0002");
        assertThat(error.getSuppressed())
                .extracting(Throwable::getMessage)
                .containsExactly("Unable to drain SQL import timing after database failure");
        assertThat(error.getSuppressed()).allMatch(failure -> null == failure.getCause());
        assertThat(List.of(error.getSuppressed()).toString()).doesNotContain("credential", "secret", "path");
    }

    @Test
    void timedExecutionPropagatesObserverFailureWhenSqlSucceeded() throws Exception {
        final var target = mock(Connection.class);
        final var statement = mock(Statement.class);
        final var result = mock(ResultSet.class);
        final var metadata = timingMetadata();
        final var observerFailure = new IllegalStateException("observer failure");
        when(statement.execute("TIMING")).thenReturn(true);
        when(statement.getResultSet()).thenReturn(result);
        when(result.getMetaData()).thenReturn(metadata);
        when(result.next()).thenReturn(true);
        when(result.getString(1)).thenReturn("jdbt.timing.v1");
        when(result.getLong(2)).thenReturn(1L);
        when(result.getString(3)).thenReturn("phase/final-validation");
        when(result.getString(4)).thenReturn("__JDBT_ACTIVE_SQL_BATCH__");
        when(result.getString(5)).thenReturn("phase");
        when(result.getString(6)).thenReturn("succeeded");
        when(result.getLong(7)).thenReturn(1L);
        when(result.wasNull()).thenReturn(false);
        final var driver = timedDriver(target, statement);

        assertThatThrownBy(() -> driver.execute("TIMING", false, ignored -> {
                    throw observerFailure;
                }))
                .isSameAs(observerFailure);

        verify(result).close();
    }

    @Test
    void timedExecutionRetainsSinkWhenObserverFails() throws Exception {
        final var target = mock(Connection.class);
        final var statement = mock(Statement.class);
        final var result = mock(ResultSet.class);
        final var metadata = timingMetadata();
        final var observerFailure = new IllegalStateException("observer failure");
        when(statement.execute("TIMING")).thenReturn(true);
        when(statement.getResultSet()).thenReturn(result);
        when(result.getMetaData()).thenReturn(metadata);
        when(result.next()).thenReturn(true);
        when(result.getString(1)).thenReturn("jdbt.timing.v1");
        when(result.getLong(2)).thenReturn(1L);
        when(result.getString(3)).thenReturn("phase/final-validation");
        when(result.getString(4)).thenReturn("__JDBT_ACTIVE_SQL_BATCH__");
        when(result.getString(5)).thenReturn("phase");
        when(result.getString(6)).thenReturn("succeeded");
        when(result.getLong(7)).thenReturn(1L);
        when(result.wasNull()).thenReturn(false);
        final var driver = timedDriver(target, statement);

        final RuntimeException error;
        try {
            driver.execute("TIMING", false, ignored -> {
                throw observerFailure;
            });
            throw new AssertionError("Expected observer failure");
        } catch (final RuntimeException expected) {
            error = expected;
        }

        assertThat(error).isSameAs(observerFailure);
        assertThat(error).hasNoSuppressedExceptions();
    }

    @Test
    void timedExecutionRejectsUnsupportedProtocolWithoutReadingPayload() throws Exception {
        final var target = mock(Connection.class);
        final var statement = mock(Statement.class);
        final var result = mock(ResultSet.class);
        final var metadata = timingMetadata();
        when(statement.execute("TIMING")).thenReturn(true);
        when(statement.getResultSet()).thenReturn(result);
        when(result.getMetaData()).thenReturn(metadata);
        when(result.next()).thenReturn(true);
        when(result.getString(1)).thenReturn("jdbt.timing.v2");
        final var driver = timedDriver(target, statement);

        assertThatThrownBy(() -> driver.execute("TIMING", false, ignored -> {}))
                .isInstanceOf(DatabaseException.class)
                .hasMessage("Unsupported SQL import timing protocol");

        verify(result, never()).getString(3);
        verify(result).close();
    }

    @Test
    void timedExecutionRejectsNullRequiredField() throws Exception {
        final var target = mock(Connection.class);
        final var statement = mock(Statement.class);
        final var result = mock(ResultSet.class);
        final var metadata = timingMetadata();
        when(statement.execute("TIMING")).thenReturn(true);
        when(statement.getResultSet()).thenReturn(result);
        when(result.getMetaData()).thenReturn(metadata);
        when(result.next()).thenReturn(true);
        when(result.getString(1)).thenReturn("jdbt.timing.v1");
        when(result.getLong(2)).thenReturn(1L);
        when(result.getString(3)).thenReturn(null);
        when(result.wasNull()).thenReturn(false);
        final var driver = timedDriver(target, statement);

        assertThatThrownBy(() -> driver.execute("TIMING", false, ignored -> {}))
                .isInstanceOf(DatabaseException.class)
                .hasMessage("SQL import timing operation ID must not be null");

        verify(result).close();
    }

    @Test
    void timedExecutionRejectsProtocolColumnTypeMismatchBeforeReadingRows() throws Exception {
        final var target = mock(Connection.class);
        final var statement = mock(Statement.class);
        final var result = mock(ResultSet.class);
        final var metadata = timingMetadata();
        when(metadata.getColumnType(7)).thenReturn(Types.VARCHAR);
        when(statement.execute("TIMING")).thenReturn(true);
        when(statement.getResultSet()).thenReturn(result);
        when(result.getMetaData()).thenReturn(metadata);
        final var driver = timedDriver(target, statement);

        assertThatThrownBy(() -> driver.execute("TIMING", false, ignored -> {}))
                .isInstanceOf(DatabaseException.class)
                .hasMessage("Invalid SQL import timing column type");

        verify(result, never()).next();
        verify(result).close();
    }

    @Test
    void timedExecutionRejectsIncompleteProtocolShapeBeforeReadingRows() throws Exception {
        final var target = mock(Connection.class);
        final var statement = mock(Statement.class);
        final var result = mock(ResultSet.class);
        final var metadata = mock(ResultSetMetaData.class);
        when(statement.execute("TIMING")).thenReturn(true);
        when(statement.getResultSet()).thenReturn(result);
        when(result.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(2);
        when(metadata.getColumnLabel(1)).thenReturn("Protocol");
        final var driver = timedDriver(target, statement);

        assertThatThrownBy(() -> driver.execute("TIMING", false, ignored -> {}))
                .isInstanceOf(DatabaseException.class)
                .hasMessage("Invalid SQL import timing result shape");

        verify(result, never()).next();
        verify(result).close();
    }

    @Test
    void identityToggleAndControlCatalogImportShareTargetSessionInOrder() throws Exception {
        final var target = mock(Connection.class);
        final var control = mock(Connection.class);
        final var identityQuery = mock(PreparedStatement.class);
        final var identityResult = mock(ResultSet.class);
        final var statement = mock(Statement.class);
        when(target.prepareStatement(contains("COLUMNPROPERTY"))).thenReturn(identityQuery);
        when(identityQuery.executeQuery()).thenReturn(identityResult);
        when(identityResult.next()).thenReturn(true, true);
        when(identityResult.getLong(1)).thenReturn(1L, 1L);
        when(target.getCatalog()).thenReturn("DB");
        when(target.createStatement()).thenReturn(statement);
        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> controlDatabase ? control : target);
        final var metadata = new DatabaseMetadata("1", "hash", null, null, false, true, false, false);
        final var importConfig = new ImportConfig("default", List.of("Core"), "import", List.of(), List.of());
        driver.open(config, false);

        driver.preTableImport(metadata, importConfig, "[Core].[tbl]");
        driver.execute("INSERT IMPORT ROWS", true);
        driver.postTableImport(metadata, importConfig, "[Core].[tbl]");

        final var ordered = inOrder(target, statement);
        ordered.verify(target).createStatement();
        ordered.verify(statement).execute("SET IDENTITY_INSERT [Core].[tbl] ON");
        ordered.verify(target).getCatalog();
        ordered.verify(target).setCatalog("msdb");
        ordered.verify(target).createStatement();
        ordered.verify(statement).execute("INSERT IMPORT ROWS");
        ordered.verify(target).setCatalog("DB");
        ordered.verify(target).createStatement();
        ordered.verify(statement).execute("SET IDENTITY_INSERT [Core].[tbl] OFF");
        verify(control, never()).createStatement();
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
        final var metadata = new DatabaseMetadata("Version.1", "hash", "C:\\data", "C:\\log", false, true, true, false);

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

        driver.drop(new DatabaseMetadata("1", "hash"), config);

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
        final var metadata = new DatabaseMetadata("1", "hash", null, null, true, false, true, false);

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
    void verifySchemaConstraintsReturnsOnlyViolations() throws Exception {
        final var target = mock(Connection.class);
        final var statement = mock(Statement.class);
        final var result = mock(ResultSet.class);
        final var metadata = mock(ResultSetMetaData.class);
        when(target.createStatement()).thenReturn(statement);
        when(statement.executeQuery("EXEC [Core].spCheckConstraints")).thenReturn(result);
        when(result.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(2);
        when(metadata.getColumnLabel(1)).thenReturn("ConstraintName");
        when(metadata.getColumnLabel(2)).thenReturn("IsViolation");
        when(result.next()).thenReturn(true, true, false);
        when(result.getObject(1)).thenReturn("CK_Valid", "CK_Invalid");
        when(result.getObject(2)).thenReturn(false, true);
        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> target);
        driver.open(config, false);

        assertThat(driver.verifySchemaConstraints("Core"))
                .isEqualTo(new QueryResult(
                        List.of("ConstraintName", "IsViolation"), List.of(List.of("CK_Invalid", true))));
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
        final var metadata = new DatabaseMetadata("1", "hash");
        driver.preTableImport(
                metadata, new ImportConfig("default", List.of(), "import", List.of(), List.of()), "[dbo].[tbl]");
        driver.postTableImport(
                metadata, new ImportConfig("default", List.of(), "import", List.of(), List.of()), "[dbo].[tbl]");

        verify(statement, times(2)).execute("SET IDENTITY_INSERT [dbo].[tbl] ON");
        verify(statement, times(2)).execute("SET IDENTITY_INSERT [dbo].[tbl] OFF");
        verify(statement).execute("DBCC DBREINDEX (N'[dbo].[tbl]', '', 0) WITH NO_INFOMSGS");
    }

    @Test
    void fixtureImportDoesNotToggleIdentityInsertWhenIdentityAbsent() throws Exception {
        final var target = mock(Connection.class);
        final var identityQuery = mock(PreparedStatement.class);
        final var identityResult = mock(ResultSet.class);
        when(target.prepareStatement(contains("COLUMNPROPERTY"))).thenReturn(identityQuery);
        when(identityQuery.executeQuery()).thenReturn(identityResult);
        when(identityResult.next()).thenReturn(true, true);
        when(identityResult.getLong(1)).thenReturn(0L, 0L);
        final var statement = mock(Statement.class);
        when(target.createStatement()).thenReturn(statement);
        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> target);
        driver.open(config, false);

        driver.preFixtureImport("[dbo].[tbl]");
        driver.postFixtureImport("[dbo].[tbl]");

        verify(statement, never()).execute(contains("IDENTITY_INSERT"));
    }

    @Test
    void postImportMaintenanceHonorsReindexAndShrinkOptions() throws Exception {
        final var target = mock(Connection.class);
        final var statement = mock(Statement.class);
        when(target.createStatement()).thenReturn(statement);
        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> target);
        driver.open(config, false);
        final var importConfig = new ImportConfig("default", List.of("Core"), "import", List.of(), List.of());
        final var noMaintenance = new DatabaseMetadata("1", "hash", null, null, false, true, false, false);

        driver.postDatabaseImport(noMaintenance, importConfig);
        driver.postDataModuleImport(noMaintenance, importConfig, "Core", List.of("[Core].[foo]"));

        verify(statement, never()).execute(contains("sp_updatestats"));
        verify(statement, never()).execute(contains("SHRINKDATABASE"));
        verify(statement, never()).execute(contains("DBREINDEX"));

        final var shrinkAndReindex = new DatabaseMetadata("1", "hash", null, null, false, true, true, true);
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
    void postImportMaintenanceExposesEachStableOperationBoundary() throws Exception {
        final var target = mock(Connection.class);
        final var statement = mock(Statement.class);
        final var identityQuery = mock(PreparedStatement.class);
        final var identityResult = mock(ResultSet.class);
        when(target.createStatement()).thenReturn(statement);
        when(target.prepareStatement(contains("COLUMNPROPERTY"))).thenReturn(identityQuery);
        when(identityQuery.executeQuery()).thenReturn(identityResult);
        when(identityResult.next()).thenReturn(true);
        when(identityResult.getLong(1)).thenReturn(0L);
        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> target);
        driver.open(config, false);
        final var importConfig = new ImportConfig("default", List.of("Core"), "import", List.of(), List.of());
        final var metadata = new DatabaseMetadata("1", "hash", null, null, false, true, true, true);
        final var observed = new ArrayList<String>();
        final ImportMaintenanceObserver observer = (operation, subject, action) -> {
            observed.add(operation + (null == subject ? "" : ':' + subject));
            action.run();
        };

        driver.postTableImport(metadata, importConfig, "[Core].[foo]", observer);
        driver.postDataModuleImport(metadata, importConfig, "Core", List.of("[Core].[foo]", "[Core].[bar]"), observer);
        driver.postDatabaseImport(metadata, importConfig, observer);

        assertThat(observed)
                .containsExactly(
                        "post-table-reindex:[Core].[foo]",
                        "module-shrink-notruncate:Core",
                        "module-shrink-truncate:Core",
                        "post-shrink-reindex:[Core].[foo]",
                        "post-shrink-reindex:[Core].[bar]",
                        "database-update-statistics",
                        "database-update-usage");
        verify(statement, times(7)).execute(anyString());
    }

    private SqlServerDbDriver timedDriver(final Connection target, final Statement... statements) throws Exception {
        final var setupStatement = mock(Statement.class);
        when(target.createStatement()).thenReturn(setupStatement, statements);
        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> target);
        driver.open(config, false);
        driver.enableImportTiming();
        return driver;
    }

    private static ResultSetMetaData timingMetadata() throws SQLException {
        final var metadata = mock(ResultSetMetaData.class);
        final var labels = List.of(
                "Protocol", "Ordinal", "OperationId", "ParentOperationId", "Kind", "Status", "ElapsedMicroseconds");
        final var types = List.of(
                Types.VARCHAR, Types.BIGINT, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.BIGINT);
        when(metadata.getColumnCount()).thenReturn(labels.size());
        for (int i = 0; i < labels.size(); i++) {
            when(metadata.getColumnLabel(eq(i + 1))).thenReturn(labels.get(i));
            when(metadata.getColumnType(eq(i + 1))).thenReturn(types.get(i));
        }
        return metadata;
    }

    @Test
    void migrationMethodsCreateAndQueryMigrationTable() throws Exception {
        final var target = mock(Connection.class);
        final var tableExists = mock(PreparedStatement.class);
        final var tableExistsResult = mock(ResultSet.class);
        final var shouldMigrate = mock(PreparedStatement.class);
        final var shouldMigrateResult = mock(ResultSet.class);
        final var markMigration = mock(PreparedStatement.class);
        final var createMigrationTable = mock(Statement.class);
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
        when(target.createStatement()).thenReturn(createMigrationTable);
        when(tableExists.executeQuery()).thenReturn(tableExistsResult);
        when(tableExistsResult.next()).thenReturn(true);
        when(tableExistsResult.getLong(1)).thenReturn(0L);
        when(shouldMigrate.executeQuery()).thenReturn(shouldMigrateResult);
        when(shouldMigrateResult.next()).thenReturn(true);
        when(shouldMigrateResult.getLong(1)).thenReturn(0L);

        final var driver = new SqlServerDbDriver((connection, controlDatabase) -> target);
        driver.open(config, false);

        assertThat(driver.shouldMigrate("001_init")).isTrue();
        driver.markMigrationAsRun("001_init");

        verify(createMigrationTable)
                .execute("CREATE TABLE [dbo].[tblMigration]([Migration] VARCHAR(255),[AppliedAt] DATETIME)");
        verify(target).prepareStatement("SELECT COUNT(*) FROM [dbo].[tblMigration] WHERE [Migration] = ?");
        verify(target)
                .prepareStatement("INSERT INTO [dbo].[tblMigration]([Migration],[AppliedAt]) VALUES (?, GETDATE())");
        verify(shouldMigrate).setString(1, "001_init");
        verify(markMigration).setString(1, "001_init");
        verify(markMigration).executeUpdate();
    }
}
