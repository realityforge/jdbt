package org.realityforge.jdbt.db.sqlserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.db.DatabaseConnection;
import org.realityforge.jdbt.db.DatabaseException;
import org.realityforge.jdbt.db.DatabaseMetadata;
import org.realityforge.jdbt.db.DbDriver;
import org.realityforge.jdbt.db.ImportMaintenanceObserver;
import org.realityforge.jdbt.db.QueryResult;
import org.realityforge.jdbt.db.SqlTimingObservation;
import org.realityforge.jdbt.db.SqlTimingObserver;

@SuppressWarnings({"SqlNoDataSourceInspection", "SqlResolve"})
public final class SqlServerDbDriver implements DbDriver {
    private static final String TIMING_PROTOCOL = "jdbt.timing.v1";
    private static final List<String> TIMING_COLUMNS =
            List.of("Protocol", "Ordinal", "OperationId", "ParentOperationId", "Kind", "Status", "ElapsedMicroseconds");
    private static final List<Integer> TIMING_COLUMN_TYPES = List.of(
            Types.VARCHAR, Types.BIGINT, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.BIGINT);
    private static final Logger LOGGER = Logger.getLogger(SqlServerDbDriver.class.getName());

    @FunctionalInterface
    interface ConnectionFactory {
        Connection connect(DatabaseConnection config, boolean controlDatabase) throws SQLException;
    }

    private final ConnectionFactory connectionFactory;
    private @Nullable DatabaseConnection config;
    private @Nullable Connection targetConnection;
    private @Nullable Connection controlConnection;
    private @Nullable String importTimingTable;

    public SqlServerDbDriver() {
        this(SqlServerDbDriver::openSqlServerConnection);
    }

    SqlServerDbDriver(final ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void open(final DatabaseConnection connection, final boolean openControlDatabase) {
        close();
        config = connection;
        if (openControlDatabase) {
            controlConnection = connect(true);
        } else {
            targetConnection = connect(false);
        }
    }

    @Override
    public void close() {
        closeQuietly(targetConnection);
        closeQuietly(controlConnection);
        targetConnection = null;
        controlConnection = null;
        importTimingTable = null;
    }

    @Override
    public void drop(final DatabaseMetadata database, final DatabaseConnection connection) {
        final var control = controlConnection();
        executeSql(control, "SET DEADLOCK_PRIORITY HIGH");
        if (database.deleteBackupHistory()) {
            executeSql(
                    control,
                    "EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N'"
                            + sqlString(connection.database())
                            + "'");
        }
        if (database.forceDrop()) {
            executeSql(
                    control,
                    existsDatabaseSql(
                            connection.database(),
                            "ALTER DATABASE "
                                    + quote(connection.database())
                                    + " SET SINGLE_USER WITH ROLLBACK IMMEDIATE"));
        }
        executeSql(control, existsDatabaseSql(connection.database(), "DROP DATABASE " + quote(connection.database())));
    }

    @Override
    public void createDatabase(final DatabaseMetadata database, final DatabaseConnection connection) {
        executeSql(controlConnection(), createDatabaseSql(database, connection.database()));
        if (null != database.version()) {
            execute(
                    "EXEC sys.sp_addextendedproperty @name = N'DatabaseSchemaVersion', @value = N'"
                            + sqlString(database.version())
                            + "'",
                    false);
        }
    }

    @Override
    public void createSchema(final String schemaName) {
        if (!schemaExists(schemaName)) {
            execute("CREATE SCHEMA " + quote(schemaName), false);
        }
    }

    @Override
    public void dropSchema(final String schemaName, final List<String> tablesInDropOrder) {
        dropObjects(schemaName, "P", "PROCEDURE");
        dropObjects(schemaName, "FN", "FUNCTION");
        dropObjects(schemaName, "IF", "FUNCTION");
        dropObjects(schemaName, "TF", "FUNCTION");
        dropObjects(schemaName, "V", "VIEW");
        for (final var table : tablesInDropOrder) {
            if (tableExists(table)) {
                execute("DROP TABLE " + table, false);
            }
        }
        dropObjects(schemaName, "SO", "SEQUENCE");
        if (schemaExists(schemaName)) {
            execute("DROP SCHEMA " + quote(schemaName), false);
        }
    }

    @Override
    public void execute(final String sql, final boolean executeInControlDatabase) {
        final var target = targetConnection;
        if (executeInControlDatabase && null != target) {
            executeInCatalog(target, "msdb", () -> executeSql(target, sql));
        } else {
            final var connection = executeInControlDatabase ? controlConnection() : targetConnection();
            executeSql(connection, sql);
        }
    }

    @Override
    public void execute(
            final String sql, final boolean executeInControlDatabase, final SqlTimingObserver timingObserver) {
        final var target = targetConnection;
        if (executeInControlDatabase && null != target) {
            executeInCatalog(target, "msdb", () -> executeTimedSql(target, sql, timingObserver));
        } else {
            final var connection = executeInControlDatabase ? controlConnection() : targetConnection();
            executeTimedSql(connection, sql, timingObserver);
        }
    }

    @Override
    public void enableImportTiming() {
        final var connection = targetConnection();
        final var connectionConfig = config;
        if (null == connectionConfig) {
            throw new IllegalStateException("Import timing requested before driver was opened");
        }
        final var timingTable = quote(connectionConfig.database()) + ".[dbo].[tblImportTiming]";
        executeSql(connection, """
            DECLARE @LockResult INT
            EXEC @LockResult = [sys].[sp_getapplock]
              @Resource = N'jdbt.import.timing',
              @LockMode = 'Exclusive',
              @LockOwner = 'Session',
              @LockTimeout = 0,
              @DbPrincipal = 'public'
            IF @LockResult < 0
              THROW 51000, 'Another timed JDBT operation is active in this database', 1

            IF OBJECT_ID(N'[dbo].[tblImportTiming]', 'U') IS NOT NULL
              DROP TABLE [dbo].[tblImportTiming]

            CREATE TABLE [dbo].[tblImportTiming]
            (
              [Ordinal] BIGINT NOT NULL,
              [OperationId] VARCHAR(2000) NOT NULL,
              [ParentOperationId] VARCHAR(2000) NULL,
              [Kind] VARCHAR(100) NOT NULL,
              [StartedAt] DATETIME2(7) NOT NULL,
              [Status] VARCHAR(20) NOT NULL,
              [ElapsedMicroseconds] BIGINT NOT NULL
            )

            EXEC [sys].[sp_set_session_context]
              @key = N'jdbt.import.timing',
              @value = N'jdbt.timing.v1',
              @read_only = 1
            """);
        importTimingTable = timingTable;
    }

    @Override
    public void preFixtureImport(final String tableName) {
        if (hasIdentityColumn(tableName)) {
            execute("SET IDENTITY_INSERT " + tableName + " ON", false);
        }
    }

    @Override
    public void insert(final String tableName, final Map<String, Object> record) {
        final var columns = new ArrayList<>(record.keySet());
        final var columnSql =
                String.join(", ", columns.stream().map(SqlServerDbDriver::quote).toList());
        final var placeholderSql =
                String.join(", ", columns.stream().map(column -> "?").toList());
        final var sql = "INSERT INTO " + tableName + " (" + columnSql + ") VALUES (" + placeholderSql + ")";
        try (var statement = targetConnection().prepareStatement(sql)) {
            for (int i = 0; i < columns.size(); i++) {
                statement.setObject(i + 1, record.get(columns.get(i)));
            }
            statement.executeUpdate();
        } catch (final SQLException sqle) {
            throw new DatabaseException("Failed to insert record into " + tableName, sqle);
        }
    }

    @Override
    public void postFixtureImport(final String tableName) {
        if (hasIdentityColumn(tableName)) {
            execute("SET IDENTITY_INSERT " + tableName + " OFF", false);
        }
    }

    @Override
    public void updateSequence(final String sequenceName, final long value) {
        execute("ALTER SEQUENCE " + sequenceName + " RESTART WITH " + value, false);
    }

    @Override
    public void preTableImport(
            final DatabaseMetadata database, final ImportConfig importConfig, final String tableName) {
        preFixtureImport(tableName);
    }

    @Override
    public void postTableImport(
            final DatabaseMetadata database, final ImportConfig importConfig, final String tableName) {
        postTableImport(database, importConfig, tableName, (operation, subject, action) -> action.run());
    }

    @Override
    public void postTableImport(
            final DatabaseMetadata database,
            final ImportConfig importConfig,
            final String tableName,
            final ImportMaintenanceObserver observer) {
        postFixtureImport(tableName);
        if (database.reindexOnImport()) {
            observer.run("post-table-reindex", tableName, () -> reindex(tableName));
        }
    }

    @Override
    public void postDataModuleImport(
            final DatabaseMetadata database,
            final ImportConfig importConfig,
            final String moduleName,
            final List<String> tablesInOrder) {
        postDataModuleImport(
                database, importConfig, moduleName, tablesInOrder, (operation, subject, action) -> action.run());
    }

    @Override
    public void postDataModuleImport(
            final DatabaseMetadata database,
            final ImportConfig importConfig,
            final String moduleName,
            final List<String> tablesInOrder,
            final ImportMaintenanceObserver observer) {
        if (database.shrinkOnImport()) {
            final var prefix = "DECLARE @DbName VARCHAR(100); SET @DbName = DB_NAME(); ";
            observer.run(
                    "module-shrink-notruncate",
                    moduleName,
                    () -> execute(prefix + "DBCC SHRINKDATABASE(@DbName, 10, NOTRUNCATE) WITH NO_INFOMSGS", false));
            observer.run(
                    "module-shrink-truncate",
                    moduleName,
                    () -> execute(prefix + "DBCC SHRINKDATABASE(@DbName, 10, TRUNCATEONLY) WITH NO_INFOMSGS", false));
            if (database.reindexOnImport()) {
                for (final var table : tablesInOrder) {
                    observer.run("post-shrink-reindex", table, () -> reindex(table));
                }
            }
        }
    }

    @Override
    public void postDatabaseImport(final DatabaseMetadata database, final ImportConfig importConfig) {
        postDatabaseImport(database, importConfig, (operation, subject, action) -> action.run());
    }

    @Override
    public void postDatabaseImport(
            final DatabaseMetadata database,
            final ImportConfig importConfig,
            final ImportMaintenanceObserver observer) {
        if (database.reindexOnImport()) {
            observer.run("database-update-statistics", null, () -> execute("EXEC dbo.sp_updatestats", false));
            observer.run(
                    "database-update-usage",
                    null,
                    () -> execute(
                            "DECLARE @DbName VARCHAR(100); SET @DbName = DB_NAME(); "
                                    + "DBCC UPDATEUSAGE(@DbName) WITH NO_INFOMSGS, COUNT_ROWS",
                            false));
        }
    }

    @Override
    public boolean supportsAssertMacros() {
        return true;
    }

    @Override
    public boolean supportsStandardImportScripts() {
        return true;
    }

    @Override
    public List<String> columnNamesForTable(final String tableName) {
        final var sql = "SELECT C.name AS column_name FROM sys.syscolumns C WHERE C.id = OBJECT_ID(?) ORDER BY C.colid";
        final var columns = new ArrayList<String>();
        try (var statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, tableName);
            try (var resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    columns.add(quote(resultSet.getString(1)));
                }
            }
        } catch (final SQLException sqle) {
            throw new DatabaseException("Failed to query column metadata for " + tableName, sqle);
        }
        return List.copyOf(columns);
    }

    @Override
    public List<String> primaryKeyColumnNamesForTable(final String tableName) {
        final var resolved = parseTableName(tableName);
        final var sql = "SELECT U.COLUMN_NAME "
                + "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS C "
                + "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE U ON U.CONSTRAINT_CATALOG = C.CONSTRAINT_CATALOG "
                + "AND U.CONSTRAINT_SCHEMA = C.CONSTRAINT_SCHEMA AND U.CONSTRAINT_NAME = C.CONSTRAINT_NAME "
                + "WHERE C.CONSTRAINT_TYPE = 'PRIMARY KEY' AND C.TABLE_SCHEMA = ? AND C.TABLE_NAME = ? "
                + "ORDER BY U.COLUMN_NAME";
        final var columns = new ArrayList<String>();
        try (var statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, resolved.schema());
            statement.setString(2, resolved.table());
            try (var resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    columns.add(quote(resultSet.getString(1)));
                }
            }
        } catch (final SQLException sqle) {
            throw new DatabaseException("Failed to query primary key metadata for " + tableName, sqle);
        }
        return List.copyOf(columns);
    }

    @Override
    public QueryResult query(final String sql) {
        try (var statement = targetConnection().createStatement()) {
            try (var resultSet = statement.executeQuery(sql)) {
                final var metadata = resultSet.getMetaData();
                final var columnCount = metadata.getColumnCount();
                final var columnLabels = new ArrayList<String>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    columnLabels.add(metadata.getColumnLabel(i));
                }
                final var rows = new ArrayList<List<Object>>();
                while (resultSet.next()) {
                    final var row = new ArrayList<Object>(columnCount);
                    for (int i = 1; i <= columnCount; i++) {
                        row.add(resultSet.getObject(i));
                    }
                    rows.add(row);
                }
                return new QueryResult(columnLabels, rows);
            }
        } catch (final SQLException sqle) {
            throw new DatabaseException("Failed to query SQL Server", sqle);
        }
    }

    @Override
    public QueryResult verifySchemaConstraints(final String schemaName) {
        final var result = query("EXEC " + quote(schemaName) + ".spCheckConstraints");
        final var violationIndex = result.columnLabels().indexOf("IsViolation");
        if (-1 == violationIndex) {
            throw new DatabaseException("Constraint check result is missing IsViolation");
        }
        return new QueryResult(
                result.columnLabels(),
                result.rows().stream()
                        .filter(row -> Boolean.TRUE.equals(row.get(violationIndex))
                                || row.get(violationIndex) instanceof Number number && 0 != number.intValue())
                        .toList());
    }

    @Override
    public void setupMigrations() {
        if (!tableExists("[dbo].[tblMigration]")) {
            execute("CREATE TABLE [dbo].[tblMigration]([Migration] VARCHAR(255),[AppliedAt] DATETIME)", false);
        }
    }

    @Override
    public boolean shouldMigrate(final String migrationName) {
        setupMigrations();
        final var sql = "SELECT COUNT(*) FROM [dbo].[tblMigration] WHERE [Migration] = ?";
        try (var statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, migrationName);
            try (var resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return true;
                }
                return 0 == resultSet.getLong(1);
            }
        } catch (final SQLException sqle) {
            throw new DatabaseException("Failed to query migration state", sqle);
        }
    }

    @Override
    public void markMigrationAsRun(final String migrationName) {
        final var sql = "INSERT INTO [dbo].[tblMigration]([Migration],[AppliedAt]) VALUES (?, GETDATE())";
        try (var statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, migrationName);
            statement.executeUpdate();
        } catch (final SQLException sqle) {
            throw new DatabaseException("Failed to record migration", sqle);
        }
    }

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
        return "SELECT CAST(current_value AS BIGINT) FROM sys.sequences WHERE object_id = OBJECT_ID('"
                + sqlString(sequenceName)
                + "')";
    }

    private static boolean databaseExists(final Connection connection, final String databaseName) {
        final var sql = "SELECT COUNT(*) FROM sys.databases WHERE name = ?";
        try (var statement = connection.prepareStatement(sql)) {
            statement.setString(1, databaseName);
            try (var resultSet = statement.executeQuery()) {
                return resultSet.next() && resultSet.getLong(1) > 0;
            }
        } catch (final SQLException sqle) {
            throw new DatabaseException("Failed to query database metadata", sqle);
        }
    }

    private boolean schemaExists(final String schemaName) {
        final var sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?";
        try (var statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, schemaName);
            try (var resultSet = statement.executeQuery()) {
                return resultSet.next() && resultSet.getLong(1) > 0;
            }
        } catch (final SQLException sqle) {
            throw new DatabaseException("Failed to query schema metadata", sqle);
        }
    }

    private boolean tableExists(final String tableName) {
        final var sql =
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME) ="
                        + " OBJECT_ID(?)";
        try (var statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, tableName);
            try (var resultSet = statement.executeQuery()) {
                return resultSet.next() && resultSet.getLong(1) > 0;
            }
        } catch (final SQLException sqle) {
            throw new DatabaseException("Failed to query table metadata", sqle);
        }
    }

    private boolean hasIdentityColumn(final String tableName) {
        final var sql =
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(OBJECT_ID(?), COLUMN_NAME,"
                        + " 'IsIdentity') = 1";
        try (var statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, tableName);
            try (var resultSet = statement.executeQuery()) {
                return resultSet.next() && resultSet.getLong(1) > 0;
            }
        } catch (final SQLException sqle) {
            throw new DatabaseException("Failed to query identity metadata", sqle);
        }
    }

    private void dropObjects(final String schemaName, final String objectType, final String sqlKind) {
        final var sql = "SELECT QUOTENAME(SCHEMA_NAME(schema_id)) + '.' + QUOTENAME(name) "
                + "FROM sys.objects WHERE schema_id = SCHEMA_ID(?) AND type = ? ORDER BY name";
        try (var statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, schemaName);
            statement.setString(2, objectType);
            try (var resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    execute("DROP " + sqlKind + ' ' + resultSet.getString(1), false);
                }
            }
        } catch (final SQLException sqle) {
            throw new DatabaseException("Failed to query schema objects for " + schemaName, sqle);
        }
    }

    private static SchemaAndTable parseTableName(final String tableName) {
        String value = tableName.trim().replace("[", "").replace("]", "").replace("\"", "");
        final var separator = value.lastIndexOf('.');
        if (-1 == separator) {
            return new SchemaAndTable("dbo", value);
        }
        return new SchemaAndTable(value.substring(0, separator), value.substring(separator + 1));
    }

    private Connection targetConnection() {
        if (null == targetConnection) {
            targetConnection = connect(false);
        }
        return targetConnection;
    }

    private Connection controlConnection() {
        if (null == controlConnection) {
            controlConnection = connect(true);
        }
        return controlConnection;
    }

    private Connection connect(final boolean controlDatabase) {
        final var connectionConfig = this.config;
        if (null == connectionConfig) {
            throw new IllegalStateException("Connection requested before driver was opened");
        }
        try {
            return connectionFactory.connect(connectionConfig, controlDatabase);
        } catch (final SQLException sqle) {
            throw new DatabaseException("Failed to connect to SQL Server", sqle);
        }
    }

    private static void executeSql(final Connection connection, final String sql) {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (final SQLException sqle) {
            throw new DatabaseException("Failed to execute SQL", sqle);
        }
    }

    private void executeTimedSql(
            final Connection connection, final String sql, final SqlTimingObserver timingObserver) {
        final var timingTable = importTimingTable;
        if (null == timingTable) {
            throw new IllegalStateException("Timed SQL execution requested before import timing was enabled");
        }
        final DatabaseException primary;
        try (var statement = connection.createStatement()) {
            if (processTimingResults(statement, sql, timingObserver)) {
                executeSql(connection, "TRUNCATE TABLE " + timingTable);
            }
            return;
        } catch (final RuntimeException timingFailure) {
            throw timingFailure;
        } catch (final SQLException sqle) {
            primary = new DatabaseException("Failed to execute SQL", sqle);
        }
        drainImportTiming(connection, timingTable, timingObserver, primary);
        throw primary;
    }

    private static boolean processTimingResults(
            final Statement statement, final String sql, final SqlTimingObserver timingObserver) throws SQLException {
        var hasResultSet = statement.execute(sql);
        var timingResult = false;
        while (true) {
            if (hasResultSet) {
                try (var resultSet = statement.getResultSet()) {
                    timingResult |= processTimingResult(resultSet, timingObserver);
                }
            } else if (-1 == statement.getUpdateCount()) {
                break;
            }
            hasResultSet = statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
        }
        return timingResult;
    }

    private static void drainImportTiming(
            final Connection connection,
            final String timingTable,
            final SqlTimingObserver timingObserver,
            final DatabaseException primary) {
        try (var statement = connection.createStatement()) {
            processTimingResults(statement, timingDrainSql(timingTable), timingObserver);
        } catch (final SQLException | RuntimeException ignored) {
            primary.addSuppressed(new DatabaseException("Unable to drain SQL import timing after database failure"));
        }
    }

    private static String timingDrainSql(final String timingTable) {
        return "IF OBJECT_ID(N'" + sqlString(timingTable) + "', 'U') IS NOT NULL "
                + "SELECT 'jdbt.timing.v1' AS [Protocol], [Ordinal], [OperationId], [ParentOperationId], [Kind],"
                + " [Status], [ElapsedMicroseconds] FROM " + timingTable + " ORDER BY [Ordinal]";
    }

    private static boolean processTimingResult(final ResultSet resultSet, final SqlTimingObserver timingObserver)
            throws SQLException {
        final var metadata = resultSet.getMetaData();
        if (!isTimingResult(metadata)) {
            return false;
        }
        validateTimingColumns(metadata);
        while (resultSet.next()) {
            final var protocol = resultSet.getString(1);
            if (!TIMING_PROTOCOL.equals(protocol)) {
                throw new DatabaseException("Unsupported SQL import timing protocol");
            }
            final var ordinal = resultSet.getLong(2);
            if (resultSet.wasNull()) {
                throw new DatabaseException("SQL import timing ordinal must not be null");
            }
            final var operationId = requiredTimingString(resultSet, 3, "operation ID");
            final var parentOperationId = resultSet.getString(4);
            final var kind = requiredTimingString(resultSet, 5, "kind");
            final var status = requiredTimingString(resultSet, 6, "status");
            final var elapsedMicroseconds = resultSet.getLong(7);
            if (resultSet.wasNull()) {
                throw new DatabaseException("SQL import timing elapsed time must not be null");
            }
            timingObserver.observe(new SqlTimingObservation(
                    ordinal, operationId, parentOperationId, kind, status, elapsedMicroseconds));
        }
        return true;
    }

    private static boolean isTimingResult(final ResultSetMetaData metadata) throws SQLException {
        return metadata.getColumnCount() > 0 && TIMING_COLUMNS.get(0).equals(metadata.getColumnLabel(1));
    }

    private static void validateTimingColumns(final ResultSetMetaData metadata) throws SQLException {
        if (TIMING_COLUMNS.size() != metadata.getColumnCount()) {
            throw new DatabaseException("Invalid SQL import timing result shape");
        }
        for (int i = 0; i < TIMING_COLUMNS.size(); i++) {
            if (!TIMING_COLUMNS.get(i).equals(metadata.getColumnLabel(i + 1))) {
                throw new DatabaseException("Invalid SQL import timing result shape");
            }
        }
        validateTimingColumnTypes(metadata);
    }

    private static void validateTimingColumnTypes(final ResultSetMetaData metadata) throws SQLException {
        for (int i = 0; i < TIMING_COLUMN_TYPES.size(); i++) {
            if (TIMING_COLUMN_TYPES.get(i) != metadata.getColumnType(i + 1)) {
                throw new DatabaseException("Invalid SQL import timing column type");
            }
        }
    }

    private static String requiredTimingString(final ResultSet resultSet, final int column, final String field)
            throws SQLException {
        final var value = resultSet.getString(column);
        if (null == value) {
            throw new DatabaseException("SQL import timing " + field + " must not be null");
        }
        return value;
    }

    private static void executeInCatalog(final Connection connection, final String catalog, final Runnable action) {
        final String originalCatalog;
        try {
            originalCatalog = connection.getCatalog();
        } catch (final SQLException sqle) {
            throw new DatabaseException("Failed to read SQL Server connection catalog", sqle);
        }

        @Nullable RuntimeException failure = null;
        try {
            connection.setCatalog(catalog);
            action.run();
        } catch (final SQLException sqle) {
            failure = new DatabaseException("Failed to select SQL Server catalog " + catalog, sqle);
            throw failure;
        } catch (final RuntimeException e) {
            failure = e;
            throw e;
        } finally {
            try {
                connection.setCatalog(originalCatalog);
            } catch (final SQLException sqle) {
                if (null != failure) {
                    failure.addSuppressed(sqle);
                } else {
                    throw new DatabaseException(
                            "Failed to restore SQL Server connection catalog " + originalCatalog, sqle);
                }
            }
        }
    }

    private static String createDatabaseSql(final DatabaseMetadata database, final String databaseName) {
        final var version = database.version();
        final var dbFilename = null == version ? databaseName : databaseName + '_' + version.replace('.', '_');
        final var parts = new ArrayList<String>();
        parts.add("CREATE DATABASE " + quote(databaseName));
        if (null != database.dataPath()) {
            parts.add("ON PRIMARY (NAME = "
                    + quote(dbFilename)
                    + ", FILENAME='"
                    + sqlString(database.dataPath())
                    + "\\"
                    + sqlString(dbFilename)
                    + ".mdf')");
        }
        if (null != database.logPath()) {
            parts.add("LOG ON (NAME = "
                    + quote(dbFilename + "_LOG")
                    + ", FILENAME='"
                    + sqlString(database.logPath())
                    + "\\"
                    + sqlString(dbFilename)
                    + ".ldf')");
        }
        return String.join(" ", parts);
    }

    private static String existsDatabaseSql(final String databaseName, final String actionSql) {
        return "IF EXISTS (SELECT * FROM sys.master_files WHERE state = 0 AND db_name(database_id) = '"
                + sqlString(databaseName)
                + "') "
                + actionSql;
    }

    private void reindex(final String tableName) {
        execute("DBCC DBREINDEX (N'" + sqlString(tableName) + "', '', 0) WITH NO_INFOMSGS", false);
    }

    private static String quote(final String value) {
        return '[' + value.replace("]", "]]") + ']';
    }

    private static String sqlString(final String value) {
        return value.replace("'", "''");
    }

    private static Connection openSqlServerConnection(final DatabaseConnection config, final boolean controlDatabase)
            throws SQLException {
        final var database = controlDatabase ? "msdb" : config.database();
        final var jdbcUrl = "jdbc:sqlserver://"
                + config.host()
                + ':'
                + config.port()
                + ";databaseName="
                + database
                + ";encrypt=false;trustServerCertificate=true";
        return DriverManager.getConnection(jdbcUrl, config.username(), config.password());
    }

    private static void closeQuietly(final @Nullable Connection connection) {
        if (null == connection) {
            return;
        }
        try {
            connection.close();
        } catch (final SQLException sqle) {
            LOGGER.log(Level.FINEST, "Ignoring close failure", sqle);
        }
    }

    private record SchemaAndTable(String schema, String table) {}
}
