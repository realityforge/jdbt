package org.realityforge.jdbt.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.runtime.RuntimeDatabase;
import org.realityforge.jdbt.runtime.RuntimeExecutionException;

@SuppressWarnings({"SqlNoDataSourceInspection", "SqlResolve"})
final class SqlServerDbDriver implements DbDriver {
    private static final Logger LOGGER = Logger.getLogger(SqlServerDbDriver.class.getName());

    @FunctionalInterface
    interface ConnectionFactory {
        Connection connect(DatabaseConnection config, boolean controlDatabase) throws SQLException;
    }

    private final ConnectionFactory connectionFactory;
    private @Nullable DatabaseConnection config;
    private @Nullable Connection targetConnection;
    private @Nullable Connection controlConnection;

    SqlServerDbDriver() {
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
    }

    @Override
    public void drop(final RuntimeDatabase database, final DatabaseConnection connection) {
        final var control = controlConnection();
        if (databaseExists(control, connection.database())) {
            executeSql(
                    control,
                    "ALTER DATABASE " + quote(connection.database()) + " SET SINGLE_USER WITH ROLLBACK IMMEDIATE");
            executeSql(control, "DROP DATABASE " + quote(connection.database()));
        }
    }

    @Override
    public void createDatabase(final RuntimeDatabase database, final DatabaseConnection connection) {
        final var control = controlConnection();
        if (!databaseExists(control, connection.database())) {
            executeSql(control, "CREATE DATABASE " + quote(connection.database()));
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
        for (final var table : tablesInDropOrder) {
            if (tableExists(table)) {
                execute("DROP TABLE " + table, false);
            }
        }
        if (schemaExists(schemaName)) {
            execute("DROP SCHEMA " + quote(schemaName), false);
        }
    }

    @Override
    public void execute(final String sql, final boolean executeInControlDatabase) {
        final var connection = executeInControlDatabase ? controlConnection() : targetConnection();
        executeSql(connection, sql);
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
            throw new RuntimeExecutionException("Failed to insert record into " + tableName, sqle);
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
    public void preTableImport(final ImportConfig importConfig, final String tableName) {
        preFixtureImport(tableName);
    }

    @Override
    public void postTableImport(final ImportConfig importConfig, final String tableName) {
        postFixtureImport(tableName);
    }

    @Override
    public void postDataModuleImport(final ImportConfig importConfig, final String moduleName) {}

    @Override
    public void postDatabaseImport(final ImportConfig importConfig) {}

    @Override
    public boolean supportsImportAssertFilters() {
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
            throw new RuntimeExecutionException("Failed to query column metadata for " + tableName, sqle);
        }
        return List.copyOf(columns);
    }

    @Override
    public void setupMigrations() {
        if (!tableExists("[dbo].[tblMigration]")) {
            execute(
                    "CREATE TABLE [dbo].[tblMigration]([Namespace] VARCHAR(50),[Migration] VARCHAR(255),[AppliedAt] DATETIME)",
                    false);
        }
    }

    @Override
    public boolean shouldMigrate(final String namespace, final String migrationName) {
        setupMigrations();
        final var sql = "SELECT COUNT(*) FROM [dbo].[tblMigration] WHERE [Namespace] = ? AND [Migration] = ?";
        try (var statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, namespace);
            statement.setString(2, migrationName);
            try (var resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return true;
                }
                return 0 == resultSet.getLong(1);
            }
        } catch (final SQLException sqle) {
            throw new RuntimeExecutionException("Failed to query migration state", sqle);
        }
    }

    @Override
    public void markMigrationAsRun(final String namespace, final String migrationName) {
        final var sql =
                "INSERT INTO [dbo].[tblMigration]([Namespace],[Migration],[AppliedAt]) VALUES (?, ?, GETDATE())";
        try (var statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, namespace);
            statement.setString(2, migrationName);
            statement.executeUpdate();
        } catch (final SQLException sqle) {
            throw new RuntimeExecutionException("Failed to record migration", sqle);
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

    private static boolean databaseExists(final Connection connection, final String databaseName) {
        final var sql = "SELECT COUNT(*) FROM sys.databases WHERE name = ?";
        try (var statement = connection.prepareStatement(sql)) {
            statement.setString(1, databaseName);
            try (var resultSet = statement.executeQuery()) {
                return resultSet.next() && resultSet.getLong(1) > 0;
            }
        } catch (final SQLException sqle) {
            throw new RuntimeExecutionException("Failed to query database metadata", sqle);
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
            throw new RuntimeExecutionException("Failed to query schema metadata", sqle);
        }
    }

    private boolean tableExists(final String tableName) {
        final var sql =
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME) = OBJECT_ID(?)";
        try (var statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, tableName);
            try (var resultSet = statement.executeQuery()) {
                return resultSet.next() && resultSet.getLong(1) > 0;
            }
        } catch (final SQLException sqle) {
            throw new RuntimeExecutionException("Failed to query table metadata", sqle);
        }
    }

    private boolean hasIdentityColumn(final String tableName) {
        final var sql =
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(OBJECT_ID(?), COLUMN_NAME, 'IsIdentity') = 1";
        try (var statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, tableName);
            try (var resultSet = statement.executeQuery()) {
                return resultSet.next() && resultSet.getLong(1) > 0;
            }
        } catch (final SQLException sqle) {
            throw new RuntimeExecutionException("Failed to query identity metadata", sqle);
        }
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
            throw new RuntimeExecutionException("Failed to connect to SQL Server", sqle);
        }
    }

    private static void executeSql(final Connection connection, final String sql) {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (final SQLException sqle) {
            throw new RuntimeExecutionException("Failed to execute SQL", sqle);
        }
    }

    private static String quote(final String value) {
        return '[' + value.replace("]", "]]") + ']';
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
}
