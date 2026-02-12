package org.realityforge.jdbt.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.runtime.RuntimeDatabase;
import org.realityforge.jdbt.runtime.RuntimeExecutionException;

public final class PostgresDbDriver implements DbDriver {
    private static final Logger LOGGER = Logger.getLogger(PostgresDbDriver.class.getName());

    @FunctionalInterface
    interface ConnectionFactory {
        Connection connect(DatabaseConnection config, boolean controlDatabase) throws SQLException;
    }

    private final ConnectionFactory connectionFactory;
    private @Nullable DatabaseConnection config;
    private @Nullable Connection targetConnection;
    private @Nullable Connection controlConnection;

    public PostgresDbDriver() {
        this(PostgresDbDriver::openPostgresConnection);
    }

    PostgresDbDriver(final ConnectionFactory connectionFactory) {
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
        executeSql(controlConnection(), "DROP DATABASE IF EXISTS " + quoteIdentifier(connection.database()));
    }

    @Override
    public void createDatabase(final RuntimeDatabase database, final DatabaseConnection connection) {
        executeSql(controlConnection(), "CREATE DATABASE " + quoteIdentifier(connection.database()));
    }

    @Override
    public void createSchema(final String schemaName) {
        if (!schemaExists(schemaName)) {
            execute("CREATE SCHEMA " + quoteIdentifier(schemaName), false);
        }
    }

    @Override
    public void dropSchema(final String schemaName, final List<String> tablesInDropOrder) {
        execute("DROP SCHEMA IF EXISTS " + quoteIdentifier(schemaName) + " CASCADE", false);
    }

    @Override
    public void execute(final String sql, final boolean executeInControlDatabase) {
        final var connection = executeInControlDatabase ? controlConnection() : targetConnection();
        executeSql(connection, sql);
    }

    @Override
    public void preFixtureImport(final String tableName) {}

    @Override
    public void insert(final String tableName, final Map<String, Object> record) {
        final var columns = new ArrayList<>(record.keySet());
        final var columnSql = String.join(
                ", ", columns.stream().map(PostgresDbDriver::quoteIdentifier).toList());
        final var placeholderSql =
                String.join(", ", columns.stream().map(column -> "?").toList());
        final var sql = "INSERT INTO " + tableName + " (" + columnSql + ") VALUES (" + placeholderSql + ")";
        try (PreparedStatement statement = targetConnection().prepareStatement(sql)) {
            for (int i = 0; i < columns.size(); i++) {
                statement.setObject(i + 1, record.get(columns.get(i)));
            }
            statement.executeUpdate();
        } catch (final SQLException sqle) {
            throw new RuntimeExecutionException("Failed to insert record into " + tableName, sqle);
        }
    }

    @Override
    public void postFixtureImport(final String tableName) {}

    @Override
    public void updateSequence(final String sequenceName, final long value) {
        execute("ALTER SEQUENCE " + sequenceName + " RESTART WITH " + value, false);
    }

    @Override
    public void preTableImport(final ImportConfig importConfig, final String tableName) {}

    @Override
    public void postTableImport(final ImportConfig importConfig, final String tableName) {}

    @Override
    public void postDataModuleImport(final ImportConfig importConfig, final String moduleName) {}

    @Override
    public void postDatabaseImport(final ImportConfig importConfig) {}

    @Override
    public List<String> columnNamesForTable(final String tableName) {
        final var resolved = parseTableName(tableName);
        final var sql =
                "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
        final var columns = new ArrayList<String>();
        try (PreparedStatement statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, resolved.schema());
            statement.setString(2, resolved.table());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    columns.add(quoteIdentifier(resultSet.getString(1)));
                }
            }
        } catch (final SQLException sqle) {
            throw new RuntimeExecutionException("Failed to query column metadata for " + tableName, sqle);
        }
        return List.copyOf(columns);
    }

    @Override
    public void setupMigrations() {
        if (!tableExists("public", "tblMigration")) {
            execute(
                    "CREATE TABLE \"tblMigration\"(\"Namespace\" varchar(50),\"Migration\" varchar(255),\"AppliedAt\" timestamp)",
                    false);
        }
    }

    @Override
    public boolean shouldMigrate(final String namespace, final String migrationName) {
        setupMigrations();
        final var sql = "SELECT COUNT(*) FROM \"tblMigration\" WHERE \"Namespace\" = ? AND \"Migration\" = ?";
        try (PreparedStatement statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, namespace);
            statement.setString(2, migrationName);
            try (ResultSet resultSet = statement.executeQuery()) {
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
                "INSERT INTO \"tblMigration\"(\"Namespace\",\"Migration\",\"AppliedAt\") VALUES (?, ?, current_timestamp)";
        try (PreparedStatement statement = targetConnection().prepareStatement(sql)) {
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
        if (!targetDatabase.equals(sourceDatabase)) {
            throw new RuntimeExecutionException(
                    "PostgreSQL standard import across databases is not supported. Provide explicit import SQL files.");
        }
        return "INSERT INTO "
                + tableName
                + '(' + String.join(", ", columns)
                + ")\n  SELECT "
                + String.join(", ", columns)
                + " FROM "
                + tableName
                + "\n";
    }

    @Override
    public String generateStandardSequenceImportSql(
            final String sequenceName, final String targetDatabase, final String sourceDatabase) {
        if (!targetDatabase.equals(sourceDatabase)) {
            throw new RuntimeExecutionException(
                    "PostgreSQL standard sequence import across databases is not supported. Provide explicit import SQL files.");
        }
        return "SELECT setval('" + sequenceName + "', COALESCE((SELECT last_value FROM " + sequenceName
                + "), 1), true);";
    }

    private boolean schemaExists(final String schemaName) {
        final var sql = "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?";
        try (PreparedStatement statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, schemaName);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() && resultSet.getLong(1) > 0;
            }
        } catch (final SQLException sqle) {
            throw new RuntimeExecutionException("Failed to query schema metadata", sqle);
        }
    }

    private boolean tableExists(final String schemaName, final String tableName) {
        final var sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
        try (PreparedStatement statement = targetConnection().prepareStatement(sql)) {
            statement.setString(1, schemaName);
            statement.setString(2, tableName);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() && resultSet.getLong(1) > 0;
            }
        } catch (final SQLException sqle) {
            throw new RuntimeExecutionException("Failed to query table metadata", sqle);
        }
    }

    private static SchemaAndTable parseTableName(final String tableName) {
        String value = tableName.trim().replace("[", "").replace("]", "").replace("\"", "");
        final var separator = value.lastIndexOf('.');
        if (-1 == separator) {
            return new SchemaAndTable("public", value);
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
            throw new RuntimeExecutionException("Failed to connect to PostgreSQL", sqle);
        }
    }

    private static void executeSql(final Connection connection, final String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (final SQLException sqle) {
            throw new RuntimeExecutionException("Failed to execute SQL", sqle);
        }
    }

    private static String quoteIdentifier(final String value) {
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    private static Connection openPostgresConnection(final DatabaseConnection config, final boolean controlDatabase)
            throws SQLException {
        final var database = controlDatabase ? "postgres" : config.database();
        final var jdbcUrl = "jdbc:postgresql://" + config.host() + ':' + config.port() + '/' + database;
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
