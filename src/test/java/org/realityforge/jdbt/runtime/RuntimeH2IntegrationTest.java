package org.realityforge.jdbt.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.config.ModuleGroupConfig;
import org.realityforge.jdbt.db.DatabaseConnection;
import org.realityforge.jdbt.db.DatabaseException;
import org.realityforge.jdbt.db.DatabaseMetadata;
import org.realityforge.jdbt.db.DbDriver;
import org.realityforge.jdbt.db.QueryResult;
import org.realityforge.jdbt.files.FileResolver;
import org.realityforge.jdbt.repository.RepositoryConfig;

final class RuntimeH2IntegrationTest {
    @Test
    void runtimeCanCreateAndLoadFixturesAgainstInMemoryDatabase(@TempDir final Path tempDir) throws Exception {
        createFile(tempDir, "db/Core/ddl/create.sql", "CREATE TABLE FOO(ID INT PRIMARY KEY, NAME VARCHAR(30))");
        createFile(tempDir, "db/Core/base-fixtures/FOO.yml", "row1:\n  ID: 1\n  NAME: base\n");
        createFile(tempDir, "db/datasets/snapshot/before/delete.sql", "DELETE FROM FOO");
        createFile(tempDir, "db/Core/custom-datasets/snapshot/FOO.yml", "row2:\n  ID: 2\n  NAME: snapshot\n");

        final var jdbcUrl = "jdbc:h2:mem:jdbt_" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
        final var connection = new DatabaseConnection("", 0, jdbcUrl, "sa", "");
        final var driver = new H2Driver();
        final var engine = new RuntimeEngine(driver, new FileResolver());

        engine.createWithDataset(runtimeDatabase(tempDir.resolve("db")), connection, true, "snapshot", Map.of());

        try (var db = DriverManager.getConnection(jdbcUrl, "sa", "");
                var statement = db.createStatement();
                var result = statement.executeQuery("SELECT ID, NAME FROM FOO ORDER BY ID")) {
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(2);
            assertThat(result.getString(2)).isEqualTo("snapshot");
            assertThat(result.next()).isFalse();
        }
    }

    private static RuntimeDatabase runtimeDatabase(final Path searchDir) {
        final var repository = new RepositoryConfig(
                List.of("Core"), Map.of(), Map.of("Core", List.of("FOO")), Map.of("Core", List.of()));
        return new RuntimeDatabase(
                "default",
                repository,
                List.of(searchDir),
                List.of(),
                List.of(),
                "index.txt",
                List.of("ddl"),
                List.of("down"),
                List.of("final"),
                List.of(),
                List.of(),
                "base-fixtures",
                "custom-datasets",
                List.of("before"),
                List.of("after"),
                List.of("snapshot"),
                false,
                false,
                "migrations",
                "1",
                "hash",
                Map.of("custom", new ImportConfig("custom", repository.modules(), "load", List.of(), List.of())),
                Map.of("group", new ModuleGroupConfig("group", repository.modules(), true)));
    }

    private static void createFile(final Path root, final String relativePath, final String content)
            throws IOException {
        final var file = root.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private static final class H2Driver implements DbDriver {
        private @Nullable Connection connection;

        @Override
        public void open(final DatabaseConnection connectionConfig, final boolean openControlDatabase) {
            try {
                connection = DriverManager.getConnection(
                        connectionConfig.database(), connectionConfig.username(), connectionConfig.password());
            } catch (final SQLException sqle) {
                throw new DatabaseException("Failed to open H2 database", sqle);
            }
        }

        @Override
        public void close() {
            try {
                connection().close();
            } catch (final SQLException sqle) {
                throw new DatabaseException("Failed to close H2 database", sqle);
            } finally {
                connection = null;
            }
        }

        @Override
        public void drop(final DatabaseMetadata database, final DatabaseConnection connection) {}

        @Override
        public void createDatabase(final DatabaseMetadata database, final DatabaseConnection connection) {}

        @Override
        public void createSchema(final String schemaName) {}

        @Override
        public void dropSchema(final String schemaName, final List<String> tablesInDropOrder) {}

        @Override
        public void execute(final String sql, final boolean executeInControlDatabase) {
            try (var statement = connection().createStatement()) {
                statement.execute(sql);
            } catch (final SQLException sqle) {
                throw new DatabaseException("Failed to execute H2 SQL", sqle);
            }
        }

        @Override
        public void preFixtureImport(final String tableName) {}

        @Override
        public void insert(final String tableName, final Map<String, Object> record) {
            final var columns = new ArrayList<>(record.keySet());
            final var placeholders = columns.stream().map(column -> "?").toList();
            final var sql = "INSERT INTO " + tableName + '(' + String.join(", ", columns) + ") VALUES ("
                    + String.join(", ", placeholders) + ')';
            try (var statement = connection().prepareStatement(sql)) {
                for (int i = 0; i < columns.size(); i++) {
                    statement.setObject(i + 1, record.get(columns.get(i)));
                }
                statement.executeUpdate();
            } catch (final SQLException sqle) {
                throw new DatabaseException("Failed to insert H2 fixture", sqle);
            }
        }

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
            return List.of("ID", "NAME");
        }

        @Override
        public List<String> primaryKeyColumnNamesForTable(final String tableName) {
            return List.of("ID");
        }

        @Override
        public QueryResult query(final String sql) {
            try (var statement = connection().createStatement()) {
                try (var resultSet = statement.executeQuery(sql)) {
                    final var metadata = resultSet.getMetaData();
                    final var columnLabels = new ArrayList<String>();
                    for (int i = 1; i <= metadata.getColumnCount(); i++) {
                        columnLabels.add(metadata.getColumnLabel(i));
                    }
                    final var rows = new ArrayList<List<Object>>();
                    while (resultSet.next()) {
                        final var row = new ArrayList<Object>();
                        for (int i = 1; i <= metadata.getColumnCount(); i++) {
                            row.add(resultSet.getObject(i));
                        }
                        rows.add(row);
                    }
                    return new QueryResult(columnLabels, rows);
                }
            } catch (final SQLException sqle) {
                throw new DatabaseException("Failed to query H2 database", sqle);
            }
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
            return "INSERT INTO " + tableName + '(' + String.join(", ", columns) + ") SELECT "
                    + String.join(", ", columns) + " FROM " + tableName;
        }

        @Override
        public String generateStandardSequenceImportSql(
                final String sequenceName, final String targetDatabase, final String sourceDatabase) {
            return "";
        }

        @Override
        public String generateDefaultSequenceExportSql(final String sequenceName) {
            return "SELECT 1";
        }

        private Connection connection() {
            if (null == connection) {
                throw new IllegalStateException("H2 connection is not open");
            }
            return connection;
        }
    }
}
