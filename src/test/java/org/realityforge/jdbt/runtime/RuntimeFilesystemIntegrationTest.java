package org.realityforge.jdbt.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.config.ModuleGroupConfig;
import org.realityforge.jdbt.db.DatabaseConnection;
import org.realityforge.jdbt.db.DatabaseMetadata;
import org.realityforge.jdbt.db.DbDriver;
import org.realityforge.jdbt.db.QueryResult;
import org.realityforge.jdbt.files.FileResolver;
import org.realityforge.jdbt.repository.RepositoryConfig;

final class RuntimeFilesystemIntegrationTest {
    private final DatabaseConnection target = new DatabaseConnection("127.0.0.1", 1433, "TARGET", "sa", "secret");
    private final DatabaseConnection source = new DatabaseConnection("127.0.0.1", 1433, "SOURCE", "sa", "secret");

    @Test
    void createWithDatasetRecordsConfiguredDirectoryOrder(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/hooks/before/010.sql", "before");
        createFile(tempDir, "db/Core/ddl/020.sql", "core ddl");
        createFile(tempDir, "db/Core/views/030.sql", "core view");
        createFile(tempDir, "db/Core/base-fixtures/Core.foo.yml", "r1:\n  ID: 1\n");
        createFile(tempDir, "db/custom-datasets/snapshot/before/040.sql", "dataset before");
        createFile(tempDir, "db/Core/custom-datasets/snapshot/Core.foo.yml", "r2:\n  ID: 2\n");
        createFile(tempDir, "db/custom-datasets/snapshot/after/050.sql", "dataset after");
        createFile(tempDir, "db/Core/final/060.sql", "core final");
        createFile(tempDir, "db/hooks/after/070.sql", "after");

        final var driver = new TranscriptDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(tempDir.resolve("db"));

        engine.createWithDataset(database, target, true, "snapshot", Map.of());

        assertThat(driver.transcript()).isEqualTo("""
            open target
            sql:before
            create-schema:Core
            sql:core ddl
            sql:core view
            sql:DELETE FROM [Core].[foo]
            pre-fixture:[Core].[foo]
            insert:[Core].[foo]:{ID=1}
            post-fixture:[Core].[foo]
            sql:dataset before
            sql:DELETE FROM [Core].[foo]
            pre-fixture:[Core].[foo]
            insert:[Core].[foo]:{ID=2}
            post-fixture:[Core].[foo]
            sql:dataset after
            sql:core final
            sql:after
            close
            """);
    }

    @Test
    void importFailureCanBeResumedFromFailedTable(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/Core/load/Core.foo.sql", "first import");
        createFile(tempDir, "db/Core/load/Core.bar.sql", "FAIL second import");
        createFile(tempDir, "db/Core/load/Core.baz.sql", "third import");

        final var firstDriver = new TranscriptDriver("FAIL");
        final var engine = new RuntimeEngine(firstDriver, new FileResolver());
        final var database = runtimeDatabase(tempDir.resolve("db"));

        assertThatThrownBy(() -> engine.databaseImport(database, "custom", null, target, source, null, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Problem importing Core.bar")
                .hasMessageContaining("--resume-at=Core.bar");
        assertThat(firstDriver.transcript()).isEqualTo("""
            open target
            sql:DELETE FROM [Core].[baz]
            sql:DELETE FROM [Core].[bar]
            sql:DELETE FROM [Core].[foo]
            pre-table:custom:[Core].[foo]
            sql:first import
            post-table:custom:[Core].[foo]
            pre-table:custom:[Core].[bar]
            sql:FAIL second import
            close
            """);

        final var resumeDriver = new TranscriptDriver();
        new RuntimeEngine(resumeDriver, new FileResolver())
                .databaseImport(database, "custom", null, target, source, "Core.bar", Map.of());

        assertThat(resumeDriver.transcript()).isEqualTo("""
            open target
            sql:DELETE FROM [Core].[bar]
            pre-table:custom:[Core].[bar]
            sql:FAIL second import
            post-table:custom:[Core].[bar]
            pre-table:custom:[Core].[baz]
            sql:third import
            post-table:custom:[Core].[baz]
            post-module:custom:Core
            post-import:custom
            close
            """);
    }

    private static RuntimeDatabase runtimeDatabase(final Path searchDir) {
        final var repository = new RepositoryConfig(
                List.of("Core"),
                Map.of(),
                Map.of("Core", List.of("[Core].[foo]", "[Core].[bar]", "[Core].[baz]")),
                Map.of("Core", List.of()));
        return new RuntimeDatabase(
                "default",
                repository,
                List.of(searchDir),
                List.of(),
                List.of(),
                "index.txt",
                List.of("ddl", "views"),
                List.of("down"),
                List.of("final"),
                List.of("hooks/before"),
                List.of("hooks/after"),
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

    private static final class TranscriptDriver implements DbDriver {
        private final List<String> events = new ArrayList<>();
        private final String failOn;

        private TranscriptDriver() {
            this("");
        }

        private TranscriptDriver(final String failOn) {
            this.failOn = failOn;
        }

        private String transcript() {
            return String.join("\n", events) + "\n";
        }

        @Override
        public void open(final DatabaseConnection connection, final boolean openControlDatabase) {
            events.add(openControlDatabase ? "open control" : "open target");
        }

        @Override
        public void close() {
            events.add("close");
        }

        @Override
        public void drop(final DatabaseMetadata database, final DatabaseConnection connection) {
            events.add("drop:" + database.key());
        }

        @Override
        public void createDatabase(final DatabaseMetadata database, final DatabaseConnection connection) {
            events.add("create-database:" + database.key());
        }

        @Override
        public void createSchema(final String schemaName) {
            events.add("create-schema:" + schemaName);
        }

        @Override
        public void dropSchema(final String schemaName, final List<String> tablesInDropOrder) {
            events.add("drop-schema:" + schemaName + ':' + tablesInDropOrder);
        }

        @Override
        public void execute(final String sql, final boolean executeInControlDatabase) {
            events.add("sql:" + sql.trim());
            if (!failOn.isEmpty() && sql.contains(failOn)) {
                throw new RuntimeExecutionException("forced failure on " + failOn);
            }
        }

        @Override
        public void preFixtureImport(final String tableName) {
            events.add("pre-fixture:" + tableName);
        }

        @Override
        public void insert(final String tableName, final Map<String, Object> record) {
            events.add("insert:" + tableName + ':' + new LinkedHashMap<>(record));
        }

        @Override
        public void postFixtureImport(final String tableName) {
            events.add("post-fixture:" + tableName);
        }

        @Override
        public void updateSequence(final String sequenceName, final long value) {
            events.add("sequence:" + sequenceName + ':' + value);
        }

        @Override
        public void preTableImport(
                final DatabaseMetadata database, final ImportConfig importConfig, final String tableName) {
            events.add("pre-table:" + importConfig.key() + ':' + tableName);
        }

        @Override
        public void postTableImport(
                final DatabaseMetadata database, final ImportConfig importConfig, final String tableName) {
            events.add("post-table:" + importConfig.key() + ':' + tableName);
        }

        @Override
        public void postDataModuleImport(
                final DatabaseMetadata database,
                final ImportConfig importConfig,
                final String moduleName,
                final List<String> tablesInOrder) {
            events.add("post-module:" + importConfig.key() + ':' + moduleName);
        }

        @Override
        public void postDatabaseImport(final DatabaseMetadata database, final ImportConfig importConfig) {
            events.add("post-import:" + importConfig.key());
        }

        @Override
        public List<String> columnNamesForTable(final String tableName) {
            events.add("columns:" + tableName);
            return List.of("ID");
        }

        @Override
        public List<String> primaryKeyColumnNamesForTable(final String tableName) {
            events.add("primary-keys:" + tableName);
            return List.of("ID");
        }

        @Override
        public QueryResult query(final String sql) {
            events.add("query:" + sql.trim());
            return new QueryResult(List.of("ID"), List.of(List.of(1)));
        }

        @Override
        public void setupMigrations() {
            events.add("setup-migrations");
        }

        @Override
        public boolean shouldMigrate(final String namespace, final String migrationName) {
            events.add("should-migrate:" + namespace + ':' + migrationName);
            return true;
        }

        @Override
        public void markMigrationAsRun(final String namespace, final String migrationName) {
            events.add("mark-migration:" + namespace + ':' + migrationName);
        }

        @Override
        public String generateStandardImportSql(
                final String tableName,
                final String targetDatabase,
                final String sourceDatabase,
                final List<String> columns) {
            return "standard import " + tableName;
        }

        @Override
        public String generateStandardSequenceImportSql(
                final String sequenceName, final String targetDatabase, final String sourceDatabase) {
            return "standard sequence " + sequenceName;
        }

        @Override
        public String generateDefaultSequenceExportSql(final String sequenceName) {
            return "standard export sequence " + sequenceName;
        }
    }
}
