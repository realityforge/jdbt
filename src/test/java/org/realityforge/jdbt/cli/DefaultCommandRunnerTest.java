package org.realityforge.jdbt.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.db.DatabaseConnection;
import org.realityforge.jdbt.db.DatabaseMetadata;
import org.realityforge.jdbt.db.DbDriver;
import org.realityforge.jdbt.db.DbDriverFactory;
import org.realityforge.jdbt.db.QueryResult;
import org.realityforge.jdbt.files.FileResolver;
import org.realityforge.jdbt.runtime.RuntimeExecutionException;

final class DefaultCommandRunnerTest {
    private final DatabaseConnection target = new DatabaseConnection("127.0.0.1", 1433, "DB", "sa", "secret");
    private final DatabaseConnection source = new DatabaseConnection("127.0.0.1", 1433, "SRC", "sa", "secret");

    @Test
    void statusCreateDropMigrateImportAndGroupCommandsExecute(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", projectConfig(true));
        writeFile(tempDir, "repository.yml", repositoryConfig());

        final var runner = createRunner(tempDir);

        final var originalOut = System.out;
        final var output = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
            runner.status("default", "sqlserver");
        } finally {
            System.setOut(originalOut);
        }

        runner.create("default", "noop", target, true, Map.of());
        runner.createWithDataset("default", "noop", target, true, "seed", Map.of());
        runner.drop("default", "noop", target, Map.of());
        runner.migrate("default", "noop", target, Map.of());
        runner.databaseImport("default", "noop", null, null, target, source, null, Map.of());
        runner.createByImport("default", "noop", null, target, source, null, true, Map.of());
        runner.loadDataset("default", "noop", "seed", target, Map.of());
        runner.upModuleGroup("default", "noop", "all", target, Map.of());
        runner.downModuleGroup("default", "noop", "all", target, Map.of());

        assertThat(output.toString(StandardCharsets.UTF_8))
                .contains("Database Version")
                .contains("Migration Support");
    }

    @Test
    void packageDataWritesZipOutput(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", projectConfig(false));
        writeFile(tempDir, "repository.yml", repositoryConfig());
        writeFile(tempDir, "MyModule/a.sql", "SELECT 1");

        final var runner = createRunner(tempDir);
        final var output = tempDir.resolve("out.zip");
        runner.packageData("default", output);

        assertThat(output).exists();
        assertThat(Files.size(output)).isGreaterThan(0L);
        try (var zip = new ZipFile(output.toFile())) {
            assertThat(zip.getEntry("data/repository.yml")).isNotNull();
            assertThat(zip.getEntry("data/MyModule/a.sql")).isNotNull();
            assertThat(zip.getEntry("repository.yml")).isNull();
        }

        final var consumer = tempDir.resolve("consumer");
        writeFile(consumer, "jdbt.yml", "postDbArtifacts: ['" + output + "']\n");

        final var runtime = new ProjectRuntimeLoader(consumer).load(null);
        assertThat(runtime.database().repository().modules()).containsExactly("MyModule");
        assertThat(runtime.database().postDbArtifacts().get(0).files()).contains("MyModule/a.sql");
    }

    @Test
    void packageDataArtifactExecutesThroughRuntime(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", projectConfig(false));
        writeFile(tempDir, "repository.yml", repositoryConfig());
        writeFile(tempDir, "import-hooks/pre/001.sql", "artifact pre");
        writeFile(tempDir, "MyModule/import/MyModule.foo.sql", "artifact import __SOURCE__ __TARGET__ __TABLE__");
        writeFile(tempDir, "import-hooks/post/002.sql", "artifact post");

        final var output = tempDir.resolve("out.zip");
        createRunner(tempDir).packageData("default", output);

        final var consumer = tempDir.resolve("consumer");
        writeFile(
                consumer,
                "jdbt.yml",
                "postDbArtifacts: ['" + output + "']\nimports:\n  default:\n    modules: [MyModule]\n");
        final var driverFactory = new RecordingDriverFactory();
        final var runner =
                new DefaultCommandRunner(new ProjectRuntimeLoader(consumer), driverFactory, new FileResolver());

        runner.databaseImport("default", "recording", "default", null, target, source, null, Map.of());

        assertThat(driverFactory.driver.transcript()).isEqualTo("""
            open target
            sql:artifact pre
            sql:DELETE FROM [MyModule].[foo]
            pre-table:default:[MyModule].[foo]
            sql:artifact import SRC DB [MyModule].[foo]
            post-table:default:[MyModule].[foo]
            post-module:default:MyModule
            sql:artifact post
            post-import:default
            close
            """);
    }

    @Test
    void exportFixturesExecutesThroughRuntimeWithDefaultOutputDirectory(@TempDir final Path tempDir)
            throws IOException {
        writeFile(tempDir, "jdbt.yml", projectConfig(false));
        writeFile(tempDir, "repository.yml", repositoryConfig());
        writeFile(tempDir, "fixtures.properties", "MyModule.foo=SELECT __TENANT__ AS ID, 'A' AS NAME\n");
        final var driverFactory = new RecordingDriverFactory();
        final var runner =
                new DefaultCommandRunner(new ProjectRuntimeLoader(tempDir), driverFactory, new FileResolver());

        runner.exportFixtures(
                "default",
                "recording",
                target,
                tempDir.resolve("fixtures.properties"),
                null,
                null,
                Map.of("tenant", "7"));

        assertThat(driverFactory.driver.transcript()).isEqualTo("""
            open target
            query:SELECT 7 AS ID, 'A' AS NAME
            close
            """);
        assertThat(Files.readString(tempDir.resolve("MyModule/fixtures/MyModule.foo.yml"), StandardCharsets.UTF_8))
                .isEqualTo("""
                    r1:
                      ID: 7
                      NAME: "A"
                    """);
    }

    @Test
    void databaseImportRequiresDefaultImportWhenImportNotProvided(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", projectConfigWithoutImports());
        writeFile(tempDir, "repository.yml", repositoryConfig());
        final var runner = createRunner(tempDir);

        assertThatThrownBy(
                        () -> runner.databaseImport("default", "sqlserver", null, null, target, source, null, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Unable to locate import definition by key");
    }

    private static String projectConfig(final boolean withMigrations) {
        return """
            datasets: [seed]
            migrations: %s
            imports:
              default:
                modules: [MyModule]
            moduleGroups:
              all:
                modules: [MyModule]
            filterProperties:
              tenant:
                pattern: __TENANT__
                default: "0"
            """.formatted(withMigrations);
    }

    private static String projectConfigWithoutImports() {
        return """
            datasets: [seed]
            imports: {}
            moduleGroups:
              all:
                modules: [MyModule]
            """;
    }

    private static String repositoryConfig() {
        return """
            modules:
              MyModule:
                tables: ["[MyModule].[foo]"]
                sequences: []
            """;
    }

    private static void writeFile(final Path root, final String relativePath, final String content) throws IOException {
        final var file = root.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private static DefaultCommandRunner createRunner(final Path tempDir) {
        return new DefaultCommandRunner(new ProjectRuntimeLoader(tempDir), new TestDriverFactory(), new FileResolver());
    }

    private static final class TestDriverFactory extends DbDriverFactory {
        @Override
        public DbDriver create(final String driver) {
            return super.create("noop");
        }
    }

    private static final class RecordingDriverFactory extends DbDriverFactory {
        private final RecordingDriver driver = new RecordingDriver();

        @Override
        public DbDriver create(final String driver) {
            return this.driver;
        }
    }

    private static final class RecordingDriver implements DbDriver {
        private final List<String> events = new ArrayList<>();

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
        public void drop(final DatabaseMetadata database, final DatabaseConnection connection) {}

        @Override
        public void createDatabase(final DatabaseMetadata database, final DatabaseConnection connection) {}

        @Override
        public void createSchema(final String schemaName) {}

        @Override
        public void dropSchema(final String schemaName, final List<String> tablesInDropOrder) {}

        @Override
        public void execute(final String sql, final boolean executeInControlDatabase) {
            events.add("sql:" + sql.trim());
        }

        @Override
        public void preFixtureImport(final String tableName) {}

        @Override
        public void insert(final String tableName, final Map<String, Object> record) {}

        @Override
        public void postFixtureImport(final String tableName) {}

        @Override
        public void updateSequence(final String sequenceName, final long value) {}

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
            return List.of("[ID]");
        }

        @Override
        public List<String> primaryKeyColumnNamesForTable(final String tableName) {
            return List.of("[ID]");
        }

        @Override
        public QueryResult query(final String sql) {
            events.add("query:" + sql.trim());
            return new QueryResult(List.of("ID", "NAME"), List.of(List.of(7, "A")));
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
            return "";
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
    }
}
