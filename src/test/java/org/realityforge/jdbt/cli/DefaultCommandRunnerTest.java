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
import org.realityforge.jdbt.db.ImportMaintenanceObserver;
import org.realityforge.jdbt.db.MigrationStatus;
import org.realityforge.jdbt.db.QueryResult;
import org.realityforge.jdbt.db.SqlTimingObserver;
import org.realityforge.jdbt.files.FileResolver;
import org.realityforge.jdbt.runtime.RuntimeExecutionException;

final class DefaultCommandRunnerTest {
    private final DatabaseConnection target = new DatabaseConnection("127.0.0.1", 1433, "DB", "sa", "secret");
    private final DatabaseConnection source = new DatabaseConnection("127.0.0.1", 1433, "SRC", "sa", "secret");

    @Test
    void statusCreateDropMigrateAndImportCommandsExecute(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", projectConfig());
        writeFile(tempDir, "repository.yml", repositoryConfig());
        writeFile(tempDir, "migrations/001_test.sql", "SELECT 1");

        final var runner = createRunner(tempDir);

        final var originalOut = System.out;
        final var output = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
            runner.status(Map.of());
        } finally {
            System.setOut(originalOut);
        }

        runner.create(target, true, Map.of());
        runner.createWithDataset(target, true, "seed", Map.of());
        runner.drop(target, Map.of());
        runner.migrate(target, Map.of());
        runner.databaseImport(null, target, source, null, null, Map.of());
        runner.createByImport(null, target, source, null, true, null, Map.of());
        runner.loadDataset("seed", target, Map.of());
        runner.verifyConstraints(target, List.of("MyModule"), List.of(), Map.of());

        assertThat(output.toString(StandardCharsets.UTF_8))
                .contains("Database Version")
                .contains("Migration Support");
    }

    @Test
    void packageDataWritesZipOutput(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", projectConfig());
        writeFile(tempDir, "repository.yml", repositoryConfig());
        writeFile(tempDir, "MyModule/a.sql", "SELECT 1");

        final var runner = createRunner(tempDir);
        final var output = tempDir.resolve("out.zip");
        runner.packageData(output);

        assertThat(output).exists();
        assertThat(Files.size(output)).isGreaterThan(0L);
        try (var zip = new ZipFile(output.toFile())) {
            assertThat(zip.getEntry("data/repository.yml")).isNotNull();
            assertThat(zip.getEntry("data/MyModule/a.sql")).isNotNull();
            assertThat(zip.getEntry("repository.yml")).isNull();
        }

        final var consumer = tempDir.resolve("consumer");
        writeFile(consumer, "jdbt.yml", "postDbArtifacts: ['" + output + "']\n");

        final var runtime = new ProjectRuntimeLoader(consumer).load();
        assertThat(runtime.database().repository().modules()).containsExactly("MyModule");
        assertThat(runtime.database().postDbArtifacts().get(0).files()).contains("MyModule/a.sql");
    }

    @Test
    void packageDataReadsCanonicalResourcesFromConfiguredRoot(@TempDir final Path tempDir) throws IOException {
        final var projectDirectory = tempDir.resolve("profile");
        final var resourceRoot = tempDir.resolve("resources");
        writeFile(projectDirectory, "jdbt.yml", "resourceRoot: ../resources\n");
        writeFile(projectDirectory, "repository.yml", repositoryConfig());
        writeFile(resourceRoot, "MyModule/a.sql", "SELECT 1");

        final var output = tempDir.resolve("out.zip");
        createRunner(projectDirectory).packageData(output);

        try (var zip = new ZipFile(output.toFile())) {
            assertThat(zip.getEntry("data/MyModule/a.sql")).isNotNull();
        }
    }

    @Test
    void packageDataArtifactExecutesThroughRuntime(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", projectConfig());
        writeFile(tempDir, "repository.yml", repositoryConfig());
        writeFile(tempDir, "import-hooks/pre/001.sql", "artifact pre");
        writeFile(tempDir, "MyModule/import/MyModule.foo.sql", "artifact import __SOURCE__ __TARGET__ __TABLE__");
        writeFile(tempDir, "import-hooks/post/002.sql", "artifact post");

        final var output = tempDir.resolve("out.zip");
        createRunner(tempDir).packageData(output);

        final var consumer = tempDir.resolve("consumer");
        writeFile(
                consumer,
                "jdbt.yml",
                "postDbArtifacts: ['" + output + "']\nimports:\n  default:\n    modules: [MyModule]\n");
        final var driver = new RecordingDriver();
        final var runner = new DefaultCommandRunner(new ProjectRuntimeLoader(consumer), driver, new FileResolver());

        runner.databaseImport("default", target, source, null, null, Map.of());

        assertThat(driver.transcript()).isEqualTo("""
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
        writeFile(tempDir, "jdbt.yml", projectConfig());
        writeFile(tempDir, "repository.yml", repositoryConfig());
        writeFile(tempDir, "fixtures.properties", "MyModule.foo=SELECT __TENANT__ AS ID, 'A' AS NAME\n");
        final var driver = new RecordingDriver();
        final var runner = new DefaultCommandRunner(new ProjectRuntimeLoader(tempDir), driver, new FileResolver());

        runner.exportFixtures(target, tempDir.resolve("fixtures.properties"), null, null, Map.of("tenant", "7"));

        assertThat(driver.transcript()).isEqualTo("""
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
    void exportFixturesDefaultsToProjectDirectoryRatherThanResourceRoot(@TempDir final Path tempDir)
            throws IOException {
        final var projectDirectory = tempDir.resolve("profile");
        final var resourceRoot = tempDir.resolve("resources");
        writeFile(projectDirectory, "jdbt.yml", "resourceRoot: ../resources\n" + projectConfig());
        writeFile(projectDirectory, "repository.yml", repositoryConfig());
        writeFile(projectDirectory, "fixtures.properties", "MyModule.foo=SELECT 1 AS ID\n");
        Files.createDirectories(resourceRoot);
        final var driver = new RecordingDriver();
        final var runner =
                new DefaultCommandRunner(new ProjectRuntimeLoader(projectDirectory), driver, new FileResolver());

        runner.exportFixtures(target, projectDirectory.resolve("fixtures.properties"), null, null, Map.of());

        assertThat(projectDirectory.resolve("MyModule/fixtures/MyModule.foo.yml"))
                .exists();
        assertThat(resourceRoot.resolve("MyModule/fixtures/MyModule.foo.yml")).doesNotExist();
    }

    @Test
    void databaseImportRequiresDefaultImportWhenImportNotProvided(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", projectConfigWithoutImports());
        writeFile(tempDir, "repository.yml", repositoryConfig());
        final var runner = createRunner(tempDir);

        assertThatThrownBy(() -> runner.databaseImport(null, target, source, null, null, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Unable to locate import definition by key");
    }

    @Test
    void importTimingPathsResolveFromProjectAndTruncateBeforeExecution(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", projectConfig());
        writeFile(tempDir, "repository.yml", repositoryConfig());
        writeFile(tempDir, "evidence/import.ndjson", "stale timing\n");
        final var driver = new RecordingDriver();
        final var runner = new DefaultCommandRunner(new ProjectRuntimeLoader(tempDir), driver, new FileResolver());

        runner.databaseImport(null, target, source, null, Path.of("evidence/import.ndjson"), Map.of());

        assertThat(tempDir.resolve("evidence/import.ndjson"))
                .content(StandardCharsets.UTF_8)
                .doesNotContain("stale timing")
                .contains("\"operation_id\":\"command/import\"")
                .contains("\"parent_operation_id\":null");

        final var absoluteOutput = tempDir.resolve("absolute/create.ndjson").toAbsolutePath();
        runner.createByImport(null, target, source, null, true, absoluteOutput, Map.of());

        assertThat(absoluteOutput)
                .content(StandardCharsets.UTF_8)
                .contains("\"operation_id\":\"command/create-by-import\"");
    }

    @Test
    void timingOutputSetupFailureOccursBeforeDatabaseMutation(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", projectConfig());
        writeFile(tempDir, "repository.yml", repositoryConfig());
        Files.createDirectories(tempDir.resolve("timing-directory"));
        final var driver = new RecordingDriver();
        final var runner = new DefaultCommandRunner(new ProjectRuntimeLoader(tempDir), driver, new FileResolver());

        assertThatThrownBy(
                        () -> runner.databaseImport(null, target, source, null, Path.of("timing-directory"), Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessage("Unable to open import timing output")
                .hasNoCause();
        assertThat(driver.events).isEmpty();
    }

    @Test
    void emitStandardImportsUsesDefaultAndNamedImportDefinitions(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", """
            imports:
              default:
                modules: [MyModule]
              empty:
                modules: []
            """);
        writeFile(tempDir, "repository.yml", repositoryConfig());
        writeFile(tempDir, "MyModule/import/MyModule.foo.sql", "EXPLICIT IMPORT");
        writeFile(tempDir, "MyModule/import/MyModule.foo.yml", "row: {ID: 1}\n");
        final var runner = new DefaultCommandRunner(new ProjectRuntimeLoader(tempDir));

        runner.emitStandardImports(null, null, false);

        assertThat(tempDir.resolve("tmp/imports/MyModule/import/MyModule.foo.sql"))
                .content()
                .contains("[__TARGET__].[MyModule].[foo]", "[__SOURCE__].[MyModule].[foo]")
                .doesNotContain("IDENTITY_INSERT");

        final var namedOutput = tempDir.resolve("tmp/named");
        runner.emitStandardImports("empty", namedOutput, false);
        assertThat(namedOutput).isDirectory().isEmptyDirectory();
    }

    @Test
    void emitStandardImportsUsesRepositoryMetadataFromDatabaseArtifact(@TempDir final Path tempDir) throws IOException {
        final var producer = tempDir.resolve("producer");
        writeFile(producer, "jdbt.yml", "{}\n");
        writeFile(producer, "repository.yml", """
            modules:
              Artifact:
                tables: [{name: "[Artifact].[tbl]", columns: ["[ID]", "[Code]"], indexes: []}]
                sequences: []
            """);
        final var artifact = tempDir.resolve("artifact.zip");
        new DefaultCommandRunner(new ProjectRuntimeLoader(producer)).packageData(artifact);

        final var consumer = tempDir.resolve("consumer");
        writeFile(consumer, "jdbt.yml", """
            postDbArtifacts: ['%s']
            imports:
              default:
                modules: [Artifact]
            """.formatted(artifact));
        final var output = consumer.resolve("generated");

        new DefaultCommandRunner(new ProjectRuntimeLoader(consumer)).emitStandardImports(null, output, false);

        assertThat(output.resolve("Artifact/import/Artifact.tbl.sql"))
                .content()
                .contains("([ID], [Code])", "SELECT [ID], [Code]");
    }

    private static String projectConfig() {
        return """
            datasets: [seed]
            imports:
              default:
                modules: [MyModule]
            filterProperties:
              tenant:
                pattern: __TENANT__
                default: "0"
            """;
    }

    private static String projectConfigWithoutImports() {
        return """
            datasets: [seed]
            imports: {}
            """;
    }

    private static String repositoryConfig() {
        return """
            modules:
              MyModule:
                tables: [{name: "[MyModule].[foo]", columns: ["[ID]"], indexes: []}]
                sequences: []
            """;
    }

    private static void writeFile(final Path root, final String relativePath, final String content) throws IOException {
        final var file = root.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private static DefaultCommandRunner createRunner(final Path tempDir) {
        return new DefaultCommandRunner(new ProjectRuntimeLoader(tempDir), new RecordingDriver(), new FileResolver());
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
        public void execute(
                final String sql, final boolean executeInControlDatabase, final SqlTimingObserver timingObserver) {
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
                final DatabaseMetadata database,
                final ImportConfig importConfig,
                final String tableName,
                final ImportMaintenanceObserver observer) {
            events.add("post-table:" + importConfig.key() + ':' + tableName);
        }

        @Override
        public void postDataModuleImport(
                final DatabaseMetadata database,
                final ImportConfig importConfig,
                final String moduleName,
                final List<String> tablesInOrder,
                final ImportMaintenanceObserver observer) {
            events.add("post-module:" + importConfig.key() + ':' + moduleName);
        }

        @Override
        public void postDatabaseImport(
                final DatabaseMetadata database,
                final ImportConfig importConfig,
                final ImportMaintenanceObserver observer) {
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
        public QueryResult verifySchemaConstraints(final String schemaName) {
            events.add("verify:" + schemaName);
            return new QueryResult(List.of(), List.of());
        }

        @Override
        public MigrationStatus prepareMigrations() {
            return new MigrationStatus(true, null);
        }

        @Override
        public void initializeMigrationState(final Map<String, String> migrations) {}

        @Override
        public boolean shouldMigrate(final String migrationName, final String checksum) {
            return true;
        }

        @Override
        public void recordMigration(final String migrationName, final String checksum) {}

        @Override
        public void applyMigration(final String migrationName, final String checksum, final Runnable action) {
            action.run();
        }

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
