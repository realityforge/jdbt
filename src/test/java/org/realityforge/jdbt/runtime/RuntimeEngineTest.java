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
import org.realityforge.jdbt.db.DbDriver;
import org.realityforge.jdbt.files.ArtifactContent;
import org.realityforge.jdbt.files.FileResolver;
import org.realityforge.jdbt.repository.RepositoryConfig;

final class RuntimeEngineTest {
    private final DatabaseConnection connection = new DatabaseConnection("127.0.0.1", 1433, "DBT_TEST", "sa", "secret");
    private final DatabaseConnection sourceConnection =
            new DatabaseConnection("127.0.0.1", 1433, "IMPORT_DB", "sa", "secret");

    @Test
    void statusReportsVersionHashAndMigrationFlag() {
        final var migrationsOn =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(Path.of(".")));
        final var migrationsOff = new RuntimeDatabase(
                migrationsOn.key(),
                migrationsOn.repository(),
                migrationsOn.searchDirs(),
                migrationsOn.preDbArtifacts(),
                migrationsOn.postDbArtifacts(),
                migrationsOn.indexFileName(),
                migrationsOn.upDirs(),
                migrationsOn.downDirs(),
                migrationsOn.finalizeDirs(),
                migrationsOn.preCreateDirs(),
                migrationsOn.postCreateDirs(),
                migrationsOn.fixtureDirName(),
                migrationsOn.datasetsDirName(),
                migrationsOn.preDatasetDirs(),
                migrationsOn.postDatasetDirs(),
                migrationsOn.datasets(),
                false,
                false,
                "migrations",
                "2",
                "abc",
                migrationsOn.imports(),
                migrationsOn.moduleGroups());

        final var engine = new RuntimeEngine(new RecordingDriver(), new FileResolver());

        assertThat(engine.status(migrationsOn)).contains("Migration Support: Yes");
        assertThat(engine.status(migrationsOff))
                .contains("Migration Support: No")
                .contains("Database Version: 2")
                .contains("Database Schema Hash: abc");
    }

    @Test
    void createExecutesExpectedFlowIncludingFixtures(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/db-hooks/pre/pre.sql", "PRE");
        createFile(tempDir, "db/MyModule/./up.sql", "UP");
        createFile(tempDir, "db/MyModule/finalize/final.sql", "FINAL");
        createFile(tempDir, "db/db-hooks/post/post.sql", "POST");
        createFile(tempDir, "db/MyModule/fixtures/MyModule.foo.yml", "1:\n  ID: 1\n");

        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(tempDir.resolve("db")));

        engine.create(database, connection, false);

        assertThat(driver.calls)
                .containsExactly(
                        "open(true)",
                        "drop(default)",
                        "createDatabase(default)",
                        "close",
                        "open(false)",
                        "execute(false):PRE",
                        "createSchema(MyModule)",
                        "execute(false):UP",
                        "execute(false):DELETE FROM [MyModule].[foo]",
                        "preFixtureImport([MyModule].[foo])",
                        "insert([MyModule].[foo],{ID=1})",
                        "postFixtureImport([MyModule].[foo])",
                        "execute(false):FINAL",
                        "execute(false):POST",
                        "setupMigrations",
                        "close");
    }

    @Test
    void dropUsesControlDatabaseConnection() {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(Path.of(".")));

        engine.drop(database, connection);

        assertThat(driver.calls).containsExactly("open(true)", "drop(default)", "close");
    }

    @Test
    void upModuleGroupExecutesOnlyModulesInGroup(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/./a.sql", "A");
        createFile(tempDir, "db/MyOtherModule/./b.sql", "B");

        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                "default",
                RepositoryConfigTestData.twoModules(),
                List.of(tempDir.resolve("db")),
                Map.of("grp", new ModuleGroupConfig("grp", List.of("MyOtherModule"), false)),
                List.of());

        engine.upModuleGroup(database, "grp", connection);

        assertThat(driver.calls)
                .containsExactly("open(false)", "createSchema(MyOtherModule)", "execute(false):B", "close");
    }

    @Test
    void downModuleGroupDropsSchemaInReverseModuleAndTableOrder(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/down/downA.sql", "DA");
        createFile(tempDir, "db/MyOtherModule/down/downB.sql", "DB");

        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                "default",
                RepositoryConfigTestData.twoModules(),
                List.of(tempDir.resolve("db")),
                Map.of("grp", new ModuleGroupConfig("grp", List.of("MyModule", "MyOtherModule"), false)),
                List.of());

        engine.downModuleGroup(database, "grp", connection);

        assertThat(driver.calls)
                .containsExactly(
                        "open(false)",
                        "execute(false):DB",
                        "dropSchema(MyOtherModule,[[MyOtherModule].[bark], [MyOtherModule].[baz]])",
                        "execute(false):DA",
                        "dropSchema(MyModule,[[MyModule].[bar], [MyModule].[foo]])",
                        "close");
    }

    @Test
    void loadDatasetRequiresKnownDataset(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(tempDir));

        assertThatThrownBy(() -> engine.loadDataset(database, "missing", connection))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Unknown dataset");
    }

    @Test
    void importExecutesDeleteThenDefaultTableImportAndHooks(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/import-hooks/pre/pre.sql", "PRE __SOURCE__ __TARGET__");
        createFile(tempDir, "db/import-hooks/post/post.sql", "POST __SOURCE__ __TARGET__");

        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", List.of("[MyModule].[foo]", "[MyModule].[bar]")),
                Map.of("MyModule", List.of()));
        final var importConfig = new ImportConfig(
                "default", List.of("MyModule"), "import", List.of("import-hooks/pre"), List.of("import-hooks/post"));
        final var database = runtimeDatabase(
                "default",
                repository,
                List.of(tempDir.resolve("db")),
                Map.of(),
                List.of("defaultDataset"),
                Map.of("default", importConfig));

        engine.databaseImport(database, "default", null, connection, sourceConnection, null);

        assertThat(driver.calls)
                .containsExactly(
                        "open(false)",
                        "execute(true):PRE IMPORT_DB DBT_TEST",
                        "execute(false):DELETE FROM [MyModule].[bar]",
                        "execute(false):DELETE FROM [MyModule].[foo]",
                        "preTableImport(default,[MyModule].[foo])",
                        "columnNamesForTable([MyModule].[foo])",
                        "execute(true):INSERT INTO [DBT_TEST].[MyModule].[foo]([ID])\n  SELECT [ID] FROM [IMPORT_DB].[MyModule].[foo]",
                        "postTableImport(default,[MyModule].[foo])",
                        "preTableImport(default,[MyModule].[bar])",
                        "columnNamesForTable([MyModule].[bar])",
                        "execute(true):INSERT INTO [DBT_TEST].[MyModule].[bar]([ID])\n  SELECT [ID] FROM [IMPORT_DB].[MyModule].[bar]",
                        "postTableImport(default,[MyModule].[bar])",
                        "postDataModuleImport(default,MyModule)",
                        "execute(true):POST IMPORT_DB DBT_TEST",
                        "postDatabaseImport(default)",
                        "close");
    }

    @Test
    void importResumesAtTableAndErrorsOnUnknownResume(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", List.of("[MyModule].[foo]", "[MyModule].[bar]", "[MyModule].[baz]")),
                Map.of("MyModule", List.of()));
        final var database = runtimeDatabase("default", repository, List.of(tempDir.resolve("db")));

        engine.databaseImport(database, "default", null, connection, sourceConnection, "MyModule.bar");
        assertThat(driver.calls)
                .containsSubsequence(
                        "open(false)",
                        "execute(false):DELETE FROM [MyModule].[bar]",
                        "preTableImport(default,[MyModule].[bar])",
                        "preTableImport(default,[MyModule].[baz])",
                        "postDatabaseImport(default)",
                        "close");
        assertThat(driver.calls).doesNotContain("execute(false):DELETE FROM [MyModule].[foo]");

        driver.calls.clear();
        assertThatThrownBy(() ->
                        engine.databaseImport(database, "default", null, connection, sourceConnection, "Missing.Table"))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Partial import unable to be completed");
    }

    @Test
    void importWithModuleGroupDeletesAllBeforeImport(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var repository = RepositoryConfigTestData.twoModules();
        final var database = runtimeDatabase(
                "default",
                repository,
                List.of(tempDir.resolve("db")),
                Map.of("grp", new ModuleGroupConfig("grp", List.of("MyModule", "MyOtherModule"), true)),
                List.of(),
                Map.of("default", new ImportConfig("default", repository.modules(), "import", List.of(), List.of())));

        engine.databaseImport(database, "default", "grp", connection, sourceConnection, null);

        final var firstDelete = indexOf(driver.calls, "execute(false):DELETE FROM [MyModule].[bar]");
        final var secondDelete = indexOf(driver.calls, "execute(false):DELETE FROM [MyModule].[foo]");
        final var thirdDelete = indexOf(driver.calls, "execute(false):DELETE FROM [MyOtherModule].[bark]");
        final var firstImport = indexOf(driver.calls, "preTableImport(default,[MyModule].[foo])");

        assertThat(firstDelete).isNotNegative();
        assertThat(secondDelete).isNotNegative();
        assertThat(thirdDelete).isNotNegative();
        assertThat(firstImport).isGreaterThan(secondDelete).isGreaterThan(thirdDelete);
    }

    @Test
    void importRejectsUnexpectedImportFiles(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/import/unexpected.sql", "SELECT 1");
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(tempDir.resolve("db")));

        assertThatThrownBy(() -> engine.databaseImport(database, "default", null, connection, sourceConnection, null))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Discovered additional files in import directory");
    }

    @Test
    void createWithDatasetRunsDatasetHooksAndFixtureLoad(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/./up.sql", "UP");
        createFile(tempDir, "db/MyModule/finalize/final.sql", "FINAL");
        createFile(tempDir, "db/datasets/myset/pre/pre.sql", "DSPRE");
        createFile(tempDir, "db/datasets/myset/post/post.sql", "DSPOST");
        createFile(tempDir, "db/MyModule/datasets/myset/MyModule.foo.yml", "1:\n  ID: 2\n");

        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                "default",
                RepositoryConfigTestData.singleModule(),
                List.of(tempDir.resolve("db")),
                Map.of(),
                List.of("myset"));

        engine.createWithDataset(database, connection, false, "myset");

        assertThat(driver.calls)
                .containsSubsequence(
                        "open(true)",
                        "drop(default)",
                        "createDatabase(default)",
                        "open(false)",
                        "createSchema(MyModule)",
                        "execute(false):UP",
                        "execute(false):DSPRE",
                        "execute(false):DELETE FROM [MyModule].[foo]",
                        "preFixtureImport([MyModule].[foo])",
                        "insert([MyModule].[foo],{ID=2})",
                        "execute(false):DSPOST",
                        "execute(false):FINAL");
    }

    @Test
    void createByImportSkipsCreatePathWhenResuming(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", List.of("[MyModule].[foo]", "[MyModule].[bar]")),
                Map.of("MyModule", List.of()));
        final var database = runtimeDatabase("default", repository, List.of(tempDir.resolve("db")));

        engine.createByImport(database, "default", connection, sourceConnection, "MyModule.bar", false);

        assertThat(driver.calls)
                .containsSubsequence(
                        "open(false)",
                        "execute(false):DELETE FROM [MyModule].[bar]",
                        "preTableImport(default,[MyModule].[bar])",
                        "postDatabaseImport(default)",
                        "close");
        assertThat(driver.calls).doesNotContain("open(true)", "drop(default)", "createSchema(MyModule)");
    }

    @Test
    void importIncludesSequenceProcessingAndModuleGroupValidation(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/import/MyModule.fooSeq.yml", "--- 23\n");
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", List.of("[MyModule].[foo]")),
                Map.of("MyModule", List.of("[MyModule].[fooSeq]")));
        final var database = runtimeDatabase("default", repository, List.of(tempDir.resolve("db")));

        engine.databaseImport(database, "default", null, connection, sourceConnection, null);
        assertThat(driver.calls).contains("updateSequence([MyModule].[fooSeq],23)");

        assertThatThrownBy(
                        () -> engine.databaseImport(database, "default", "missing", connection, sourceConnection, null))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Unable to locate module group definition");
    }

    @Test
    void importUsesArtifactLocationsWhenImportFilesInZip(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var repository = RepositoryConfigTestData.singleModule();
        final var database = new RuntimeDatabase(
                "default",
                repository,
                List.of(tempDir.resolve("db")),
                List.of(),
                List.of(new InMemoryArtifact(
                        "post", Map.of("MyModule/import/MyModule.foo.sql", "SELECT __SOURCE__ __TARGET__"))),
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
                List.of("defaultDataset"),
                true,
                false,
                "migrations",
                "1",
                "hash",
                Map.of("default", new ImportConfig("default", repository.modules(), "import", List.of(), List.of())),
                Map.of());

        engine.databaseImport(database, "default", null, connection, sourceConnection, null);
        assertThat(driver.calls).contains("execute(true):SELECT IMPORT_DB DBT_TEST");
    }

    @Test
    void migrateHonorsShouldMigrateDecisions(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/migrations/001_a.sql", "M1");
        createFile(tempDir, "db/migrations/002_b.sql", "M2");

        final var driver = new RecordingDriver();
        driver.migrateDecision.put("001_a", false);
        driver.migrateDecision.put("002_b", true);
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(tempDir.resolve("db")));

        engine.migrate(database, connection);

        assertThat(driver.calls)
                .containsSubsequence(
                        "open(false)",
                        "shouldMigrate(default,001_a)",
                        "shouldMigrate(default,002_b)",
                        "execute(false):M2",
                        "markMigrationAsRun(default,002_b)",
                        "close");
        assertThat(driver.calls).doesNotContain("markMigrationAsRun(default,001_a)");
    }

    @Test
    void migrateSkipsExecutionBeforeReleaseVersionBoundary(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/migrations/001_x.sql", "M1");
        createFile(tempDir, "db/migrations/002_Release-Version_1.sql", "M2");
        createFile(tempDir, "db/migrations/003_z.sql", "M3");

        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                "default",
                RepositoryConfigTestData.singleModule(),
                List.of(tempDir.resolve("db")),
                Map.of(),
                List.of("defaultDataset"),
                Map.of("default", new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of())),
                true,
                false,
                "Version_1");

        engine.migrate(database, connection);

        assertThat(driver.calls)
                .containsSubsequence(
                        "shouldMigrate(default,001_x)",
                        "markMigrationAsRun(default,001_x)",
                        "shouldMigrate(default,002_Release-Version_1)",
                        "markMigrationAsRun(default,002_Release-Version_1)",
                        "shouldMigrate(default,003_z)",
                        "execute(false):M3",
                        "markMigrationAsRun(default,003_z)");
        assertThat(driver.calls).doesNotContain("execute(false):M1", "execute(false):M2");
    }

    @Test
    void createUsesMigrationRecordModeWhenConfigured(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/migrations/001_x.sql", "M1");

        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                "default",
                RepositoryConfigTestData.singleModule(),
                List.of(tempDir.resolve("db")),
                Map.of(),
                List.of("defaultDataset"),
                Map.of("default", new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of())),
                true,
                true,
                "1");

        engine.create(database, connection, false);

        assertThat(driver.calls).containsSubsequence("setupMigrations", "markMigrationAsRun(default,001_x)");
        assertThat(driver.calls).doesNotContain("execute(false):M1", "shouldMigrate(default,001_x)");
    }

    private static RuntimeDatabase runtimeDatabase(
            final String key, final RepositoryConfig repository, final List<Path> searchDirs) {
        return runtimeDatabase(key, repository, searchDirs, Map.of(), List.of("defaultDataset"));
    }

    private static RuntimeDatabase runtimeDatabase(
            final String key,
            final RepositoryConfig repository,
            final List<Path> searchDirs,
            final Map<String, ModuleGroupConfig> moduleGroups,
            final List<String> datasets) {
        return runtimeDatabase(
                key,
                repository,
                searchDirs,
                moduleGroups,
                datasets,
                Map.of("default", new ImportConfig("default", repository.modules(), "import", List.of(), List.of())));
    }

    private static RuntimeDatabase runtimeDatabase(
            final String key,
            final RepositoryConfig repository,
            final List<Path> searchDirs,
            final Map<String, ModuleGroupConfig> moduleGroups,
            final List<String> datasets,
            final Map<String, ImportConfig> imports) {
        return runtimeDatabase(key, repository, searchDirs, moduleGroups, datasets, imports, true, false, "1");
    }

    private static RuntimeDatabase runtimeDatabase(
            final String key,
            final RepositoryConfig repository,
            final List<Path> searchDirs,
            final Map<String, ModuleGroupConfig> moduleGroups,
            final List<String> datasets,
            final Map<String, ImportConfig> imports,
            final boolean migrationsEnabled,
            final boolean migrationsAppliedAtCreate,
            final String version) {
        return new RuntimeDatabase(
                key,
                repository,
                searchDirs,
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
                datasets,
                migrationsEnabled,
                migrationsAppliedAtCreate,
                "migrations",
                version,
                "hash",
                imports,
                moduleGroups);
    }

    private static int indexOf(final List<String> values, final String expected) {
        for (int i = 0; i < values.size(); i++) {
            if (expected.equals(values.get(i))) {
                return i;
            }
        }
        return -1;
    }

    private static void createFile(final Path root, final String relativePath, final String content)
            throws IOException {
        final var file = root.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private static final class RecordingDriver implements DbDriver {
        private final List<String> calls = new ArrayList<>();
        private final Map<String, Boolean> migrateDecision = new LinkedHashMap<>();

        @Override
        public void open(final DatabaseConnection connection, final boolean openControlDatabase) {
            calls.add("open(" + openControlDatabase + ")");
        }

        @Override
        public void close() {
            calls.add("close");
        }

        @Override
        public void drop(final RuntimeDatabase database, final DatabaseConnection connection) {
            calls.add("drop(" + database.key() + ")");
        }

        @Override
        public void createDatabase(final RuntimeDatabase database, final DatabaseConnection connection) {
            calls.add("createDatabase(" + database.key() + ")");
        }

        @Override
        public void createSchema(final String schemaName) {
            calls.add("createSchema(" + schemaName + ")");
        }

        @Override
        public void dropSchema(final String schemaName, final List<String> tablesInDropOrder) {
            calls.add("dropSchema(" + schemaName + "," + tablesInDropOrder + ")");
        }

        @Override
        public void execute(final String sql, final boolean executeInControlDatabase) {
            calls.add("execute(" + executeInControlDatabase + "):" + sql.trim());
        }

        @Override
        public void preFixtureImport(final String tableName) {
            calls.add("preFixtureImport(" + tableName + ")");
        }

        @Override
        public void insert(final String tableName, final Map<String, Object> record) {
            calls.add("insert(" + tableName + "," + new LinkedHashMap<>(record) + ")");
        }

        @Override
        public void postFixtureImport(final String tableName) {
            calls.add("postFixtureImport(" + tableName + ")");
        }

        @Override
        public void updateSequence(final String sequenceName, final long value) {
            calls.add("updateSequence(" + sequenceName + ',' + value + ")");
        }

        @Override
        public void preTableImport(final ImportConfig importConfig, final String tableName) {
            calls.add("preTableImport(" + importConfig.key() + ',' + tableName + ")");
        }

        @Override
        public void postTableImport(final ImportConfig importConfig, final String tableName) {
            calls.add("postTableImport(" + importConfig.key() + ',' + tableName + ")");
        }

        @Override
        public void postDataModuleImport(final ImportConfig importConfig, final String moduleName) {
            calls.add("postDataModuleImport(" + importConfig.key() + ',' + moduleName + ")");
        }

        @Override
        public void postDatabaseImport(final ImportConfig importConfig) {
            calls.add("postDatabaseImport(" + importConfig.key() + ")");
        }

        @Override
        public List<String> columnNamesForTable(final String tableName) {
            calls.add("columnNamesForTable(" + tableName + ")");
            return List.of("[ID]");
        }

        @Override
        public void setupMigrations() {
            calls.add("setupMigrations");
        }

        @Override
        public boolean shouldMigrate(final String namespace, final String migrationName) {
            calls.add("shouldMigrate(" + namespace + ',' + migrationName + ")");
            return migrateDecision.getOrDefault(migrationName, true);
        }

        @Override
        public void markMigrationAsRun(final String namespace, final String migrationName) {
            calls.add("markMigrationAsRun(" + namespace + ',' + migrationName + ")");
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
                    + "].sys.sequences WHERE object_id = OBJECT_ID('["
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
    }

    private record InMemoryArtifact(String id, Map<String, String> entries) implements ArtifactContent {
        @Override
        public List<String> files() {
            return List.copyOf(entries.keySet());
        }

        @Override
        public String readText(final String path) {
            final var value = entries.get(path);
            if (null == value) {
                throw new RuntimeExecutionException("Missing artifact entry " + path);
            }
            return value;
        }
    }

    private static final class RepositoryConfigTestData {
        static RepositoryConfig singleModule() {
            return new RepositoryConfig(
                    List.of("MyModule"),
                    Map.of(),
                    Map.of("MyModule", List.of("[MyModule].[foo]")),
                    Map.of("MyModule", List.of()));
        }

        static RepositoryConfig twoModules() {
            return new RepositoryConfig(
                    List.of("MyModule", "MyOtherModule"),
                    Map.of(),
                    Map.of(
                            "MyModule", List.of("[MyModule].[foo]", "[MyModule].[bar]"),
                            "MyOtherModule", List.of("[MyOtherModule].[baz]", "[MyOtherModule].[bark]")),
                    Map.of("MyModule", List.of(), "MyOtherModule", List.of()));
        }
    }
}
