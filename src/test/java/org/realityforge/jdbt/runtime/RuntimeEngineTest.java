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
import org.realityforge.jdbt.files.FileResolver;
import org.realityforge.jdbt.repository.RepositoryConfig;

final class RuntimeEngineTest {
    private final DatabaseConnection connection = new DatabaseConnection("127.0.0.1", 1433, "DB", "sa", "secret");

    @Test
    void statusReportsVersionHashAndMigrationFlag() {
        final RuntimeDatabase migrationsOn =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(Path.of(".")));
        final RuntimeDatabase migrationsOff = new RuntimeDatabase(
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
                "2",
                "abc",
                migrationsOn.imports(),
                migrationsOn.moduleGroups());

        final RuntimeEngine engine = new RuntimeEngine(new RecordingDriver(), new FileResolver());

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

        final RecordingDriver driver = new RecordingDriver();
        final RuntimeEngine engine = new RuntimeEngine(driver, new FileResolver());
        final RuntimeDatabase database =
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
                        "close");
    }

    @Test
    void dropUsesControlDatabaseConnection() {
        final RecordingDriver driver = new RecordingDriver();
        final RuntimeEngine engine = new RuntimeEngine(driver, new FileResolver());
        final RuntimeDatabase database =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(Path.of(".")));

        engine.drop(database, connection);

        assertThat(driver.calls).containsExactly("open(true)", "drop(default)", "close");
    }

    @Test
    void upModuleGroupExecutesOnlyModulesInGroup(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/./a.sql", "A");
        createFile(tempDir, "db/MyOtherModule/./b.sql", "B");

        final RecordingDriver driver = new RecordingDriver();
        final RuntimeEngine engine = new RuntimeEngine(driver, new FileResolver());
        final RuntimeDatabase database = runtimeDatabase(
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

        final RecordingDriver driver = new RecordingDriver();
        final RuntimeEngine engine = new RuntimeEngine(driver, new FileResolver());
        final RuntimeDatabase database = runtimeDatabase(
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
        final RecordingDriver driver = new RecordingDriver();
        final RuntimeEngine engine = new RuntimeEngine(driver, new FileResolver());
        final RuntimeDatabase database =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(tempDir));

        assertThatThrownBy(() -> engine.loadDataset(database, "missing", connection))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Unknown dataset");
    }

    private RuntimeDatabase runtimeDatabase(
            final String key, final RepositoryConfig repository, final List<Path> searchDirs) {
        return runtimeDatabase(key, repository, searchDirs, Map.of(), List.of("defaultDataset"));
    }

    private RuntimeDatabase runtimeDatabase(
            final String key,
            final RepositoryConfig repository,
            final List<Path> searchDirs,
            final Map<String, ModuleGroupConfig> moduleGroups,
            final List<String> datasets) {
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
                true,
                "1",
                "hash",
                Map.of("default", new ImportConfig("default", repository.modules(), "import", List.of(), List.of())),
                moduleGroups);
    }

    private void createFile(final Path root, final String relativePath, final String content) throws IOException {
        final Path file = root.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private static final class RecordingDriver implements DbDriver {
        private final List<String> calls = new ArrayList<>();

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
