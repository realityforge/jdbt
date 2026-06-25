package org.realityforge.jdbt.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.realityforge.jdbt.config.FilterPropertyConfig;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.config.ModuleGroupConfig;
import org.realityforge.jdbt.db.DatabaseConnection;
import org.realityforge.jdbt.db.DatabaseMetadata;
import org.realityforge.jdbt.db.DbDriver;
import org.realityforge.jdbt.db.QueryResult;
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
        final var output = new ArrayList<String>();
        final var engine = new RuntimeEngine(driver, new FileResolver(), output::add);
        final var database =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(tempDir.resolve("db")));

        engine.create(database, connection, false, Map.of());

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
        assertThat(output)
                .containsExactly(
                        "               : db-hooks/pre/pre.sql",
                        "MyModule       : up.sql",
                        "Fixture        : MyModule.foo",
                        "MyModule       : finalize/final.sql",
                        "               : db-hooks/post/post.sql");
    }

    @Test
    void dropUsesControlDatabaseConnection() {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(Path.of(".")));

        engine.drop(database, connection, Map.of());

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

        engine.upModuleGroup(database, "grp", connection, Map.of());

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

        engine.downModuleGroup(database, "grp", connection, Map.of());

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

        assertThatThrownBy(() -> engine.loadDataset(database, "missing", connection, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Unknown dataset");
    }

    @Test
    void importExecutesDeleteThenDefaultTableImportAndHooks(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/import-hooks/pre/pre.sql", "PRE __SOURCE__ __TARGET__");
        createFile(tempDir, "db/import-hooks/post/post.sql", "POST __SOURCE__ __TARGET__");

        final var driver = new RecordingDriver();
        final var output = new ArrayList<String>();
        final var engine = new RuntimeEngine(driver, new FileResolver(), output::add);
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

        engine.databaseImport(database, "default", null, connection, sourceConnection, null, Map.of());

        assertThat(driver.calls)
                .containsExactly(
                        "open(false)",
                        "execute(true):PRE IMPORT_DB DBT_TEST",
                        "execute(false):DELETE FROM [MyModule].[bar]",
                        "execute(false):DELETE FROM [MyModule].[foo]",
                        "preTableImport(default,[MyModule].[foo])",
                        "columnNamesForTable([MyModule].[foo])",
                        "execute(true):INSERT INTO [DBT_TEST].[MyModule].[foo]([ID])\n"
                                + "  SELECT [ID] FROM [IMPORT_DB].[MyModule].[foo]",
                        "postTableImport(default,[MyModule].[foo])",
                        "preTableImport(default,[MyModule].[bar])",
                        "columnNamesForTable([MyModule].[bar])",
                        "execute(true):INSERT INTO [DBT_TEST].[MyModule].[bar]([ID])\n"
                                + "  SELECT [ID] FROM [IMPORT_DB].[MyModule].[bar]",
                        "postTableImport(default,[MyModule].[bar])",
                        "postDataModuleImport(default,MyModule)",
                        "execute(true):POST IMPORT_DB DBT_TEST",
                        "postDatabaseImport(default)",
                        "close");
        assertThat(output)
                .containsExactly(
                        "               : import-hooks/pre/pre.sql",
                        "MyModule       : Importing MyModule.foo (By D)",
                        "MyModule       : Importing MyModule.bar (By D)",
                        "               : import-hooks/post/post.sql");
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

        engine.databaseImport(database, "default", null, connection, sourceConnection, "MyModule.bar", Map.of());
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
        assertThatThrownBy(() -> engine.databaseImport(
                        database, "default", null, connection, sourceConnection, "Missing.Table", Map.of()))
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

        engine.databaseImport(database, "default", "grp", connection, sourceConnection, null, Map.of());

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
    void standaloneImportUsesImportModuleOrderAndDeletesEachModuleBeforeImport(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var repository = RepositoryConfigTestData.twoModules();
        final var importConfig =
                new ImportConfig("custom", List.of("MyOtherModule", "MyModule"), "import", List.of(), List.of());
        final var database = runtimeDatabase(
                "default",
                repository,
                List.of(tempDir.resolve("db")),
                Map.of(),
                List.of(),
                Map.of("custom", importConfig));

        engine.databaseImport(database, "custom", null, connection, sourceConnection, null, Map.of());

        final var transcript = String.join("\n", driver.calls);
        assertThat(transcript)
                .containsSubsequence(
                        "execute(false):DELETE FROM [MyOtherModule].[bark]",
                        "execute(false):DELETE FROM [MyOtherModule].[baz]",
                        "preTableImport(custom,[MyOtherModule].[baz])",
                        "preTableImport(custom,[MyOtherModule].[bark])",
                        "execute(false):DELETE FROM [MyModule].[bar]",
                        "execute(false):DELETE FROM [MyModule].[foo]",
                        "preTableImport(custom,[MyModule].[foo])",
                        "preTableImport(custom,[MyModule].[bar])");
    }

    @Test
    void importRejectsUnexpectedImportFiles(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/import/unexpected.sql", "SELECT 1");
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(tempDir.resolve("db")));

        assertThatThrownBy(() ->
                        engine.databaseImport(database, "default", null, connection, sourceConnection, null, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Discovered additional files in import directory");
    }

    @Test
    void createWithDatasetRunsDatasetHooksAndFixtureLoad(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/./up.sql", "UP");
        createFile(tempDir, "db/MyModule/finalize/final.sql", "FINAL");
        createFile(tempDir, "db/datasets/myset/pre/pre.sql", "SELECT 'go up' AS Direction\nGO\nDSPRE");
        createFile(tempDir, "db/datasets/myset/post/post.sql", "DSPOST");
        createFile(tempDir, "db/MyModule/datasets/myset/MyModule.foo.yml", """
            --- !!omap
            - r1:
                ID: 2
            - r2:
                ID: 3
            """);

        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                "default",
                RepositoryConfigTestData.singleModule(),
                List.of(tempDir.resolve("db")),
                Map.of(),
                List.of("myset"));

        engine.createWithDataset(database, connection, false, "myset", Map.of());

        assertThat(driver.calls)
                .containsSubsequence(
                        "open(true)",
                        "drop(default)",
                        "createDatabase(default)",
                        "open(false)",
                        "createSchema(MyModule)",
                        "execute(false):UP",
                        "execute(false):SELECT 'go up' AS Direction",
                        "execute(false):DSPRE",
                        "execute(false):DELETE FROM [MyModule].[foo]",
                        "preFixtureImport([MyModule].[foo])",
                        "insert([MyModule].[foo],{ID=2})",
                        "insert([MyModule].[foo],{ID=3})",
                        "postFixtureImport([MyModule].[foo])",
                        "execute(false):DSPOST",
                        "execute(false):FINAL");
        assertThat(driver.calls)
                .filteredOn(call -> call.contains("FixtureImport") || call.startsWith("insert([MyModule].[foo]"))
                .containsExactly(
                        "preFixtureImport([MyModule].[foo])",
                        "insert([MyModule].[foo],{ID=2})",
                        "insert([MyModule].[foo],{ID=3})",
                        "postFixtureImport([MyModule].[foo])");
    }

    @Test
    void exportFixturesUsesRepositoryOrderAndWritesTableAndSequenceYaml(@TempDir final Path tempDir)
            throws IOException {
        createFile(tempDir, "exports.properties", """
            MyOtherModule.baz=SELECT '__TENANT__' AS NAME
            MyModule.foo=
            MyModule.fooSeq=
            """);
        final var driver = new RecordingDriver();
        driver.queryResults.put(
                "SELECT * FROM [MyModule].[foo] ORDER BY [ID] ASC",
                new QueryResult(
                        List.of("ID", "NAME", "DELETED", "CREATED", "DAY", "TIME", "INSTANT", "OFFSET"),
                        List.of(Arrays.asList(
                                1,
                                "A",
                                null,
                                Timestamp.valueOf("2026-06-25 08:09:10"),
                                Date.valueOf("2026-06-25"),
                                Time.valueOf("08:09:10"),
                                Instant.parse("2026-06-25T08:09:10Z"),
                                OffsetDateTime.parse("2026-06-25T18:09:10+10:00")))));
        driver.queryResults.put(
                "SELECT current_value FROM [MyModule].[fooSeq]",
                new QueryResult(List.of("current_value"), List.of(List.of(42L))));
        driver.queryResults.put("SELECT 'tenant-7' AS NAME", new QueryResult(List.of("NAME"), List.of(List.of("B"))));
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var repository = new RepositoryConfig(
                List.of("MyModule", "MyOtherModule"),
                Map.of(),
                Map.of(
                        "MyModule", List.of("[MyModule].[foo]"),
                        "MyOtherModule", List.of("[MyOtherModule].[baz]")),
                Map.of("MyModule", List.of("[MyModule].[fooSeq]"), "MyOtherModule", List.of()));
        final var database = runtimeDatabase(
                "default",
                repository,
                List.of(tempDir.resolve("db")),
                Map.of(),
                List.of("defaultDataset"),
                Map.of("default", new ImportConfig("default", repository.modules(), "import", List.of(), List.of())),
                Map.of("tenant", new FilterPropertyConfig("__TENANT__", "tenant-0", List.of("tenant-7"))));

        engine.exportFixtures(
                database,
                connection,
                tempDir.resolve("exports.properties"),
                null,
                tempDir.resolve("out"),
                Map.of("tenant", "tenant-7"));

        assertThat(driver.calls)
                .containsSubsequence(
                        "open(false)",
                        "primaryKeyColumnNamesForTable([MyModule].[foo])",
                        "query:SELECT * FROM [MyModule].[foo] ORDER BY [ID] ASC",
                        "generateDefaultSequenceExportSql([MyModule].[fooSeq])",
                        "query:SELECT current_value FROM [MyModule].[fooSeq]",
                        "query:SELECT 'tenant-7' AS NAME",
                        "close");
        assertThat(Files.readString(tempDir.resolve("out/MyModule/fixtures/MyModule.foo.yml"), StandardCharsets.UTF_8))
                .isEqualTo("""
                    r1:
                      ID: 1
                      NAME: "A"
                      CREATED: "25 Jun 2026 08:09:10"
                      DAY: "2026-06-25"
                      TIME: "08:09:10"
                      INSTANT: "25 Jun 2026 08:09:10"
                      OFFSET: "25 Jun 2026 18:09:10"
                    """);
        assertThat(Files.readString(
                        tempDir.resolve("out/MyModule/fixtures/MyModule.fooSeq.yml"), StandardCharsets.UTF_8))
                .isEqualTo("42\n");
        assertThat(Files.readString(
                        tempDir.resolve("out/MyOtherModule/fixtures/MyOtherModule.baz.yml"), StandardCharsets.UTF_8))
                .isEqualTo("""
                    r1:
                      NAME: "B"
                    """);
    }

    @Test
    void exportFixturesWritesEmptyMapForEmptyTableResult(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "exports.properties", "MyModule.foo=SELECT ID FROM [MyModule].[foo] WHERE 1 = 0\n");
        final var driver = new RecordingDriver();
        driver.queryResults.put(
                "SELECT ID FROM [MyModule].[foo] WHERE 1 = 0", new QueryResult(List.of("ID"), List.of()));
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(tempDir.resolve("db")));

        engine.exportFixtures(
                database, connection, tempDir.resolve("exports.properties"), null, tempDir.resolve("out"), Map.of());

        assertThat(Files.readString(tempDir.resolve("out/MyModule/fixtures/MyModule.foo.yml"), StandardCharsets.UTF_8))
                .isEqualTo("{}\n");
    }

    @Test
    void exportFixturesCanWriteToDatasetDirectory(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "exports.properties", "MyModule.foo=SELECT ID FROM [MyModule].[foo] WHERE 1 = 0\n");
        final var driver = new RecordingDriver();
        driver.queryResults.put(
                "SELECT ID FROM [MyModule].[foo] WHERE 1 = 0", new QueryResult(List.of("ID"), List.of()));
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                "default",
                RepositoryConfigTestData.singleModule(),
                List.of(tempDir.resolve("db")),
                Map.of(),
                List.of("sample"));

        engine.exportFixtures(
                database,
                connection,
                tempDir.resolve("exports.properties"),
                "sample",
                tempDir.resolve("out"),
                Map.of());

        assertThat(Files.readString(
                        tempDir.resolve("out/MyModule/datasets/sample/MyModule.foo.yml"), StandardCharsets.UTF_8))
                .isEqualTo("{}\n");
    }

    @Test
    void verifyConstraintsFailsWhenAnyCheckReturnsRows(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        driver.queryResults.put(
                "verify:Core",
                new QueryResult(
                        List.of("ConstraintName", "SchemaName", "TableName"),
                        List.of(List.of("CK_tblFoo", "Core", "tblFoo"))));
        driver.queryResults.put(
                "EXEC [Analysis].[spPerformChecks]",
                new QueryResult(
                        List.of("Category", "Description", "ViewSQL"),
                        List.of(List.of("Data", "Broken", "SELECT * FROM x"))));
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(tempDir.resolve("db")));

        assertThatThrownBy(() -> engine.verifyConstraints(
                        database, connection, List.of("Core"), List.of("EXEC [Analysis].[spPerformChecks]"), Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Failed Constraints")
                .hasMessageContaining("ConstraintName=CK_tblFoo")
                .hasMessageContaining("Failed Checks")
                .hasMessageContaining("Category=Data");
        assertThat(driver.calls)
                .containsSubsequence(
                        "open(false)",
                        "verifySchemaConstraints(Core)",
                        "query:EXEC [Analysis].[spPerformChecks]",
                        "close");
    }

    @Test
    void exportFixturesRejectsInvalidInputsBeforePartialOutput(@TempDir final Path tempDir) throws IOException {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(tempDir.resolve("db")));

        createFile(tempDir, "unknown.properties", "MyModule.missing=\n");
        assertThatThrownBy(() -> engine.exportFixtures(
                        database, connection, tempDir.resolve("unknown.properties"), null, tempDir, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("unknown table or sequence key");

        createFile(tempDir, "duplicate.properties", "MyModule.foo=SELECT 1\nMyModule.foo=SELECT 2\n");
        assertThatThrownBy(() -> engine.exportFixtures(
                        database, connection, tempDir.resolve("duplicate.properties"), null, tempDir, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Duplicate export properties key");

        createFile(tempDir, "nopk.properties", "MyModule.foo=\n");
        driver.primaryKeyColumnNames = List.of();
        assertThatThrownBy(() -> engine.exportFixtures(
                        database, connection, tempDir.resolve("nopk.properties"), null, tempDir, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("no primary key");

        final var duplicateCleanRepository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", List.of("[MyModule].[foo]")),
                Map.of("MyModule", List.of("\"MyModule\".\"foo\"")));
        final var duplicateCleanDatabase =
                runtimeDatabase("default", duplicateCleanRepository, List.of(tempDir.resolve("db")));
        assertThatThrownBy(() -> engine.exportFixtures(
                        duplicateCleanDatabase,
                        connection,
                        tempDir.resolve("nopk.properties"),
                        null,
                        tempDir,
                        Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Duplicate clean fixture export key");
    }

    @Test
    void exportFixturesRejectsInvalidQueryShapes(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "exports.properties", "MyModule.foo=SELECT 1 AS ID, 2 AS ID\n");
        final var driver = new RecordingDriver();
        driver.queryResults.put(
                "SELECT 1 AS ID, 2 AS ID", new QueryResult(List.of("ID", "ID"), List.of(List.of(1, 2))));
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(tempDir.resolve("db")));

        assertThatThrownBy(() -> engine.exportFixtures(
                        database, connection, tempDir.resolve("exports.properties"), null, tempDir, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("duplicate column label");

        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", List.of()),
                Map.of("MyModule", List.of("[MyModule].[fooSeq]")));
        final var sequenceDatabase = runtimeDatabase("default", repository, List.of(tempDir.resolve("db")));
        createFile(tempDir, "sequence.properties", "MyModule.fooSeq=SELECT value FROM seq\n");
        driver.queryResults.put(
                "SELECT value FROM seq", new QueryResult(List.of("value"), List.of(List.of(1), List.of(2))));

        assertThatThrownBy(() -> engine.exportFixtures(
                        sequenceDatabase, connection, tempDir.resolve("sequence.properties"), null, tempDir, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("exactly one row with exactly one column");
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

        engine.createByImport(database, "default", connection, sourceConnection, "MyModule.bar", false, Map.of());

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

        engine.databaseImport(database, "default", null, connection, sourceConnection, null, Map.of());
        assertThat(driver.calls).contains("updateSequence([MyModule].[fooSeq],23)");

        assertThatThrownBy(() -> engine.databaseImport(
                        database, "default", "missing", connection, sourceConnection, null, Map.of()))
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

        engine.databaseImport(database, "default", null, connection, sourceConnection, null, Map.of());
        assertThat(driver.calls).contains("execute(true):SELECT IMPORT_DB DBT_TEST");
    }

    @Test
    void createAppliesDeclaredFilterPropertiesInConfigOrder(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/./up.sql", "SELECT __A__ __B__");

        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                "default",
                RepositoryConfigTestData.singleModule(),
                List.of(tempDir.resolve("db")),
                Map.of(),
                List.of("defaultDataset"),
                Map.of("default", new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of())),
                Map.of(
                        "first", new FilterPropertyConfig("__A__", "A", List.of()),
                        "second", new FilterPropertyConfig("__B__", "B", List.of())));

        engine.create(database, connection, true, Map.of());

        assertThat(driver.calls).contains("execute(false):SELECT A B");
    }

    @Test
    void createRejectsUndeclaredFilterProperty(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                "default",
                RepositoryConfigTestData.singleModule(),
                List.of(tempDir.resolve("db")),
                Map.of(),
                List.of("defaultDataset"),
                Map.of("default", new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of())),
                Map.of("mode", new FilterPropertyConfig("__MODE__", "bulk", List.of("bulk", "delta"))));

        assertThatThrownBy(() -> engine.create(database, connection, true, Map.of("unknown", "value")))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("not declared");
    }

    @Test
    void createRejectsMissingRequiredFilterProperty(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                "default",
                RepositoryConfigTestData.singleModule(),
                List.of(tempDir.resolve("db")),
                Map.of(),
                List.of("defaultDataset"),
                Map.of("default", new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of())),
                Map.of("tenant", new FilterPropertyConfig("__TENANT__", null, List.of())));

        assertThatThrownBy(() -> engine.create(database, connection, true, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("is required");
    }

    @Test
    void createRejectsUnsupportedFilterPropertyValue(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                "default",
                RepositoryConfigTestData.singleModule(),
                List.of(tempDir.resolve("db")),
                Map.of(),
                List.of("defaultDataset"),
                Map.of("default", new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of())),
                Map.of("mode", new FilterPropertyConfig("__MODE__", "bulk", List.of("bulk", "delta"))));

        assertThatThrownBy(() -> engine.create(database, connection, true, Map.of("mode", "other")))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("unsupported value");
    }

    @Test
    void createRejectsToolProvidedFilterOverride(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                "default",
                RepositoryConfigTestData.singleModule(),
                List.of(tempDir.resolve("db")),
                Map.of(),
                List.of("defaultDataset"),
                Map.of("default", new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of())),
                Map.of("mode", new FilterPropertyConfig("__MODE__", "bulk", List.of("bulk", "delta"))));

        assertThatThrownBy(() -> engine.create(database, connection, true, Map.of("sourceDatabase", "override")))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("tool-provided");
    }

    @Test
    void importOnlyReplacesDoubleUnderscoreTokens(@TempDir final Path tempDir) throws IOException {
        createFile(
                tempDir, "db/MyModule/import/MyModule.foo.sql", "SELECT @@SOURCE@@ @@TARGET@@ __SOURCE__ __TARGET__");

        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(tempDir.resolve("db")));

        engine.databaseImport(database, "default", null, connection, sourceConnection, null, Map.of());

        assertThat(driver.calls).contains("execute(true):SELECT @@SOURCE@@ @@TARGET@@ IMPORT_DB DBT_TEST");
    }

    @Test
    void importExpandsAssertFiltersForSqlServerOnly(@TempDir final Path tempDir) throws IOException {
        createFile(
                tempDir,
                "db/MyModule/import/MyModule.foo.sql",
                "ASSERT_ROW_COUNT(1)\nASSERT_UNCHANGED_ROW_COUNT()\nASSERT_DATABASE_VERSION('Version_2')");

        final var sqlServerDriver = new RecordingDriver(true);
        final var sqlServerEngine = new RuntimeEngine(sqlServerDriver, new FileResolver());
        final var database =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(tempDir.resolve("db")));

        sqlServerEngine.databaseImport(database, "default", null, connection, sourceConnection, null, Map.of());

        assertThat(String.join("\n", sqlServerDriver.calls))
                .doesNotContain("ASSERT_ROW_COUNT")
                .doesNotContain("ASSERT_UNCHANGED_ROW_COUNT")
                .doesNotContain("ASSERT_DATABASE_VERSION")
                .contains("COUNT(*) FROM [DBT_TEST].[MyModule].[foo]")
                .contains("COUNT(*) FROM [IMPORT_DB].[MyModule].[foo]")
                .contains("DatabaseSchemaVersion")
                .contains("RAISERROR");

        final var nonSqlServerDriver = new RecordingDriver(false);
        final var nonSqlServerEngine = new RuntimeEngine(nonSqlServerDriver, new FileResolver());

        nonSqlServerEngine.databaseImport(database, "default", null, connection, sourceConnection, null, Map.of());

        assertThat(String.join("\n", nonSqlServerDriver.calls))
                .contains("ASSERT_ROW_COUNT(1)")
                .contains("ASSERT_UNCHANGED_ROW_COUNT()")
                .contains("ASSERT_DATABASE_VERSION('Version_2')");
    }

    @Test
    void migrateHonorsShouldMigrateDecisions(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/migrations/001_a.sql", "M1");
        createFile(tempDir, "db/migrations/002_b.sql", "M2");

        final var driver = new RecordingDriver();
        driver.migrateDecision.put("001_a", false);
        driver.migrateDecision.put("002_b", true);
        final var output = new ArrayList<String>();
        final var engine = new RuntimeEngine(driver, new FileResolver(), output::add);
        final var database =
                runtimeDatabase("default", RepositoryConfigTestData.singleModule(), List.of(tempDir.resolve("db")));

        engine.migrate(database, connection, Map.of());

        assertThat(driver.calls)
                .containsSubsequence(
                        "open(false)",
                        "shouldMigrate(default,001_a)",
                        "shouldMigrate(default,002_b)",
                        "execute(false):M2",
                        "markMigrationAsRun(default,002_b)",
                        "close");
        assertThat(driver.calls).doesNotContain("markMigrationAsRun(default,001_a)");
        assertThat(output).containsExactly("Migration: 002_b.sql");
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
                Map.of(),
                true,
                false,
                "Version_1");

        engine.migrate(database, connection, Map.of());

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
                Map.of(),
                true,
                true,
                "1");

        engine.create(database, connection, false, Map.of());

        assertThat(driver.calls).containsSubsequence("setupMigrations", "markMigrationAsRun(default,001_x)");
        assertThat(driver.calls).doesNotContain("execute(false):M1", "shouldMigrate(default,001_x)");
    }

    @SuppressWarnings("SameParameterValue")
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
                Map.of("default", new ImportConfig("default", repository.modules(), "import", List.of(), List.of())),
                Map.of());
    }

    @SuppressWarnings("SameParameterValue")
    private static RuntimeDatabase runtimeDatabase(
            final String key,
            final RepositoryConfig repository,
            final List<Path> searchDirs,
            final Map<String, ModuleGroupConfig> moduleGroups,
            final List<String> datasets,
            final Map<String, ImportConfig> imports) {
        return runtimeDatabase(
                key, repository, searchDirs, moduleGroups, datasets, imports, Map.of(), true, false, "1");
    }

    private static RuntimeDatabase runtimeDatabase(
            final String key,
            final RepositoryConfig repository,
            final List<Path> searchDirs,
            final Map<String, ModuleGroupConfig> moduleGroups,
            final List<String> datasets,
            final Map<String, ImportConfig> imports,
            final Map<String, FilterPropertyConfig> filterProperties) {
        return runtimeDatabase(
                key, repository, searchDirs, moduleGroups, datasets, imports, filterProperties, true, false, "1");
    }

    @SuppressWarnings("SameParameterValue")
    private static RuntimeDatabase runtimeDatabase(
            final String key,
            final RepositoryConfig repository,
            final List<Path> searchDirs,
            final Map<String, ModuleGroupConfig> moduleGroups,
            final List<String> datasets,
            final Map<String, ImportConfig> imports,
            final Map<String, FilterPropertyConfig> filterProperties,
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
                filterProperties,
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
        private final Map<String, QueryResult> queryResults = new LinkedHashMap<>();
        private final boolean supportsImportAssertFilters;
        private List<String> primaryKeyColumnNames = List.of("[ID]");

        private RecordingDriver() {
            this(false);
        }

        private RecordingDriver(final boolean supportsImportAssertFilters) {
            this.supportsImportAssertFilters = supportsImportAssertFilters;
        }

        @Override
        public void open(final DatabaseConnection connection, final boolean openControlDatabase) {
            calls.add("open(" + openControlDatabase + ")");
        }

        @Override
        public void close() {
            calls.add("close");
        }

        @Override
        public void drop(final DatabaseMetadata database, final DatabaseConnection connection) {
            calls.add("drop(" + database.key() + ")");
        }

        @Override
        public void createDatabase(final DatabaseMetadata database, final DatabaseConnection connection) {
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
        public void preTableImport(
                final DatabaseMetadata database, final ImportConfig importConfig, final String tableName) {
            calls.add("preTableImport(" + importConfig.key() + ',' + tableName + ")");
        }

        @Override
        public void postTableImport(
                final DatabaseMetadata database, final ImportConfig importConfig, final String tableName) {
            calls.add("postTableImport(" + importConfig.key() + ',' + tableName + ")");
        }

        @Override
        public void postDataModuleImport(
                final DatabaseMetadata database,
                final ImportConfig importConfig,
                final String moduleName,
                final List<String> tablesInOrder) {
            calls.add("postDataModuleImport(" + importConfig.key() + ',' + moduleName + ")");
        }

        @Override
        public void postDatabaseImport(final DatabaseMetadata database, final ImportConfig importConfig) {
            calls.add("postDatabaseImport(" + importConfig.key() + ")");
        }

        @Override
        public boolean supportsImportAssertFilters() {
            return supportsImportAssertFilters;
        }

        @Override
        public List<String> columnNamesForTable(final String tableName) {
            calls.add("columnNamesForTable(" + tableName + ")");
            return List.of("[ID]");
        }

        @Override
        public List<String> primaryKeyColumnNamesForTable(final String tableName) {
            calls.add("primaryKeyColumnNamesForTable(" + tableName + ")");
            return primaryKeyColumnNames;
        }

        @Override
        public QueryResult query(final String sql) {
            calls.add("query:" + sql.trim());
            return queryResults.getOrDefault(sql.trim(), new QueryResult(List.of("ID"), List.of(List.of(1))));
        }

        @Override
        public QueryResult verifySchemaConstraints(final String schemaName) {
            calls.add("verifySchemaConstraints(" + schemaName + ")");
            return queryResults.getOrDefault(
                    "verify:" + schemaName,
                    new QueryResult(List.of("ConstraintName", "SchemaName", "TableName"), List.of()));
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

        @Override
        public String generateDefaultSequenceExportSql(final String sequenceName) {
            calls.add("generateDefaultSequenceExportSql(" + sequenceName + ")");
            return "SELECT current_value FROM " + sequenceName;
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
