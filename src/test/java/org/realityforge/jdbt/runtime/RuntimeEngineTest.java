package org.realityforge.jdbt.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.StringWriter;
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
import java.util.concurrent.atomic.AtomicLong;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.realityforge.jdbt.config.FilterPropertyConfig;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.db.DatabaseConnection;
import org.realityforge.jdbt.db.DatabaseException;
import org.realityforge.jdbt.db.DatabaseMetadata;
import org.realityforge.jdbt.db.DbDriver;
import org.realityforge.jdbt.db.ImportMaintenanceObserver;
import org.realityforge.jdbt.db.MigrationStatus;
import org.realityforge.jdbt.db.QueryResult;
import org.realityforge.jdbt.db.SqlTimingObservation;
import org.realityforge.jdbt.db.SqlTimingObserver;
import org.realityforge.jdbt.files.ArtifactContent;
import org.realityforge.jdbt.files.FileResolver;
import org.realityforge.jdbt.repository.RepositoryConfig;
import org.realityforge.jdbt.repository.RepositoryTable;
import org.realityforge.jdbt.repository.RowSource;

final class RuntimeEngineTest {
    private final DatabaseConnection connection = new DatabaseConnection("127.0.0.1", 1433, "DBT_TEST", "sa", "secret");
    private final DatabaseConnection sourceConnection =
            new DatabaseConnection("127.0.0.1", 1433, "IMPORT_DB", "sa", "secret");

    @Test
    void statusReportsVersionHashAndMigrationFiles(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "on/migrations/001_a.sql", "M1");
        final var migrationsOn = runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir.resolve("on"));
        final var migrationsOff = new RuntimeDatabase(
                migrationsOn.repository(),
                tempDir.resolve("off"),
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
                "migrations",
                "2",
                "abc",
                null,
                null,
                false,
                true,
                true,
                false,
                Map.of(),
                migrationsOn.imports(),
                List.of());

        final var engine = new RuntimeEngine(new RecordingDriver(), new FileResolver());

        assertThat(engine.status(migrationsOn)).contains("Migration Support: Yes");
        assertThat(engine.status(migrationsOff))
                .contains("Migration Support: No")
                .contains("Database Version: 2")
                .contains("Database Schema Hash: ")
                .doesNotContain("Database Schema Hash: abc\n");
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
        final var database = runtimeDatabase(
                singleModuleRepository(table("[MyModule].[foo]", RowSource.DEPLOYMENT)), tempDir.resolve("db"));

        engine.create(database, connection, false, Map.of());

        assertThat(driver.calls)
                .containsExactly(
                        "open(true)",
                        "drop",
                        "createDatabase",
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
        assertThat(output)
                .containsExactly(
                        "               : db-hooks/pre/pre.sql",
                        "MyModule       : up.sql",
                        "Fixture        : MyModule.foo",
                        "MyModule       : finalize/final.sql",
                        "               : db-hooks/post/post.sql");
    }

    @Test
    void createEntryPointsRejectImportRowSourceInitialFixtureBeforeDatabaseMutation(@TempDir final Path tempDir)
            throws IOException {
        createFile(tempDir, "db/MyModule/fixtures/MyModule.foo.yml", "r1:\n  ID: 1\n");
        final var repository = singleModuleRepository(table("[MyModule].[foo]", RowSource.IMPORT));
        final var database = runtimeDatabase(repository, tempDir.resolve("db"), List.of("defaultDataset"));
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());

        assertThatThrownBy(() -> engine.create(database, connection, false, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Initial Fixture")
                .hasMessageContaining("MyModule.foo");
        assertThat(driver.calls).isEmpty();

        assertThatThrownBy(() -> engine.createWithDataset(database, connection, false, "defaultDataset", Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Initial Fixture");
        assertThat(driver.calls).isEmpty();

        assertThatThrownBy(() ->
                        engine.createByImport(database, "default", connection, sourceConnection, null, false, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Initial Fixture");
        assertThat(driver.calls).isEmpty();
    }

    @Test
    void dropUsesControlDatabaseConnection() {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(RepositoryConfigTestData.singleModule(), Path.of("."));

        engine.drop(database, connection, Map.of());

        assertThat(driver.calls).containsExactly("open(true)", "drop", "close");
    }

    @Test
    void loadDatasetRequiresKnownDataset(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir);

        assertThatThrownBy(() -> engine.loadDataset(database, "missing", connection, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Unknown dataset");
    }

    @Test
    void createByImportCreatesSchemaThenImportsTablesAndRunsHooks(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/import-hooks/pre/pre.sql", "PRE __SOURCE__ __TARGET__");
        createFile(tempDir, "db/import-hooks/post/post.sql", "POST __SOURCE__ __TARGET__");

        final var driver = new RecordingDriver();
        final var output = new ArrayList<String>();
        final var engine = new RuntimeEngine(driver, new FileResolver(), output::add);
        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", tables("[MyModule].[foo]", "[MyModule].[bar]")),
                Map.of("MyModule", List.of()));
        final var importConfig = new ImportConfig(
                "default",
                List.of("MyModule"),
                "import",
                List.of("import-hooks/pre"),
                List.of("import-hooks/post"),
                List.of(),
                null,
                List.of());
        final var database = runtimeDatabase(
                repository, tempDir.resolve("db"), List.of("defaultDataset"), Map.of("default", importConfig));

        engine.createByImport(database, "default", connection, sourceConnection, null, true, Map.of());

        assertThat(driver.calls)
                .containsExactly(
                        "open(false)",
                        "createSchema(MyModule)",
                        "execute(true):PRE IMPORT_DB DBT_TEST",
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
    void contributionsRunAtTheCreateLifecycleSeamAndMigrationOmitsThem(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/./up.sql", "UP");
        createFile(tempDir, "db/MyModule/finalize/final.sql", "FINAL");
        createFile(tempDir, "db/datasets/seed/post/post.sql", "DATASET POST");
        createFile(tempDir, "db/import-hooks/post/post.sql", "IMPORT POST");
        createFile(tempDir, "db/selected/contributions/action.sql", "CONTRIBUTION");
        createFile(tempDir, "db/db-hooks/post/post.sql", "POST");
        createFile(tempDir, "db/migrations/001_test.sql", "MIGRATION");
        final var repository = singleModuleRepository(table("[MyModule].[foo]", RowSource.IMPORT));
        final var importConfig = new ImportConfig(
                "default",
                List.of("MyModule"),
                "import",
                List.of(),
                List.of("import-hooks/post"),
                List.of(),
                null,
                List.of());
        final var database = withContributions(
                runtimeDatabase(repository, tempDir.resolve("db"), List.of("seed"), Map.of("default", importConfig)),
                List.of("selected/contributions"));

        final var createDriver = new RecordingDriver();
        new RuntimeEngine(createDriver, new FileResolver()).create(database, connection, false, Map.of());
        assertThat(createDriver.calls)
                .containsSubsequence(
                        "execute(false):UP",
                        "execute(false):CONTRIBUTION",
                        "execute(false):FINAL",
                        "prepareMigrations",
                        "shouldMigrate(001_test)",
                        "recordMigration(001_test)",
                        "execute(false):POST");

        final var datasetDriver = new RecordingDriver();
        new RuntimeEngine(datasetDriver, new FileResolver())
                .createWithDataset(database, connection, false, "seed", Map.of());
        assertThat(datasetDriver.calls)
                .containsSubsequence(
                        "execute(false):UP",
                        "execute(false):DATASET POST",
                        "execute(false):CONTRIBUTION",
                        "execute(false):FINAL",
                        "prepareMigrations",
                        "shouldMigrate(001_test)",
                        "recordMigration(001_test)",
                        "execute(false):POST");

        final var importDriver = new RecordingDriver();
        new RuntimeEngine(importDriver, new FileResolver())
                .createByImport(database, "default", connection, sourceConnection, null, false, Map.of());
        assertThat(importDriver.calls)
                .containsSubsequence(
                        "execute(false):UP",
                        "execute(true):IMPORT POST",
                        "execute(false):CONTRIBUTION",
                        "execute(false):FINAL",
                        "prepareMigrations",
                        "shouldMigrate(001_test)",
                        "recordMigration(001_test)",
                        "execute(false):POST");

        final var migrationDriver = new RecordingDriver();
        migrationDriver.migrationStatus = new MigrationStatus(true, "1");
        new RuntimeEngine(migrationDriver, new FileResolver()).migrate(database, connection, Map.of());
        assertThat(migrationDriver.calls)
                .contains("execute(false):MIGRATION")
                .doesNotContain("execute(false):CONTRIBUTION");
    }

    @Test
    void lateImportsRunAfterContributionsAndCanResumeWithoutNormalImports(@TempDir final Path tempDir)
            throws IOException {
        createFile(tempDir, "db/import-hooks/post/post.sql", "EARLY POST __SOURCE__ __TARGET__");
        createFile(tempDir, "db/selected/contributions/action.sql", "CONTRIBUTION");
        createFile(tempDir, "db/import-hooks/pre-late/reconcile.sql", "RECONCILE __SOURCE__ __TARGET__");
        createFile(tempDir, "db/MyModule/late-import/MyModule.bar.sql", "LATE BAR __SOURCE__ __TARGET__");
        createFile(tempDir, "db/MyModule/late-import/MyModule.baz.sql", "LATE BAZ __SOURCE__ __TARGET__");
        createFile(tempDir, "db/MyModule/late-import/MyModule.qux.sql", "LATE QUX __SOURCE__ __TARGET__");
        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of(
                        "MyModule",
                        tables("[MyModule].[foo]", "[MyModule].[bar]", "[MyModule].[baz]", "[MyModule].[qux]")),
                Map.of("MyModule", List.of()));
        final var importConfig = new ImportConfig(
                "default",
                List.of("MyModule"),
                "import",
                List.of(),
                List.of("import-hooks/post"),
                List.of("import-hooks/pre-late"),
                "late-import",
                List.of());
        final var database = withContributions(
                runtimeDatabase(repository, tempDir.resolve("db"), List.of(), Map.of("default", importConfig)),
                List.of("selected/contributions"));

        final var driver = new RecordingDriver();
        new RuntimeEngine(driver, new FileResolver())
                .createByImport(database, "default", connection, sourceConnection, null, true, Map.of());

        assertThat(driver.calls)
                .containsSubsequence(
                        "preTableImport(default,[MyModule].[foo])",
                        "execute(true):EARLY POST IMPORT_DB DBT_TEST",
                        "execute(false):CONTRIBUTION",
                        "execute(true):RECONCILE IMPORT_DB DBT_TEST",
                        "preTableImport(default,[MyModule].[bar])",
                        "execute(true):LATE BAR IMPORT_DB DBT_TEST",
                        "preTableImport(default,[MyModule].[baz])",
                        "execute(true):LATE BAZ IMPORT_DB DBT_TEST",
                        "preTableImport(default,[MyModule].[qux])",
                        "execute(true):LATE QUX IMPORT_DB DBT_TEST",
                        "postDataModuleImport(default,MyModule)");

        final var lateTables = List.of("bar", "baz", "qux");
        for (var i = 0; i < lateTables.size(); i++) {
            final var resumeDriver = new RecordingDriver();
            final var resumeTable = lateTables.get(i);
            new RuntimeEngine(resumeDriver, new FileResolver())
                    .createByImport(
                            database,
                            "default",
                            connection,
                            sourceConnection,
                            "MyModule." + resumeTable,
                            false,
                            Map.of());
            assertThat(resumeDriver.calls)
                    .contains("execute(false):DELETE FROM [MyModule].[" + resumeTable + "]")
                    .noneMatch(call -> call.contains("CONTRIBUTION")
                            || call.contains("RECONCILE")
                            || call.contains("MyModule.foo")
                            || call.contains("EARLY POST"));
            for (var j = 0; j < lateTables.size(); j++) {
                final var importCall = "execute(true):LATE " + lateTables.get(j).toUpperCase() + " IMPORT_DB DBT_TEST";
                if (j < i) {
                    assertThat(resumeDriver.calls).doesNotContain(importCall);
                } else {
                    assertThat(resumeDriver.calls).contains(importCall);
                }
            }
        }
    }

    @Test
    void lateImportPlanRejectsOverlapAndNormalTablesAfterLateSuffix(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "overlap/MyModule/import/MyModule.bar.sql", "NORMAL");
        createFile(tempDir, "overlap/MyModule/late-import/MyModule.bar.sql", "LATE");
        createFile(tempDir, "overlap/MyModule/late-import/MyModule.baz.sql", "LATE");
        createFile(tempDir, "interleaved/MyModule/late-import/MyModule.bar.sql", "LATE");
        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", tables("[MyModule].[foo]", "[MyModule].[bar]", "[MyModule].[baz]")),
                Map.of("MyModule", List.of()));
        final var importConfig = new ImportConfig(
                "default", List.of("MyModule"), "import", List.of(), List.of(), List.of(), "late-import", List.of());
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());

        assertThatThrownBy(() -> engine.createByImport(
                        runtimeDatabase(
                                repository, tempDir.resolve("overlap"), List.of(), Map.of("default", importConfig)),
                        "default",
                        connection,
                        sourceConnection,
                        null,
                        true,
                        Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("both normal and late import assets")
                .hasMessageContaining("MyModule.bar");
        assertThat(driver.calls).isEmpty();

        assertThatThrownBy(() -> engine.createByImport(
                        runtimeDatabase(
                                repository, tempDir.resolve("interleaved"), List.of(), Map.of("default", importConfig)),
                        "default",
                        connection,
                        sourceConnection,
                        null,
                        true,
                        Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("late Import Row Source tables as a suffix")
                .hasMessageContaining("MyModule.baz");
        assertThat(driver.calls).isEmpty();
    }

    @Test
    void requiredImportFilesRejectAnIncompleteImportUnitBeforeMutation(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/import-hooks/post/metadata.sql", "METADATA");
        createFile(tempDir, "db/Action/late-import/Action.history.sql", "LATE");
        final var repository = new RepositoryConfig(
                List.of("Selected", "Action"),
                Map.of(),
                Map.of(
                        "Selected", tables("[Selected].[item]"),
                        "Action", tables("[Action].[history]")),
                Map.of("Selected", List.of(), "Action", List.of()));
        final var importConfig = new ImportConfig(
                "default",
                List.of("Selected"),
                "import",
                List.of(),
                List.of("import-hooks/post"),
                List.of(),
                null,
                List.of("import-hooks/post/metadata.sql", "Action/late-import/Action.history.sql"));
        final var driver = new RecordingDriver();

        assertThatThrownBy(() -> new RuntimeEngine(driver, new FileResolver())
                        .createByImport(
                                runtimeDatabase(
                                        repository, tempDir.resolve("db"), List.of(), Map.of("default", importConfig)),
                                "default",
                                connection,
                                sourceConnection,
                                null,
                                true,
                                Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("does not plan required import files")
                .hasMessageContaining("Action.history.sql");
        assertThat(driver.calls).isEmpty();
    }

    @Test
    void subsetImportCanShareALateDirectoryWhenNoUnitRequiresItsAssets(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/Action/late-import/Action.history.sql", "LATE");
        final var repository = new RepositoryConfig(
                List.of("Selected", "Action"),
                Map.of(),
                Map.of(
                        "Selected", tables("[Selected].[item]"),
                        "Action", tables("[Action].[history]")),
                Map.of("Selected", List.of(), "Action", List.of()));
        final var importConfig = new ImportConfig(
                "default", List.of("Selected"), "import", List.of(), List.of(), List.of(), "late-import", List.of());
        final var driver = new RecordingDriver();

        new RuntimeEngine(driver, new FileResolver())
                .createByImport(
                        runtimeDatabase(repository, tempDir.resolve("db"), List.of(), Map.of("default", importConfig)),
                        "default",
                        connection,
                        sourceConnection,
                        null,
                        true,
                        Map.of());

        assertThat(driver.calls)
                .anyMatch(call -> call.contains("[Selected].[item]"))
                .noneMatch(call -> call.contains("LATE") || call.contains("Action.history"));
    }

    @Test
    void timedCreateByImportWritesCanonicalNestedLifecycleAndSqlOperations(@TempDir final Path tempDir)
            throws IOException {
        createFile(tempDir, "db/import-hooks/pre/pre.sql", "PRE\nGO\n\nGO\nPRE2");
        createFile(tempDir, "db/MyModule/import/MyModule.foo.sql", "EXPLICIT\nGO\nEXPLICIT2");
        createFile(tempDir, "db/import-hooks/post/post.sql", "POST");
        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", tables("[MyModule].[foo]", "[MyModule].[bar]")),
                Map.of("MyModule", List.of("[MyModule].[seq]")));
        final var importConfig = new ImportConfig(
                "default",
                List.of("MyModule"),
                "import",
                List.of("import-hooks/pre"),
                List.of("import-hooks/post"),
                List.of(),
                null,
                List.of());
        final var database = withShrinkOnImport(runtimeDatabase(
                repository, tempDir.resolve("db"), List.of("defaultDataset"), Map.of("default", importConfig)));
        final var driver = new RecordingDriver(true);
        final var output = new StringWriter();
        final var clock = new AtomicLong();
        final var timing = new ImportTimingRecorder(output, () -> clock.getAndAdd(1_000L));
        final var engine = new RuntimeEngine(driver, new FileResolver(), ignored -> {}, timing);

        engine.createByImport(database, "default", connection, sourceConnection, null, true, Map.of());

        assertThat(output.toString())
                .contains(
                        observationParent("sql-directory/import-hooks%2Fpre", "phase/data-import"),
                        observationParent("sql-file/import-hooks%2Fpre%2Fpre.sql", "sql-directory/import-hooks%2Fpre"),
                        observationParent(
                                "sql-batch/import-hooks%2Fpre%2Fpre.sql/1", "sql-file/import-hooks%2Fpre%2Fpre.sql"),
                        observationParent(
                                "sql-batch/import-hooks%2Fpre%2Fpre.sql/2", "sql-file/import-hooks%2Fpre%2Fpre.sql"),
                        observationParent("table-transfer/MyModule.foo", "module/import/MyModule"),
                        observationParent(
                                "sql-file/MyModule%2Fimport%2FMyModule.foo.sql", "table-transfer/MyModule.foo"),
                        observationParent(
                                "sql-batch/MyModule%2Fimport%2FMyModule.foo.sql/2",
                                "sql-file/MyModule%2Fimport%2FMyModule.foo.sql"),
                        observationParent("sql-batch/table-transfer%2FMyModule.bar/1", "table-transfer/MyModule.bar"),
                        observationParent(
                                "sql-batch/sequence-transfer%2FMyModule.seq/1", "sequence-transfer/MyModule.seq"),
                        observationParent("maintenance/post-table-reindex/MyModule.foo", "module/import/MyModule"),
                        observationParent("maintenance/module-shrink-notruncate/MyModule", "module/import/MyModule"),
                        observationParent("maintenance/module-shrink-truncate/MyModule", "module/import/MyModule"),
                        observationParent("maintenance/post-shrink-reindex/MyModule.bar", "module/import/MyModule"),
                        observationParent("maintenance/database-update-statistics", "phase/data-import"),
                        observationParent("maintenance/database-update-usage", "phase/data-import"),
                        "\"operation_id\":\"command/create-by-import\",\"parent_operation_id\":null,\"kind\":\"command\",\"status\":\"succeeded\"")
                .doesNotContain(tempDir.toString(), "IMPORT_DB", "DBT_TEST", "secret");
    }

    @Test
    void timedCreateByImportResumeReportsOnlyExecutedSuffix(@TempDir final Path tempDir) throws IOException {
        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", tables("[MyModule].[foo]", "[MyModule].[bar]")),
                Map.of("MyModule", List.of()));
        final var database = runtimeDatabase(repository, tempDir.resolve("db"));
        final var output = new StringWriter();
        final var clock = new AtomicLong();
        final var timing = new ImportTimingRecorder(output, () -> clock.getAndAdd(1_000L));
        final var engine = new RuntimeEngine(new RecordingDriver(true), new FileResolver(), ignored -> {}, timing);

        engine.createByImport(database, "default", connection, sourceConnection, "MyModule.bar", false, Map.of());

        assertThat(output.toString())
                .contains(
                        observationParent("phase/data-import", "command/create-by-import"),
                        observationParent("module/import/MyModule", "phase/data-import"),
                        observationParent("table-clear/MyModule.bar", "module/import/MyModule"),
                        observationParent("table-transfer/MyModule.bar", "module/import/MyModule"),
                        observationParent("phase/module-finalize", "command/create-by-import"),
                        observationParent("phase/post-create", "command/create-by-import"),
                        observationParent("phase/migration-setup", "command/create-by-import"))
                .doesNotContain(
                        "phase/target-prepare",
                        "phase/pre-create",
                        "phase/module-create",
                        "table-clear/MyModule.foo",
                        "table-transfer/MyModule.foo");
    }

    @Test
    void timedImportAttachesSuccessfulSqlTimingTreeToActiveBatch(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/import-hooks/pre/timing.sql", "TIMING");
        final var importConfig = new ImportConfig(
                "default",
                List.of("MyModule"),
                "import",
                List.of("import-hooks/pre"),
                List.of(),
                List.of(),
                null,
                List.of());
        final var database = runtimeDatabase(
                RepositoryConfigTestData.singleModule(),
                tempDir.resolve("db"),
                List.of("defaultDataset"),
                Map.of("default", importConfig));
        final var timingActivationCount = new AtomicLong();
        final var driver = new RecordingDriver() {
            @Override
            public void enableImportTiming() {
                timingActivationCount.incrementAndGet();
            }

            @Override
            public void execute(
                    final String sql, final boolean executeInControlDatabase, final SqlTimingObserver timingObserver) {
                super.execute(sql, executeInControlDatabase, timingObserver);
                if ("TIMING".equals(sql.trim())) {
                    timingObserver.observe(new SqlTimingObservation(
                            1,
                            "analysis-corruption/Analysis/handwritten%2F1",
                            "phase/corruption-checks",
                            "analysis_corruption_check",
                            "succeeded",
                            11));
                    timingObserver.observe(new SqlTimingObservation(
                            2, "phase/corruption-checks", "phase/final-validation", "phase", "succeeded", 22));
                    timingObserver.observe(new SqlTimingObservation(
                            3, "phase/constraint-checks", "phase/final-validation", "phase", "succeeded", 33));
                    timingObserver.observe(new SqlTimingObservation(
                            4, "phase/final-validation", "__JDBT_ACTIVE_SQL_BATCH__", "phase", "succeeded", 44));
                }
            }
        };
        final var output = new StringWriter();
        final var clock = new AtomicLong();
        final var timing = new ImportTimingRecorder(output, () -> clock.getAndAdd(1_000L));
        final var engine = new RuntimeEngine(driver, new FileResolver(), ignored -> {}, timing);

        engine.createByImport(database, "default", connection, sourceConnection, null, true, Map.of());

        final var text = output.toString();
        assertThat(timingActivationCount).hasValue(1);
        assertThat(((RecordingDriver) driver).calls).startsWith("open(false)");
        assertThat(text)
                .contains(
                        observationParent("analysis-corruption/Analysis/handwritten%2F1", "phase/corruption-checks"),
                        observationParent("phase/corruption-checks", "phase/final-validation"),
                        observationParent("phase/constraint-checks", "phase/final-validation"),
                        observationParent("phase/final-validation", "sql-batch/import-hooks%2Fpre%2Ftiming.sql/1"))
                .doesNotContain("__JDBT_ACTIVE_SQL_BATCH__");
        assertThat(text.indexOf("analysis-corruption/Analysis/handwritten%2F1"))
                .isLessThan(text.indexOf("phase/corruption-checks"));
        assertThat(text.indexOf("phase/final-validation"))
                .isLessThan(text.indexOf("sql-batch/import-hooks%2Fpre%2Ftiming.sql/1"));
    }

    @Test
    void sqlTimingValidationRejectsMalformedTrees() {
        assertInvalidSqlTiming(
                List.of(sqlObservation(2, "phase/root", "__JDBT_ACTIVE_SQL_BATCH__", "phase", "succeeded", 1)),
                "ordinal is not contiguous");
        assertInvalidSqlTiming(
                List.of(sqlObservation(1, "phase/root", "__JDBT_ACTIVE_SQL_BATCH__", "unknown", "succeeded", 1)),
                "operation kind");
        assertInvalidSqlTiming(
                List.of(sqlObservation(1, "phase/root", "__JDBT_ACTIVE_SQL_BATCH__", "phase", "unknown", 1)),
                "operation status");
        assertInvalidSqlTiming(
                List.of(sqlObservation(1, "phase/root", "__JDBT_ACTIVE_SQL_BATCH__", "phase", "succeeded", -1)),
                "must not be negative");
        assertInvalidSqlTiming(
                List.of(
                        sqlObservation(
                                1, "phase/final-validation", "__JDBT_ACTIVE_SQL_BATCH__", "phase", "succeeded", 1),
                        sqlObservation(
                                2, "phase/final-validation", "__JDBT_ACTIVE_SQL_BATCH__", "phase", "succeeded", 1)),
                "Duplicate import timing operation ID");
        assertInvalidSqlTiming(
                List.of(sqlObservation(
                        1,
                        "analysis-corruption/Analysis/handwritten%2F1",
                        "phase/corruption-checks",
                        "analysis_corruption_check",
                        "succeeded",
                        1)),
                "contains an unknown parent");
        assertInvalidSqlTiming(
                List.of(sqlObservation(
                        1, "analysis/check", "phase/corruption-checks", "analysis_corruption_check", "succeeded", 1)),
                "corruption identity");
        assertInvalidSqlTiming(
                List.of(sqlObservation(
                        1,
                        "analysis-corruption/Analysis/handwritten%2f1",
                        "phase/corruption-checks",
                        "analysis_corruption_check",
                        "succeeded",
                        1)),
                "corruption identity");
        assertInvalidSqlTiming(
                List.of(sqlObservation(
                        1,
                        "analysis-constraint/foreign-key%2FAction.tblAction%2FFK_Action_Parent",
                        "module/constraint-check/Rose",
                        "analysis_constraint_check",
                        "succeeded",
                        1)),
                "constraint identity");
        assertInvalidSqlTiming(
                List.of(sqlObservation(1, "phase/root", null, "phase", "succeeded", 1)), "parent must not be blank");
    }

    @Test
    void timedCreateByImportFailureCompletesActiveSqlAndJavaAncestors(@TempDir final Path tempDir) {
        final var database = runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir.resolve("db"));
        final var driver = new RecordingDriver() {
            @Override
            public void execute(
                    final String sql, final boolean executeInControlDatabase, final SqlTimingObserver timingObserver) {
                super.execute(sql, executeInControlDatabase, timingObserver);
                if (sql.startsWith("INSERT INTO")) {
                    timingObserver.observe(new SqlTimingObservation(
                            1,
                            "analysis-corruption/Analysis/handwritten%2F1",
                            "phase/corruption-checks",
                            "analysis_corruption_check",
                            "failed",
                            11));
                    timingObserver.observe(new SqlTimingObservation(
                            2, "phase/corruption-checks", "phase/final-validation", "phase", "failed", 22));
                    timingObserver.observe(new SqlTimingObservation(
                            3, "phase/final-validation", "__JDBT_ACTIVE_SQL_BATCH__", "phase", "failed", 33));
                    throw new DatabaseException("database failure");
                }
            }
        };
        final var output = new StringWriter();
        final var clock = new AtomicLong();
        final var timing = new ImportTimingRecorder(output, () -> clock.getAndAdd(1_000L));
        final var engine = new RuntimeEngine(driver, new FileResolver(), ignored -> {}, timing);

        assertThatThrownBy(() ->
                        engine.createByImport(database, "default", connection, sourceConnection, null, true, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Problem importing MyModule.foo")
                .rootCause()
                .isInstanceOf(DatabaseException.class)
                .hasMessage("database failure");

        final var text = output.toString();
        assertThat(text)
                .contains(
                        failedObservation(
                                "analysis-corruption/Analysis/handwritten%2F1",
                                "phase/corruption-checks", "analysis_corruption_check"),
                        failedObservation("phase/corruption-checks", "phase/final-validation", "phase"),
                        failedObservation(
                                "phase/final-validation", "sql-batch/table-transfer%2FMyModule.foo/1", "phase"),
                        failedObservation(
                                "sql-batch/table-transfer%2FMyModule.foo/1",
                                "table-transfer/MyModule.foo", "sql_batch"),
                        failedObservation("table-transfer/MyModule.foo", "module/import/MyModule", "table_transfer"),
                        failedObservation("module/import/MyModule", "phase/data-import", "module"),
                        failedObservation("phase/data-import", "command/create-by-import", "phase"),
                        "\"operation_id\":\"command/create-by-import\",\"parent_operation_id\":null,\"kind\":\"command\",\"status\":\"failed\"");
        assertThat(text.indexOf("analysis-corruption/Analysis/handwritten%2F1"))
                .isLessThan(text.indexOf("phase/corruption-checks"));
        assertThat(text.indexOf("phase/final-validation"))
                .isLessThan(text.indexOf("sql-batch/table-transfer%2FMyModule.foo/1"));
    }

    @Test
    void createByImportUsesRowSourceAndPreservesFixtureSqlStandardPrecedence(@TempDir final Path tempDir)
            throws IOException {
        createFile(tempDir, "db/MyModule/import/MyModule.foo.yml", "r1:\n  ID: 1\n");
        createFile(tempDir, "db/MyModule/import/MyModule.bar.sql", "EXPLICIT __TABLE__ __SOURCE__ __TARGET__");
        final var repository = singleModuleRepository(
                table("[MyModule].[foo]", RowSource.IMPORT),
                table("[MyModule].[bar]", RowSource.IMPORT),
                table("[MyModule].[baz]", RowSource.IMPORT),
                table("[MyModule].[deployment]", RowSource.DEPLOYMENT));
        final var driver = new RecordingDriver();
        final var output = new ArrayList<String>();
        final var engine = new RuntimeEngine(driver, new FileResolver(), output::add);
        final var database = runtimeDatabase(repository, tempDir.resolve("db"));

        engine.createByImport(database, "default", connection, sourceConnection, null, true, Map.of());

        assertThat(driver.calls)
                .containsSubsequence(
                        "preTableImport(default,[MyModule].[foo])",
                        "preFixtureImport([MyModule].[foo])",
                        "insert([MyModule].[foo],{ID=1})",
                        "postFixtureImport([MyModule].[foo])",
                        "postTableImport(default,[MyModule].[foo])",
                        "preTableImport(default,[MyModule].[bar])",
                        "execute(true):EXPLICIT [MyModule].[bar] IMPORT_DB DBT_TEST",
                        "postTableImport(default,[MyModule].[bar])",
                        "preTableImport(default,[MyModule].[baz])",
                        "columnNamesForTable([MyModule].[baz])",
                        "postTableImport(default,[MyModule].[baz])");
        assertThat(driver.calls).noneMatch(call -> call.contains("[MyModule].[deployment]"));
        assertThat(output)
                .containsExactly(
                        "MyModule       : Importing MyModule.foo (By F)",
                        "MyModule       : Importing MyModule.bar (By S)",
                        "MyModule       : Importing MyModule.baz (By D)");
    }

    @Test
    void importRejectsDeploymentRowSourceAssetsBeforeDatabaseMutation(@TempDir final Path tempDir) throws IOException {
        final var repository = singleModuleRepository(table("[MyModule].[deployment]", RowSource.DEPLOYMENT));
        for (final var extension : List.of("yml", "sql")) {
            final var searchDir = tempDir.resolve(extension);
            createFile(searchDir, "MyModule/import/MyModule.deployment." + extension, "content");
            final var driver = new RecordingDriver();
            final var engine = new RuntimeEngine(driver, new FileResolver());
            final var database = runtimeDatabase(repository, searchDir);

            assertThatThrownBy(() -> engine.createByImport(
                            database, "default", connection, sourceConnection, null, true, Map.of()))
                    .isInstanceOf(RuntimeExecutionException.class)
                    .hasMessageContaining("Import Definition 'default'")
                    .hasMessageContaining("Deployment Row Source table 'MyModule.deployment'")
                    .hasMessageContaining(extension);
            assertThat(driver.calls).isEmpty();
        }
    }

    @Test
    void importRejectsResumeAtDeploymentRowSourceBeforeDatabaseMutation(@TempDir final Path tempDir) {
        final var repository = singleModuleRepository(
                table("[MyModule].[foo]", RowSource.IMPORT), table("[MyModule].[deployment]", RowSource.DEPLOYMENT));
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(repository, tempDir.resolve("db"));

        assertThatThrownBy(() -> engine.createByImport(
                        database, "default", connection, sourceConnection, "MyModule.deployment", true, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Import Definition 'default'")
                .hasMessageContaining("can not resume at Deployment Row Source table 'MyModule.deployment'");
        assertThat(driver.calls).isEmpty();
    }

    @Test
    void importResumesAtTableAndErrorsOnUnknownResume(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", tables("[MyModule].[foo]", "[MyModule].[bar]", "[MyModule].[baz]")),
                Map.of("MyModule", List.of()));
        final var database = runtimeDatabase(repository, tempDir.resolve("db"));

        engine.createByImport(database, "default", connection, sourceConnection, "MyModule.bar", true, Map.of());
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
        assertThatThrownBy(() -> engine.createByImport(
                        database, "default", connection, sourceConnection, "Missing.Table", true, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Partial import unable to be completed");
        assertThat(driver.calls).isEmpty();
    }

    @Test
    void importPlanRejectsConflictingSequenceAssetsBeforeDatabaseMutation(@TempDir final Path tempDir)
            throws IOException {
        createFile(tempDir, "db/MyModule/import/MyModule.seq.yml", "1");
        createFile(tempDir, "db/MyModule/import/MyModule.seq.sql", "SELECT 1");
        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", List.of()),
                Map.of("MyModule", List.of("[MyModule].[seq]")));
        final var driver = new RecordingDriver();
        final var database = runtimeDatabase(repository, tempDir.resolve("db"));

        assertThatThrownBy(() -> new RuntimeEngine(driver, new FileResolver())
                        .createByImport(database, "default", connection, sourceConnection, null, true, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("both Import Fixture")
                .hasMessageContaining("MyModule.seq");
        assertThat(driver.calls).isEmpty();
    }

    @Test
    void createByImportUsesConfiguredModuleOrder(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var repository = RepositoryConfigTestData.twoModules();
        final var importConfig = new ImportConfig(
                "custom",
                List.of("MyOtherModule", "MyModule"),
                "import",
                List.of(),
                List.of(),
                List.of(),
                null,
                List.of());
        final var database =
                runtimeDatabase(repository, tempDir.resolve("db"), List.of(), Map.of("custom", importConfig));

        engine.createByImport(database, "custom", connection, sourceConnection, null, true, Map.of());

        final var transcript = String.join("\n", driver.calls);
        assertThat(transcript)
                .containsSubsequence(
                        "createSchema(MyModule)",
                        "createSchema(MyOtherModule)",
                        "preTableImport(custom,[MyOtherModule].[baz])",
                        "preTableImport(custom,[MyOtherModule].[bark])",
                        "preTableImport(custom,[MyModule].[foo])",
                        "preTableImport(custom,[MyModule].[bar])");
    }

    @Test
    void importRejectsUnexpectedImportFiles(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/import/unexpected.sql", "SELECT 1");
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir.resolve("db"));

        assertThatThrownBy(() ->
                        engine.createByImport(database, "default", connection, sourceConnection, null, true, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Discovered additional files in import directory");
        assertThat(driver.calls).isEmpty();
    }

    @Test
    void createWithDatasetRunsDatasetHooksAndFixtureLoad(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/./up.sql", "UP");
        createFile(tempDir, "db/MyModule/finalize/final.sql", "FINAL");
        createFile(
                tempDir,
                "db/datasets/myset/pre/pre.sql",
                "ASSERT_DATABASE_VERSION('Version_2')\nSELECT 'go up' AS Direction\nGO\nDSPRE");
        createFile(tempDir, "db/datasets/myset/post/post.sql", "DSPOST");
        createFile(tempDir, "db/MyModule/datasets/myset/MyModule.foo.yml", """
            - r1:
                ID: 2
            - r2:
                ID: 3
            """);
        createFile(tempDir, "db/MyModule/datasets/myset/MyModule.bar.yml", """
            r1:
              ID: 4
            """);
        createFile(tempDir, "db/MyModule/datasets/myset/MyModule.baz.yml", "[]\n");

        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                singleModuleRepository(
                        table("[MyModule].[foo]", RowSource.IMPORT),
                        table("[MyModule].[bar]", RowSource.DEPLOYMENT),
                        table("[MyModule].[baz]", RowSource.DEPLOYMENT)),
                tempDir.resolve("db"),
                List.of("myset"));

        engine.createWithDataset(database, connection, false, "myset", Map.of());

        assertThat(driver.calls)
                .containsSubsequence(
                        "open(true)",
                        "drop",
                        "createDatabase",
                        "open(false)",
                        "createSchema(MyModule)",
                        "execute(false):UP",
                        "execute(false):SELECT 'go up' AS Direction",
                        "execute(false):DSPRE",
                        "execute(false):DELETE FROM [MyModule].[bar]",
                        "execute(false):DELETE FROM [MyModule].[foo]",
                        "preFixtureImport([MyModule].[foo])",
                        "insert([MyModule].[foo],{ID=2})",
                        "insert([MyModule].[foo],{ID=3})",
                        "postFixtureImport([MyModule].[foo])",
                        "preFixtureImport([MyModule].[bar])",
                        "insert([MyModule].[bar],{ID=4})",
                        "postFixtureImport([MyModule].[bar])",
                        "preFixtureImport([MyModule].[baz])",
                        "postFixtureImport([MyModule].[baz])",
                        "execute(false):DSPOST",
                        "execute(false):FINAL");
        assertThat(driver.calls)
                .filteredOn(call -> call.contains("FixtureImport") || call.startsWith("insert([MyModule]"))
                .containsExactly(
                        "preFixtureImport([MyModule].[foo])",
                        "insert([MyModule].[foo],{ID=2})",
                        "insert([MyModule].[foo],{ID=3})",
                        "postFixtureImport([MyModule].[foo])",
                        "preFixtureImport([MyModule].[bar])",
                        "insert([MyModule].[bar],{ID=4})",
                        "postFixtureImport([MyModule].[bar])",
                        "preFixtureImport([MyModule].[baz])",
                        "postFixtureImport([MyModule].[baz])");
        assertThat(String.join("\n", driver.calls))
                .doesNotContain("ASSERT_DATABASE_VERSION")
                .contains("Expected DatabaseSchemaVersion in current database");
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
                        "MyModule", tables("[MyModule].[foo]"),
                        "MyOtherModule", tables("[MyOtherModule].[baz]")),
                Map.of("MyModule", List.of("[MyModule].[fooSeq]"), "MyOtherModule", List.of()));
        final var database = runtimeDatabase(
                repository,
                tempDir.resolve("db"),
                List.of("defaultDataset"),
                Map.of(
                        "default",
                        new ImportConfig(
                                "default",
                                repository.modules(),
                                "import",
                                List.of(),
                                List.of(),
                                List.of(),
                                null,
                                List.of())),
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
        final var database = runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir.resolve("db"));

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
        final var database =
                runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir.resolve("db"), List.of("sample"));

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
        final var database = runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir.resolve("db"));

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
        final var database = runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir.resolve("db"));

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
                Map.of("MyModule", tables("[MyModule].[foo]")),
                Map.of("MyModule", List.of("\"MyModule\".\"foo\"")));
        final var duplicateCleanDatabase = runtimeDatabase(duplicateCleanRepository, tempDir.resolve("db"));
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
        final var database = runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir.resolve("db"));

        assertThatThrownBy(() -> engine.exportFixtures(
                        database, connection, tempDir.resolve("exports.properties"), null, tempDir, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("duplicate column label");

        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", List.of()),
                Map.of("MyModule", List.of("[MyModule].[fooSeq]")));
        final var sequenceDatabase = runtimeDatabase(repository, tempDir.resolve("db"));
        createFile(tempDir, "sequence.properties", "MyModule.fooSeq=SELECT value FROM seq\n");
        driver.queryResults.put(
                "SELECT value FROM seq", new QueryResult(List.of("value"), List.of(List.of(1), List.of(2))));

        assertThatThrownBy(() -> engine.exportFixtures(
                        sequenceDatabase, connection, tempDir.resolve("sequence.properties"), null, tempDir, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("exactly one row with exactly one column");
    }

    @Test
    void createByImportSkipsCreatePathWhenResuming(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/db-hooks/post/post.sql", "ASSERT_DATABASE_VERSION('Version_2')");
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", tables("[MyModule].[foo]", "[MyModule].[bar]")),
                Map.of("MyModule", List.of()));
        final var database = runtimeDatabase(repository, tempDir.resolve("db"));

        engine.createByImport(database, "default", connection, sourceConnection, "MyModule.bar", false, Map.of());

        assertThat(driver.calls)
                .containsSubsequence(
                        "open(false)",
                        "execute(false):DELETE FROM [MyModule].[bar]",
                        "preTableImport(default,[MyModule].[bar])",
                        "postDatabaseImport(default)",
                        "close");
        assertThat(driver.calls).doesNotContain("open(true)", "drop", "createSchema(MyModule)");
        assertThat(String.join("\n", driver.calls))
                .doesNotContain("ASSERT_DATABASE_VERSION")
                .contains("Expected DatabaseSchemaVersion in current database");
    }

    @Test
    void importIncludesSequenceProcessing(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/import/MyModule.fooSeq.yml", "--- 23\n");
        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var repository = new RepositoryConfig(
                List.of("MyModule"),
                Map.of(),
                Map.of("MyModule", tables("[MyModule].[foo]")),
                Map.of("MyModule", List.of("[MyModule].[fooSeq]")));
        final var database = runtimeDatabase(repository, tempDir.resolve("db"));

        engine.createByImport(database, "default", connection, sourceConnection, null, true, Map.of());
        assertThat(driver.calls).contains("updateSequence([MyModule].[fooSeq],23)");
    }

    @Test
    void createByImportUsesArtifactLocationsWhenImportFilesInZip(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        final var output = new StringWriter();
        final var clock = new AtomicLong();
        final var timing = new ImportTimingRecorder(output, () -> clock.getAndAdd(1_000L));
        final var engine = new RuntimeEngine(driver, new FileResolver(), ignored -> {}, timing);
        final var repository = RepositoryConfigTestData.singleModule();
        final var database = new RuntimeDatabase(
                repository,
                tempDir.resolve("db"),
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
                "migrations",
                "1",
                "hash",
                null,
                null,
                false,
                true,
                true,
                false,
                Map.of(),
                Map.of(
                        "default",
                        new ImportConfig(
                                "default",
                                repository.modules(),
                                "import",
                                List.of(),
                                List.of(),
                                List.of(),
                                null,
                                List.of())),
                List.of());

        engine.createByImport(database, "default", connection, sourceConnection, null, true, Map.of());
        assertThat(driver.calls).contains("execute(true):SELECT IMPORT_DB DBT_TEST");
        assertThat(output.toString()).contains("\"operation_id\":\"sql-file/MyModule%2Fimport%2FMyModule.foo.sql\"");
    }

    @Test
    void createAppliesDeclaredFilterPropertiesInConfigOrder(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/./up.sql", "SELECT __A__ __B__");

        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                RepositoryConfigTestData.singleModule(),
                tempDir.resolve("db"),
                List.of("defaultDataset"),
                Map.of(
                        "default",
                        new ImportConfig(
                                "default",
                                List.of("MyModule"),
                                "import",
                                List.of(),
                                List.of(),
                                List.of(),
                                null,
                                List.of())),
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
                RepositoryConfigTestData.singleModule(),
                tempDir.resolve("db"),
                List.of("defaultDataset"),
                Map.of(
                        "default",
                        new ImportConfig(
                                "default",
                                List.of("MyModule"),
                                "import",
                                List.of(),
                                List.of(),
                                List.of(),
                                null,
                                List.of())),
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
                RepositoryConfigTestData.singleModule(),
                tempDir.resolve("db"),
                List.of("defaultDataset"),
                Map.of(
                        "default",
                        new ImportConfig(
                                "default",
                                List.of("MyModule"),
                                "import",
                                List.of(),
                                List.of(),
                                List.of(),
                                null,
                                List.of())),
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
                RepositoryConfigTestData.singleModule(),
                tempDir.resolve("db"),
                List.of("defaultDataset"),
                Map.of(
                        "default",
                        new ImportConfig(
                                "default",
                                List.of("MyModule"),
                                "import",
                                List.of(),
                                List.of(),
                                List.of(),
                                null,
                                List.of())),
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
                RepositoryConfigTestData.singleModule(),
                tempDir.resolve("db"),
                List.of("defaultDataset"),
                Map.of(
                        "default",
                        new ImportConfig(
                                "default",
                                List.of("MyModule"),
                                "import",
                                List.of(),
                                List.of(),
                                List.of(),
                                null,
                                List.of())),
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
        final var database = runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir.resolve("db"));

        engine.createByImport(database, "default", connection, sourceConnection, null, true, Map.of());

        assertThat(driver.calls).contains("execute(true):SELECT @@SOURCE@@ @@TARGET@@ IMPORT_DB DBT_TEST");
    }

    @Test
    void importExpandsAssertFilters(@TempDir final Path tempDir) throws IOException {
        createFile(
                tempDir,
                "db/MyModule/import/MyModule.foo.sql",
                "ASSERT_ROW_COUNT(1)\nASSERT_UNCHANGED_ROW_COUNT()\nASSERT_DATABASE_VERSION('Version_2')");

        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir.resolve("db"));

        engine.createByImport(database, "default", connection, sourceConnection, null, true, Map.of());

        assertThat(String.join("\n", driver.calls))
                .doesNotContain("ASSERT_ROW_COUNT")
                .doesNotContain("ASSERT_UNCHANGED_ROW_COUNT")
                .doesNotContain("ASSERT_DATABASE_VERSION")
                .contains("COUNT(*) FROM [DBT_TEST].[MyModule].[foo]")
                .contains("COUNT(*) FROM [IMPORT_DB].[MyModule].[foo]")
                .contains("DatabaseSchemaVersion")
                .contains("RAISERROR");
    }

    @Test
    void createExpandsOnlyDatabaseVersionAssert(@TempDir final Path tempDir) throws IOException {
        createFile(
                tempDir,
                "db/db-hooks/post/post.sql",
                "ASSERT_DATABASE_VERSION('Version_2')\nASSERT_ROW_COUNT(1)\nASSERT_UNCHANGED_ROW_COUNT()");
        final var database = runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir.resolve("db"));

        final var driver = new RecordingDriver();
        new RuntimeEngine(driver, new FileResolver()).create(database, connection, false, Map.of());

        assertThat(String.join("\n", driver.calls))
                .doesNotContain("ASSERT_DATABASE_VERSION")
                .doesNotContain("__SOURCE__")
                .doesNotContain("__TARGET__")
                .contains("FROM sys.fn_listextendedproperty")
                .contains("Expected DatabaseSchemaVersion in current database")
                .contains("ASSERT_ROW_COUNT(1)")
                .contains("ASSERT_UNCHANGED_ROW_COUNT()");
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
        final var database = runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir.resolve("db"));

        engine.migrate(database, connection, Map.of());

        assertThat(driver.calls)
                .containsSubsequence(
                        "open(false)",
                        "prepareMigrations",
                        "shouldMigrate(001_a)",
                        "shouldMigrate(002_b)",
                        "applyMigration(002_b)",
                        "execute(false):M2",
                        "recordMigration(002_b)",
                        "close");
        assertThat(driver.calls).doesNotContain("applyMigration(001_a)", "recordMigration(001_a)");
        assertThat(driver.migrationChecksums.get("002_b")).hasSize(64);
        assertThat(output).containsExactly("Migration: 002_b.sql");
    }

    @Test
    void migrateBootstrapsThroughReleaseMarkerForLiveDatabaseVersion(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/migrations/001_x.sql", "M1");
        createFile(tempDir, "db/migrations/002_Release-Version_1.sql", "M2");
        createFile(tempDir, "db/migrations/003_z.sql", "M3");

        final var driver = new RecordingDriver();
        driver.migrationStatus = new MigrationStatus(false, "Version_1");
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                RepositoryConfigTestData.singleModule(),
                tempDir.resolve("db"),
                List.of("defaultDataset"),
                Map.of(
                        "default",
                        new ImportConfig(
                                "default",
                                List.of("MyModule"),
                                "import",
                                List.of(),
                                List.of(),
                                List.of(),
                                null,
                                List.of())),
                Map.of(),
                "Configured_Version_2");

        engine.migrate(database, connection, Map.of());

        assertThat(driver.calls)
                .containsSubsequence(
                        "prepareMigrations",
                        "initializeMigrationState([001_x, 002_Release-Version_1])",
                        "applyMigration(003_z)",
                        "execute(false):M3",
                        "recordMigration(003_z)");
        assertThat(driver.calls).doesNotContain("execute(false):M1", "execute(false):M2");
    }

    @Test
    void migrateExistingStateRunsEveryUnrecordedMigrationRegardlessOfReleaseMarkers(@TempDir final Path tempDir)
            throws IOException {
        createFile(tempDir, "db/migrations/001_x.sql", "M1");
        createFile(tempDir, "db/migrations/002_Release-1.sql", "M2");
        createFile(tempDir, "db/migrations/003_z.sql", "M3");

        final var driver = new RecordingDriver();
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir.resolve("db"));

        engine.migrate(database, connection, Map.of());

        assertThat(driver.calls)
                .containsSubsequence(
                        "execute(false):M1",
                        "recordMigration(001_x)",
                        "execute(false):M2",
                        "recordMigration(002_Release-1)",
                        "execute(false):M3",
                        "recordMigration(003_z)");
    }

    @Test
    void createInitializesMigrationStateWithoutExecutingFiles(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/migrations/001_x.sql", "M1");

        final var driver = new RecordingDriver();
        driver.migrationStatus = new MigrationStatus(false, "1");
        final var engine = new RuntimeEngine(driver, new FileResolver());
        final var database = runtimeDatabase(
                RepositoryConfigTestData.singleModule(),
                tempDir.resolve("db"),
                List.of("defaultDataset"),
                Map.of(
                        "default",
                        new ImportConfig(
                                "default",
                                List.of("MyModule"),
                                "import",
                                List.of(),
                                List.of(),
                                List.of(),
                                null,
                                List.of())),
                Map.of(),
                "1");

        engine.create(database, connection, false, Map.of());

        assertThat(driver.calls).containsSubsequence("prepareMigrations", "initializeMigrationState([001_x])");
        assertThat(driver.calls).doesNotContain("execute(false):M1", "shouldMigrate(001_x)", "applyMigration(001_x)");
    }

    @Test
    void migrateRejectsMissingLiveVersionReleaseMarkerWithoutInitializingState(@TempDir final Path tempDir)
            throws IOException {
        createFile(tempDir, "db/migrations/001_x.sql", "M1");
        final var driver = new RecordingDriver();
        driver.migrationStatus = new MigrationStatus(false, "17");
        final var database = runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir.resolve("db"));

        assertThatThrownBy(() -> new RuntimeEngine(driver, new FileResolver()).migrate(database, connection, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("no Release-17 migration exists");

        assertThat(driver.calls).doesNotContain("initializeMigrationState([001_x])", "execute(false):M1");
    }

    @Test
    void migrateRejectsProjectWithoutMigrationFilesBeforeOpeningDatabase(@TempDir final Path tempDir) {
        final var driver = new RecordingDriver();
        final var database = runtimeDatabase(RepositoryConfigTestData.singleModule(), tempDir.resolve("db"));

        assertThatThrownBy(() -> new RuntimeEngine(driver, new FileResolver()).migrate(database, connection, Map.of()))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("No migration SQL files found");

        assertThat(driver.calls).isEmpty();
    }

    private static RuntimeDatabase runtimeDatabase(final RepositoryConfig repository, final Path resourceRoot) {
        return runtimeDatabase(repository, resourceRoot, List.of("defaultDataset"));
    }

    private static RuntimeDatabase runtimeDatabase(
            final RepositoryConfig repository, final Path resourceRoot, final List<String> datasets) {
        return runtimeDatabase(
                repository,
                resourceRoot,
                datasets,
                Map.of(
                        "default",
                        new ImportConfig(
                                "default",
                                repository.modules(),
                                "import",
                                List.of(),
                                List.of(),
                                List.of(),
                                null,
                                List.of())),
                Map.of());
    }

    private static RuntimeDatabase runtimeDatabase(
            final RepositoryConfig repository,
            final Path resourceRoot,
            final List<String> datasets,
            final Map<String, ImportConfig> imports) {
        return runtimeDatabase(repository, resourceRoot, datasets, imports, Map.of(), "1");
    }

    private static RuntimeDatabase runtimeDatabase(
            final RepositoryConfig repository,
            final Path resourceRoot,
            final List<String> datasets,
            final Map<String, ImportConfig> imports,
            final Map<String, FilterPropertyConfig> filterProperties) {
        return runtimeDatabase(repository, resourceRoot, datasets, imports, filterProperties, "1");
    }

    @SuppressWarnings("SameParameterValue")
    private static RuntimeDatabase runtimeDatabase(
            final RepositoryConfig repository,
            final Path resourceRoot,
            final List<String> datasets,
            final Map<String, ImportConfig> imports,
            final Map<String, FilterPropertyConfig> filterProperties,
            final String version) {
        return new RuntimeDatabase(
                repository,
                resourceRoot,
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
                "migrations",
                version,
                "hash",
                null,
                null,
                false,
                true,
                true,
                false,
                filterProperties,
                imports,
                List.of());
    }

    private static RuntimeDatabase withContributions(
            final RuntimeDatabase database, final List<String> contributionDirs) {
        return new RuntimeDatabase(
                database.repository(),
                database.resourceRoot(),
                database.preDbArtifacts(),
                database.postDbArtifacts(),
                database.indexFileName(),
                database.upDirs(),
                database.downDirs(),
                database.finalizeDirs(),
                database.preCreateDirs(),
                database.postCreateDirs(),
                database.fixtureDirName(),
                database.datasetsDirName(),
                database.preDatasetDirs(),
                database.postDatasetDirs(),
                database.datasets(),
                database.migrationDir(),
                database.version(),
                database.schemaHash(),
                database.dataPath(),
                database.logPath(),
                database.forceDrop(),
                database.deleteBackupHistory(),
                database.reindexOnImport(),
                database.shrinkOnImport(),
                database.filterProperties(),
                database.imports(),
                contributionDirs);
    }

    private static int indexOf(final List<String> values, final String expected) {
        for (int i = 0; i < values.size(); i++) {
            if (expected.equals(values.get(i))) {
                return i;
            }
        }
        return -1;
    }

    private static String observationParent(final String operationId, final String parentOperationId) {
        return "\"operation_id\":\"" + operationId + "\",\"parent_operation_id\":\"" + parentOperationId + '"';
    }

    private static String failedObservation(
            final String operationId, final String parentOperationId, final String kind) {
        return observationParent(operationId, parentOperationId) + ",\"kind\":\"" + kind + "\",\"status\":\"failed\"";
    }

    private static SqlTimingObservation sqlObservation(
            final long ordinal,
            final String operationId,
            final @Nullable String parentOperationId,
            final String kind,
            final String status,
            final long elapsedMicroseconds) {
        return new SqlTimingObservation(ordinal, operationId, parentOperationId, kind, status, elapsedMicroseconds);
    }

    private static void assertInvalidSqlTiming(
            final List<SqlTimingObservation> observations, final String expectedMessage) {
        final var timing = new ImportTimingRecorder(new StringWriter(), System::nanoTime);
        assertThatThrownBy(() -> timing.command(
                        "command/create-by-import",
                        () -> timing.run("sql-batch/file/1", "sql_batch", () -> {
                            timing.beginSqlBatch();
                            observations.forEach(timing::recordSqlObservation);
                            timing.completeSqlBatch();
                        })))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expectedMessage);
    }

    private static RuntimeDatabase withShrinkOnImport(final RuntimeDatabase database) {
        return new RuntimeDatabase(
                database.repository(),
                database.resourceRoot(),
                database.preDbArtifacts(),
                database.postDbArtifacts(),
                database.indexFileName(),
                database.upDirs(),
                database.downDirs(),
                database.finalizeDirs(),
                database.preCreateDirs(),
                database.postCreateDirs(),
                database.fixtureDirName(),
                database.datasetsDirName(),
                database.preDatasetDirs(),
                database.postDatasetDirs(),
                database.datasets(),
                database.migrationDir(),
                database.version(),
                database.schemaHash(),
                database.dataPath(),
                database.logPath(),
                database.forceDrop(),
                database.deleteBackupHistory(),
                database.reindexOnImport(),
                true,
                database.filterProperties(),
                database.imports(),
                database.contributionDirs());
    }

    private static void createFile(final Path root, final String relativePath, final String content)
            throws IOException {
        final var file = root.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private static class RecordingDriver implements DbDriver {
        private final List<String> calls = new ArrayList<>();
        private final Map<String, Boolean> migrateDecision = new LinkedHashMap<>();
        private final Map<String, String> migrationChecksums = new LinkedHashMap<>();
        private final Map<String, QueryResult> queryResults = new LinkedHashMap<>();
        private final boolean observeMaintenance;
        private MigrationStatus migrationStatus = new MigrationStatus(true, "1");
        private List<String> primaryKeyColumnNames = List.of("[ID]");

        private RecordingDriver() {
            this(false);
        }

        private RecordingDriver(final boolean observeMaintenance) {
            this.observeMaintenance = observeMaintenance;
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
            calls.add("drop");
        }

        @Override
        public void createDatabase(final DatabaseMetadata database, final DatabaseConnection connection) {
            calls.add("createDatabase");
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
        public void execute(
                final String sql, final boolean executeInControlDatabase, final SqlTimingObserver timingObserver) {
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
                final DatabaseMetadata database,
                final ImportConfig importConfig,
                final String tableName,
                final ImportMaintenanceObserver observer) {
            calls.add("postTableImport(" + importConfig.key() + ',' + tableName + ")");
            if (observeMaintenance && database.reindexOnImport()) {
                observer.run("post-table-reindex", tableName, () -> {});
            }
        }

        @Override
        public void postDataModuleImport(
                final DatabaseMetadata database,
                final ImportConfig importConfig,
                final String moduleName,
                final List<String> tablesInOrder,
                final ImportMaintenanceObserver observer) {
            calls.add("postDataModuleImport(" + importConfig.key() + ',' + moduleName + ")");
            if (observeMaintenance && database.shrinkOnImport()) {
                observer.run("module-shrink-notruncate", moduleName, () -> {});
                observer.run("module-shrink-truncate", moduleName, () -> {});
                if (database.reindexOnImport()) {
                    for (final var table : tablesInOrder) {
                        observer.run("post-shrink-reindex", table, () -> {});
                    }
                }
            }
        }

        @Override
        public void postDatabaseImport(
                final DatabaseMetadata database,
                final ImportConfig importConfig,
                final ImportMaintenanceObserver observer) {
            calls.add("postDatabaseImport(" + importConfig.key() + ")");
            if (observeMaintenance && database.reindexOnImport()) {
                observer.run("database-update-statistics", null, () -> {});
                observer.run("database-update-usage", null, () -> {});
            }
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
        public MigrationStatus prepareMigrations() {
            calls.add("prepareMigrations");
            return migrationStatus;
        }

        @Override
        public void initializeMigrationState(final Map<String, String> migrations) {
            migrationChecksums.putAll(migrations);
            calls.add("initializeMigrationState(" + migrations.keySet() + ")");
        }

        @Override
        public boolean shouldMigrate(final String migrationName, final String checksum) {
            migrationChecksums.put(migrationName, checksum);
            calls.add("shouldMigrate(" + migrationName + ")");
            return migrateDecision.getOrDefault(migrationName, true);
        }

        @Override
        public void recordMigration(final String migrationName, final String checksum) {
            migrationChecksums.put(migrationName, checksum);
            calls.add("recordMigration(" + migrationName + ")");
        }

        @Override
        public void applyMigration(final String migrationName, final String checksum, final Runnable action) {
            calls.add("applyMigration(" + migrationName + ")");
            action.run();
            recordMigration(migrationName, checksum);
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

    private static List<RepositoryTable> tables(final String... names) {
        return Arrays.stream(names).map(name -> table(name, RowSource.IMPORT)).toList();
    }

    private static RepositoryTable table(final String name, final RowSource rowSource) {
        return new RepositoryTable(name, List.of("[ID]"), List.of(), rowSource);
    }

    private static RepositoryConfig singleModuleRepository(final RepositoryTable... tables) {
        return new RepositoryConfig(
                List.of("MyModule"), Map.of(), Map.of("MyModule", List.of(tables)), Map.of("MyModule", List.of()));
    }

    private static final class RepositoryConfigTestData {
        static RepositoryConfig singleModule() {
            return new RepositoryConfig(
                    List.of("MyModule"),
                    Map.of(),
                    Map.of("MyModule", tables("[MyModule].[foo]")),
                    Map.of("MyModule", List.of()));
        }

        static RepositoryConfig twoModules() {
            return new RepositoryConfig(
                    List.of("MyModule", "MyOtherModule"),
                    Map.of(),
                    Map.of(
                            "MyModule", tables("[MyModule].[foo]", "[MyModule].[bar]"),
                            "MyOtherModule", tables("[MyOtherModule].[baz]", "[MyOtherModule].[bark]")),
                    Map.of("MyModule", List.of(), "MyOtherModule", List.of()));
        }
    }
}
