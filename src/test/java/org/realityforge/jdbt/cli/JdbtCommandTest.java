package org.realityforge.jdbt.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.db.DatabaseConnection;

final class JdbtCommandTest {
    @Test
    void statusDispatchesToRunnerWithDefaultDriver() {
        final RecordingRunner runner = new RecordingRunner();

        final int exitCode = JdbtCommand.execute(
                new String[] {"status", "--database", "default"},
                runner,
                new PasswordResolver(java.util.Map.of(), new ByteArrayInputStream(new byte[0])));

        assertThat(exitCode).isZero();
        assertThat(runner.lastCall).isEqualTo("status");
        assertThat(runner.databaseKey).isEqualTo("default");
        assertThat(runner.driver).isEqualTo("sqlserver");
    }

    @Test
    void createDispatchesWithTargetConnectionAndNoCreateFlag() {
        final RecordingRunner runner = new RecordingRunner();

        final int exitCode = JdbtCommand.execute(
                new String[] {
                    "create",
                    "--database",
                    "default",
                    "--target-host",
                    "localhost",
                    "--target-port",
                    "1433",
                    "--target-database",
                    "db",
                    "--target-username",
                    "sa",
                    "--password",
                    "secret",
                    "--no-create"
                },
                runner,
                new PasswordResolver(java.util.Map.of(), new ByteArrayInputStream(new byte[0])));

        assertThat(exitCode).isZero();
        assertThat(runner.lastCall).isEqualTo("create");
        assertThat(runner.targetConnection).isEqualTo(new DatabaseConnection("localhost", 1433, "db", "sa", "secret"));
        assertThat(runner.noCreate).isTrue();
    }

    @Test
    void importDispatchesWithTargetAndSourceConnectionsAndResumeAt() {
        final RecordingRunner runner = new RecordingRunner();
        final int exitCode = JdbtCommand.execute(
                new String[] {
                    "import",
                    "--database",
                    "default",
                    "--import",
                    "full",
                    "--module-group",
                    "core",
                    "--resume-at",
                    "Core.Table",
                    "--target-host",
                    "thost",
                    "--target-port",
                    "1433",
                    "--target-database",
                    "tdb",
                    "--target-username",
                    "tuser",
                    "--password-env",
                    "T_PASS",
                    "--source-host",
                    "shost",
                    "--source-port",
                    "1432",
                    "--source-database",
                    "sdb",
                    "--source-username",
                    "suser",
                    "--source-password-env",
                    "S_PASS"
                },
                runner,
                new PasswordResolver(
                        java.util.Map.of("T_PASS", "target-secret", "S_PASS", "source-secret"),
                        new ByteArrayInputStream(new byte[0])));

        assertThat(exitCode).isZero();
        assertThat(runner.lastCall).isEqualTo("import");
        assertThat(runner.importKey).isEqualTo("full");
        assertThat(runner.moduleGroup).isEqualTo("core");
        assertThat(runner.resumeAt).isEqualTo("Core.Table");
        assertThat(runner.targetConnection)
                .isEqualTo(new DatabaseConnection("thost", 1433, "tdb", "tuser", "target-secret"));
        assertThat(runner.sourceConnection)
                .isEqualTo(new DatabaseConnection("shost", 1432, "sdb", "suser", "source-secret"));
    }

    @Test
    void createByImportSupportsStdinPasswords() {
        final RecordingRunner runner = new RecordingRunner();
        final byte[] stdin = "target\nsource\n".getBytes(StandardCharsets.UTF_8);
        final int exitCode = JdbtCommand.execute(
                new String[] {
                    "create-by-import",
                    "--target-host",
                    "thost",
                    "--target-port",
                    "1433",
                    "--target-database",
                    "tdb",
                    "--target-username",
                    "tuser",
                    "--password-stdin",
                    "--source-host",
                    "shost",
                    "--source-port",
                    "1433",
                    "--source-database",
                    "sdb",
                    "--source-username",
                    "suser",
                    "--source-password-stdin"
                },
                runner,
                new PasswordResolver(java.util.Map.of(), new ByteArrayInputStream(stdin)));

        assertThat(exitCode).isZero();
        assertThat(runner.lastCall).isEqualTo("create-by-import");
        assertThat(runner.targetConnection.password()).isEqualTo("target");
        assertThat(runner.sourceConnection.password()).isEqualTo("source");
    }

    @Test
    void packageDataDispatchesOutputPath() {
        final RecordingRunner runner = new RecordingRunner();

        final int exitCode = JdbtCommand.execute(
                new String[] {"package-data", "--database", "default", "--output", "build/out.zip"},
                runner,
                new PasswordResolver(java.util.Map.of(), new ByteArrayInputStream(new byte[0])));

        assertThat(exitCode).isZero();
        assertThat(runner.lastCall).isEqualTo("package-data");
        assertThat(runner.databaseKey).isEqualTo("default");
        assertThat(runner.outputFile).isEqualTo(Path.of("build/out.zip"));
    }

    @Test
    void createRequiresPasswordOption() {
        final RecordingRunner runner = new RecordingRunner();
        final int exitCode = JdbtCommand.execute(
                new String[] {
                    "create",
                    "--target-host",
                    "localhost",
                    "--target-port",
                    "1433",
                    "--target-database",
                    "db",
                    "--target-username",
                    "sa"
                },
                runner,
                new PasswordResolver(java.util.Map.of(), new ByteArrayInputStream(new byte[0])));

        assertThat(exitCode).isEqualTo(JdbtCommand.USAGE_EXIT_CODE);
    }

    private static final class RecordingRunner implements CommandRunner {
        private String lastCall = "";
        private @Nullable String databaseKey;
        private String driver = "";
        private @Nullable String importKey;
        private @Nullable String moduleGroup;
        private @Nullable String resumeAt;
        private @Nullable DatabaseConnection targetConnection;
        private @Nullable DatabaseConnection sourceConnection;
        private boolean noCreate;
        private @Nullable Path outputFile;

        @Override
        public void status(final @Nullable String databaseKey, final String driver) {
            this.lastCall = "status";
            this.databaseKey = databaseKey;
            this.driver = driver;
        }

        @Override
        public void create(
                final @Nullable String databaseKey,
                final String driver,
                final DatabaseConnection target,
                final boolean noCreate) {
            this.lastCall = "create";
            this.databaseKey = databaseKey;
            this.driver = driver;
            this.targetConnection = target;
            this.noCreate = noCreate;
        }

        @Override
        public void drop(final @Nullable String databaseKey, final String driver, final DatabaseConnection target) {
            this.lastCall = "drop";
            this.databaseKey = databaseKey;
            this.driver = driver;
            this.targetConnection = target;
        }

        @Override
        public void migrate(final @Nullable String databaseKey, final String driver, final DatabaseConnection target) {
            this.lastCall = "migrate";
            this.databaseKey = databaseKey;
            this.driver = driver;
            this.targetConnection = target;
        }

        @Override
        public void databaseImport(
                final @Nullable String databaseKey,
                final String driver,
                final @Nullable String importKey,
                final @Nullable String moduleGroup,
                final DatabaseConnection target,
                final DatabaseConnection source,
                final @Nullable String resumeAt) {
            this.lastCall = "import";
            this.databaseKey = databaseKey;
            this.driver = driver;
            this.importKey = importKey;
            this.moduleGroup = moduleGroup;
            this.targetConnection = target;
            this.sourceConnection = source;
            this.resumeAt = resumeAt;
        }

        @Override
        public void createByImport(
                final @Nullable String databaseKey,
                final String driver,
                final @Nullable String importKey,
                final DatabaseConnection target,
                final DatabaseConnection source,
                final @Nullable String resumeAt,
                final boolean noCreate) {
            this.lastCall = "create-by-import";
            this.databaseKey = databaseKey;
            this.driver = driver;
            this.importKey = importKey;
            this.targetConnection = target;
            this.sourceConnection = source;
            this.resumeAt = resumeAt;
            this.noCreate = noCreate;
        }

        @Override
        public void loadDataset(
                final @Nullable String databaseKey,
                final String driver,
                final String dataset,
                final DatabaseConnection target) {
            this.lastCall = "load-dataset";
            this.databaseKey = databaseKey;
            this.driver = driver;
            this.targetConnection = target;
        }

        @Override
        public void upModuleGroup(
                final @Nullable String databaseKey,
                final String driver,
                final String moduleGroup,
                final DatabaseConnection target) {
            this.lastCall = "up-module-group";
            this.databaseKey = databaseKey;
            this.driver = driver;
            this.moduleGroup = moduleGroup;
            this.targetConnection = target;
        }

        @Override
        public void downModuleGroup(
                final @Nullable String databaseKey,
                final String driver,
                final String moduleGroup,
                final DatabaseConnection target) {
            this.lastCall = "down-module-group";
            this.databaseKey = databaseKey;
            this.driver = driver;
            this.moduleGroup = moduleGroup;
            this.targetConnection = target;
        }

        @Override
        public void packageData(final @Nullable String databaseKey, final Path outputFile) {
            this.lastCall = "package-data";
            this.databaseKey = databaseKey;
            this.outputFile = outputFile;
        }

        @Override
        public void dumpFixtures(
                final @Nullable String databaseKey, final String driver, final DatabaseConnection target) {
            this.lastCall = "dump-fixtures";
            this.databaseKey = databaseKey;
            this.driver = driver;
            this.targetConnection = target;
        }
    }
}
