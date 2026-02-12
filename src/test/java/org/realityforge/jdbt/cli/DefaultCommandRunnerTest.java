package org.realityforge.jdbt.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.realityforge.jdbt.db.DatabaseConnection;
import org.realityforge.jdbt.db.DbDriver;
import org.realityforge.jdbt.db.DbDriverFactory;
import org.realityforge.jdbt.db.NoOpDbDriver;
import org.realityforge.jdbt.files.FileResolver;
import org.realityforge.jdbt.runtime.RuntimeExecutionException;

final class DefaultCommandRunnerTest {
    private final DatabaseConnection target = new DatabaseConnection("127.0.0.1", 1433, "DB", "sa", "secret");
    private final DatabaseConnection source = new DatabaseConnection("127.0.0.1", 1433, "SRC", "sa", "secret");

    @Test
    void statusCreateDropMigrateImportAndGroupCommandsExecute(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", projectConfig(true));
        writeFile(tempDir, "repository.yml", repositoryConfig());

        final DefaultCommandRunner runner = createRunner(tempDir);

        final PrintStream originalOut = System.out;
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
            runner.status("default", "sqlserver");
        } finally {
            System.setOut(originalOut);
        }

        runner.create("default", "noop", target, true);
        runner.drop("default", "noop", target);
        runner.migrate("default", "noop", target);
        runner.databaseImport("default", "noop", null, null, target, source, null);
        runner.createByImport("default", "noop", null, target, source, null, true);
        runner.loadDataset("default", "noop", "seed", target);
        runner.upModuleGroup("default", "noop", "all", target);
        runner.downModuleGroup("default", "noop", "all", target);

        assertThat(output.toString(StandardCharsets.UTF_8))
                .contains("Database Version")
                .contains("Migration Support");
    }

    @Test
    void packageDataWritesZipOutput(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", projectConfig(false));
        writeFile(tempDir, "repository.yml", repositoryConfig());
        writeFile(tempDir, "MyModule/a.sql", "SELECT 1");

        final DefaultCommandRunner runner = createRunner(tempDir);
        final Path output = tempDir.resolve("out.zip");
        runner.packageData("default", output);

        assertThat(output).exists();
        assertThat(Files.size(output)).isGreaterThan(0L);
    }

    @Test
    void dumpFixturesIsNotYetImplemented(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", projectConfig(false));
        writeFile(tempDir, "repository.yml", repositoryConfig());
        final DefaultCommandRunner runner = createRunner(tempDir);

        assertThatThrownBy(() -> runner.dumpFixtures("default", "sqlserver", target))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("not yet implemented");
    }

    @Test
    void databaseImportRequiresDefaultImportWhenImportNotProvided(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", projectConfigWithoutImports());
        writeFile(tempDir, "repository.yml", repositoryConfig());
        final DefaultCommandRunner runner = createRunner(tempDir);

        assertThatThrownBy(() -> runner.databaseImport("default", "sqlserver", null, null, target, source, null))
                .isInstanceOf(RuntimeExecutionException.class)
                .hasMessageContaining("Unable to locate import definition by key");
    }

    private String projectConfig(final boolean withMigrations) {
        return """
            databases:
              default:
                datasets: [seed]
                migrations: %s
                imports:
                  default:
                    modules: [MyModule]
                moduleGroups:
                  all:
                    modules: [MyModule]
            """.formatted(withMigrations);
    }

    private String projectConfigWithoutImports() {
        return """
            databases:
              default:
                datasets: [seed]
                imports: {}
                moduleGroups:
                  all:
                    modules: [MyModule]
            """;
    }

    private String repositoryConfig() {
        return """
            modules:
              MyModule:
                tables: ["[MyModule].[foo]"]
                sequences: []
            """;
    }

    private void writeFile(final Path root, final String relativePath, final String content) throws IOException {
        final Path file = root.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private DefaultCommandRunner createRunner(final Path tempDir) {
        return new DefaultCommandRunner(new ProjectRuntimeLoader(tempDir), new TestDriverFactory(), new FileResolver());
    }

    private static final class TestDriverFactory extends DbDriverFactory {
        @Override
        public DbDriver create(final String driver) {
            return new NoOpDbDriver();
        }
    }
}
