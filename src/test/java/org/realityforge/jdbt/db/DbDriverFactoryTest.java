package org.realityforge.jdbt.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.config.ConfigException;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.repository.RepositoryConfig;
import org.realityforge.jdbt.runtime.RuntimeDatabase;

final class DbDriverFactoryTest {
    @Test
    void createsSqlServerDriver() {
        assertThat(new DbDriverFactory().create("sqlserver")).isInstanceOf(SqlServerDbDriver.class);
        assertThat(new DbDriverFactory().create("SQLSERVER")).isInstanceOf(SqlServerDbDriver.class);
    }

    @Test
    void createsNoOpDriverForInternalTesting() {
        assertThat(new DbDriverFactory().create("noop")).isInstanceOf(NoOpDbDriver.class);
    }

    @Test
    void createsPostgresDriver() {
        assertThat(new DbDriverFactory().create("postgres")).isInstanceOf(PostgresDbDriver.class);
        assertThat(new DbDriverFactory().create("POSTGRES")).isInstanceOf(PostgresDbDriver.class);
    }

    @Test
    void rejectsUnsupportedDriver() {
        assertThatThrownBy(() -> new DbDriverFactory().create("oracle"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unsupported database driver");
    }

    @Test
    void noOpDriverMethodsAreSafe() {
        final var driver = new NoOpDbDriver();
        final var connection = new DatabaseConnection("127.0.0.1", 1433, "db", "sa", "secret");
        final var database = new RuntimeDatabase(
                "default",
                new RepositoryConfig(
                        List.of("MyModule"),
                        Map.of(),
                        Map.of("MyModule", List.of("[MyModule].[foo]")),
                        Map.of("MyModule", List.of())),
                List.of(Path.of(".")),
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
                List.of("seed"),
                true,
                false,
                "migrations",
                "1",
                "hash",
                Map.of("default", new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of())),
                Map.of());

        driver.open(connection, false);
        driver.drop(database, connection);
        driver.createDatabase(database, connection);
        driver.createSchema("schema");
        driver.dropSchema("schema", List.of("t"));
        driver.execute("SELECT 1", false);
        driver.preFixtureImport("t");
        driver.insert("t", Map.of("ID", 1));
        driver.postFixtureImport("t");
        driver.updateSequence("seq", 1L);
        driver.preTableImport(new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of()), "t");
        driver.postTableImport(new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of()), "t");
        driver.postDataModuleImport(
                new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of()), "MyModule");
        driver.postDatabaseImport(new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of()));
        assertThat(driver.columnNamesForTable("t")).isEmpty();
        driver.setupMigrations();
        assertThat(driver.shouldMigrate("ns", "m")).isTrue();
        driver.markMigrationAsRun("ns", "m");
        driver.close();
    }
}
