package org.realityforge.jdbt.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.config.ConfigException;
import org.realityforge.jdbt.config.ImportConfig;

final class DbDriverFactoryTest {
    @Test
    void createsSqlServerDriver() {
        assertThat(new DbDriverFactory().create("sqlserver")).isInstanceOf(SqlServerDbDriver.class);
        assertThat(new DbDriverFactory().create("sqlserver").supportsImportAssertFilters())
                .isTrue();
        assertThat(new DbDriverFactory().create("SQLSERVER")).isInstanceOf(SqlServerDbDriver.class);
    }

    @Test
    void createsNoOpDriverForInternalTesting() {
        assertThat(new DbDriverFactory().create("noop")).isInstanceOf(NoOpDbDriver.class);
    }

    @Test
    void createsPostgresDriver() {
        assertThat(new DbDriverFactory().create("postgres")).isInstanceOf(PostgresDbDriver.class);
        assertThat(new DbDriverFactory().create("postgres").supportsImportAssertFilters())
                .isFalse();
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

        driver.open(connection, false);
        final var metadata = new DatabaseMetadata("default", "1", "hash");
        driver.drop(metadata, connection);
        driver.createDatabase(metadata, connection);
        driver.createSchema("schema");
        driver.dropSchema("schema", List.of("t"));
        driver.execute("SELECT 1", false);
        driver.preFixtureImport("t");
        driver.insert("t", Map.of("ID", 1));
        driver.postFixtureImport("t");
        driver.updateSequence("seq", 1L);
        driver.preTableImport(
                metadata, new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of()), "t");
        driver.postTableImport(
                metadata, new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of()), "t");
        driver.postDataModuleImport(
                metadata,
                new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of()),
                "MyModule",
                List.of("t"));
        driver.postDatabaseImport(
                metadata, new ImportConfig("default", List.of("MyModule"), "import", List.of(), List.of()));
        assertThat(driver.columnNamesForTable("t")).isEmpty();
        driver.setupMigrations();
        assertThat(driver.shouldMigrate("ns", "m")).isTrue();
        driver.markMigrationAsRun("ns", "m");
        driver.close();
    }
}
