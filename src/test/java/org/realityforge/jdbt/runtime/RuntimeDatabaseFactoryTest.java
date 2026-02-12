package org.realityforge.jdbt.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.config.DatabaseConfig;
import org.realityforge.jdbt.config.DefaultsConfig;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.config.ModuleGroupConfig;
import org.realityforge.jdbt.repository.RepositoryConfig;

final class RuntimeDatabaseFactoryTest {
    @Test
    void fromCreatesRuntimeDatabaseWithExpectedFields() {
        final RuntimeDatabaseFactory factory = new RuntimeDatabaseFactory();
        final RepositoryConfig repository = new RepositoryConfig(
                List.of("Core"), Map.of(), Map.of("Core", List.of("[Core].[tblA]")), Map.of("Core", List.of()));
        final DatabaseConfig database = new DatabaseConfig(
                "default",
                List.of("db"),
                List.of("."),
                List.of("down"),
                List.of("finalize"),
                List.of("pre"),
                List.of("post"),
                List.of("seed"),
                "datasets",
                List.of("pre"),
                List.of("post"),
                "fixtures",
                true,
                true,
                "migrations",
                "v1",
                null,
                List.of(),
                List.of(),
                Map.of("default", new ImportConfig("default", List.of("Core"), "import", List.of(), List.of())),
                Map.of("g", new ModuleGroupConfig("g", List.of("Core"), false)));

        final RuntimeDatabase runtimeDatabase = factory.from(
                database, DefaultsConfig.rubyCompatibleDefaults(), repository, List.of(), List.of(), "hash");

        assertThat(runtimeDatabase.key()).isEqualTo("default");
        assertThat(runtimeDatabase.searchDirs()).containsExactly(java.nio.file.Path.of("db"));
        assertThat(runtimeDatabase.indexFileName()).isEqualTo("index.txt");
        assertThat(runtimeDatabase.schemaHash()).isEqualTo("hash");
        assertThat(runtimeDatabase.migrationsAppliedAtCreate()).isTrue();
        assertThat(runtimeDatabase.migrationsDirName()).isEqualTo("migrations");
        assertThat(runtimeDatabase.imports()).containsKey("default");
        assertThat(runtimeDatabase.moduleGroups()).containsKey("g");
    }
}
