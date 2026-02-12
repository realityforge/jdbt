package org.realityforge.jdbt.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
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
        final var factory = new RuntimeDatabaseFactory();
        final var repository = new RepositoryConfig(
                List.of("Core"), Map.of(), Map.of("Core", List.of("[Core].[tblA]")), Map.of("Core", List.of()));
        final var database = new DatabaseConfig(
                "default",
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

        final var runtimeDatabase = factory.from(
                database,
                DefaultsConfig.rubyCompatibleDefaults(),
                repository,
                List.of(),
                List.of(),
                "hash",
                Path.of("dbRoot"));

        assertThat(runtimeDatabase.key()).isEqualTo("default");
        assertThat(runtimeDatabase.searchDirs()).containsExactly(Path.of("dbRoot"));
        assertThat(runtimeDatabase.indexFileName()).isEqualTo("index.txt");
        assertThat(runtimeDatabase.schemaHash()).isEqualTo("hash");
        assertThat(runtimeDatabase.migrationsAppliedAtCreate()).isTrue();
        assertThat(runtimeDatabase.migrationsDirName()).isEqualTo("migrations");
        assertThat(runtimeDatabase.imports()).containsKey("default");
        assertThat(runtimeDatabase.moduleGroups()).containsKey("g");
    }
}
