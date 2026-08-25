package org.realityforge.jdbt.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.config.DatabaseConfig;
import org.realityforge.jdbt.config.FilterPropertyConfig;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.repository.RepositoryConfig;
import org.realityforge.jdbt.repository.RepositoryTable;
import org.realityforge.jdbt.repository.RowSource;

final class RuntimeDatabaseFactoryTest {
    @Test
    void fromCreatesRuntimeDatabaseWithExpectedFields() {
        final var factory = new RuntimeDatabaseFactory();
        final var repository = new RepositoryConfig(
                List.of("Core"),
                Map.of(),
                Map.of(
                        "Core",
                        List.of(new RepositoryTable("[Core].[tblA]", List.of("[ID]"), List.of(), RowSource.IMPORT))),
                Map.of("Core", List.of()));
        final var database = new DatabaseConfig(
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
                "migrations",
                "v1",
                null,
                null,
                false,
                true,
                true,
                false,
                Map.of("mode", new FilterPropertyConfig("__MODE__", "bulk", List.of("bulk", "delta"))),
                Map.of("default", new ImportConfig("default", List.of("Core"), "import", List.of(), List.of())));

        final var runtimeDatabase = factory.from(database, repository, List.of(), List.of(), "hash", Path.of("dbRoot"));

        assertThat(runtimeDatabase.resourceRoot()).isEqualTo(Path.of("dbRoot").toAbsolutePath());
        assertThat(runtimeDatabase.indexFileName()).isEqualTo("index.txt");
        assertThat(runtimeDatabase.schemaHash()).isEqualTo("hash");
        assertThat(runtimeDatabase.migrationDir()).isEqualTo("migrations");
        assertThat(runtimeDatabase.filterProperties()).containsKey("mode");
        assertThat(runtimeDatabase.imports()).containsKey("default");
    }
}
