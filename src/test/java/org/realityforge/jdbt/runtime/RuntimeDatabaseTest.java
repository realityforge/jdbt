package org.realityforge.jdbt.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.repository.RepositoryConfig;
import org.realityforge.jdbt.repository.RepositoryTable;
import org.realityforge.jdbt.repository.RowSource;

final class RuntimeDatabaseTest {
    @Test
    void delegatesOrderingAndSchemaLookupsToRepository() {
        final var repository = new RepositoryConfig(
                List.of("Core"),
                Map.of("Core", "C"),
                Map.of(
                        "Core",
                        List.of(new RepositoryTable("[C].[tblA]", List.of("[ID]"), List.of(), RowSource.IMPORT))),
                Map.of("Core", List.of("[C].[seqA]")));
        final var database = runtimeDatabase(repository);

        assertThat(database.schemaNameForModule("Core")).isEqualTo("C");
        assertThat(database.tableOrdering("Core")).containsExactly("[C].[tblA]");
        assertThat(database.tablesForModule("Core"))
                .containsExactly(new RepositoryTable("[C].[tblA]", List.of("[ID]"), List.of(), RowSource.IMPORT));
        assertThat(database.sequenceOrdering("Core")).containsExactly("[C].[seqA]");
        assertThat(database.orderedElementsForModule("Core")).containsExactly("[C].[tblA]", "[C].[seqA]");
        assertThat(database.filterProperties()).isEmpty();
    }

    private static RuntimeDatabase runtimeDatabase(final RepositoryConfig repository) {
        return new RuntimeDatabase(
                repository,
                Path.of("."),
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
    }
}
