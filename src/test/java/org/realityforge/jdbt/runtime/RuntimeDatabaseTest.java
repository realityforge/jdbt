package org.realityforge.jdbt.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.config.ModuleGroupConfig;
import org.realityforge.jdbt.files.ArtifactContent;
import org.realityforge.jdbt.repository.RepositoryConfig;

final class RuntimeDatabaseTest {
    @Test
    void delegatesOrderingAndSchemaLookupsToRepository() {
        final var repository = new RepositoryConfig(
                List.of("Core"),
                Map.of("Core", "C"),
                Map.of("Core", List.of("[C].[tblA]")),
                Map.of("Core", List.of("[C].[seqA]")));
        final var database = runtimeDatabase(repository, List.of(), List.of());

        assertThat(database.schemaNameForModule("Core")).isEqualTo("C");
        assertThat(database.tableOrdering("Core")).containsExactly("[C].[tblA]");
        assertThat(database.sequenceOrdering("Core")).containsExactly("[C].[seqA]");
        assertThat(database.orderedElementsForModule("Core")).containsExactly("[C].[tblA]", "[C].[seqA]");
    }

    @Test
    void artifactByIdPrefersPostThenPreAndReturnsNullWhenMissing() {
        final var pre = new StaticArtifact("pre");
        final var post = new StaticArtifact("post");
        final var database = runtimeDatabase(
                new RepositoryConfig(List.of("Core"), Map.of(), Map.of("Core", List.of()), Map.of("Core", List.of())),
                List.of(pre),
                List.of(post));

        assertThat(database.artifactById("post")).isEqualTo(post);
        assertThat(database.artifactById("pre")).isEqualTo(pre);
        assertThat(database.artifactById("missing")).isNull();
    }

    private static RuntimeDatabase runtimeDatabase(
            final RepositoryConfig repository,
            final List<ArtifactContent> preArtifacts,
            final List<ArtifactContent> postArtifacts) {
        return new RuntimeDatabase(
                "default",
                repository,
                List.of(Path.of(".")),
                preArtifacts,
                postArtifacts,
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
                Map.of("default", new ImportConfig("default", repository.modules(), "import", List.of(), List.of())),
                Map.of("grp", new ModuleGroupConfig("grp", repository.modules(), false)));
    }

    private record StaticArtifact(String id) implements ArtifactContent {
        @Override
        public List<String> files() {
            return List.of();
        }

        @Override
        public String readText(final String path) {
            throw new UnsupportedOperationException();
        }
    }
}
