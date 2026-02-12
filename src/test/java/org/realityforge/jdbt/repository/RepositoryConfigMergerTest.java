package org.realityforge.jdbt.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.config.ConfigException;

final class RepositoryConfigMergerTest {
    private final RepositoryConfigMerger merger = new RepositoryConfigMerger();

    @Test
    void mergePreservesPreLocalPostOrder() {
        final RepositoryConfig pre = repository("Pre", "PreSchema", "[Pre].[tblA]", "[Pre].[seqA]");
        final RepositoryConfig local = repository("Local", "Local", "[Local].[tblB]", "[Local].[seqB]");
        final RepositoryConfig post = repository("Post", "P", "[P].[tblC]", "[P].[seqC]");

        final RepositoryConfig merged = merger.merge(List.of(pre), local, List.of(post));

        assertThat(merged.modules()).containsExactly("Pre", "Local", "Post");
        assertThat(merged.schemaOverrides()).containsEntry("Pre", "PreSchema").containsEntry("Post", "P");
        assertThat(merged.tableMap().get("Local")).containsExactly("[Local].[tblB]");
        assertThat(merged.sequenceMap().get("Post")).containsExactly("[P].[seqC]");
    }

    @Test
    void mergeRejectsDuplicateModulesAcrossSources() {
        final RepositoryConfig pre = repository("Core", "Core", "[Core].[tblA]", "[Core].[seqA]");
        final RepositoryConfig local = repository("Core", "Core", "[Core].[tblB]", "[Core].[seqB]");

        assertThatThrownBy(() -> merger.merge(List.of(pre), local, List.of()))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("duplicate module definition 'Core'");
    }

    @Test
    void mergeSupportsEmptyArtifacts() {
        final RepositoryConfig local = repository("Core", "Core", "[Core].[tblA]", "[Core].[seqA]");

        final RepositoryConfig merged = merger.merge(List.of(), local, List.of());

        assertThat(merged.modules()).containsExactly("Core");
    }

    private RepositoryConfig repository(
            final String module, final String schema, final String table, final String sequence) {
        return new RepositoryConfig(
                List.of(module),
                module.equals(schema) ? Map.of() : Map.of(module, schema),
                Map.of(module, List.of(table)),
                Map.of(module, List.of(sequence)));
    }
}
