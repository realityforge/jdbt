package org.realityforge.jdbt.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.config.ConfigException;

final class RepositoryConfigTest {
    private final RepositoryConfig config = new RepositoryConfig(
            List.of("Core", "Geo"),
            Map.of("Geo", "G"),
            Map.of("Core", List.of("[Core].[tblA]"), "Geo", List.of("[G].[tblB]")),
            Map.of("Core", List.of("[Core].[seqA]"), "Geo", List.of()));

    @Test
    void schemaNameForModuleUsesOverrideWhenPresent() {
        assertThat(config.schemaNameForModule("Core")).isEqualTo("Core");
        assertThat(config.schemaNameForModule("Geo")).isEqualTo("G");
    }

    @Test
    void schemaNameForModuleRejectsUnknownModule() {
        assertThatThrownBy(() -> config.schemaNameForModule("Missing"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("non existent module");
    }

    @Test
    void orderingMethodsReturnConfiguredValues() {
        assertThat(config.tableOrdering("Core")).containsExactly("[Core].[tblA]");
        assertThat(config.sequenceOrdering("Core")).containsExactly("[Core].[seqA]");
        assertThat(config.orderedElementsForModule("Core")).containsExactly("[Core].[tblA]", "[Core].[seqA]");
    }

    @Test
    void orderingMethodsRejectUnknownModule() {
        assertThatThrownBy(() -> config.tableOrdering("Missing"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("No tables defined");
        assertThatThrownBy(() -> config.sequenceOrdering("Missing"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("No sequences defined");
    }
}
