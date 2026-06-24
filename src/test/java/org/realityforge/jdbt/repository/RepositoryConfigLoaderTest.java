package org.realityforge.jdbt.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.config.ConfigException;

final class RepositoryConfigLoaderTest {
    private final RepositoryConfigLoader loader = new RepositoryConfigLoader();

    @Test
    void loadSupportsOmapListStyle() {
        final var config = loader.load("""
            modules:
              - Core:
                  schema: Core
                  tables:
                    - '[Core].[tblA]'
                  sequences:
                    - '[Core].[tblASeq]'
              - Geo:
                  schema: G
                  tables:
                    - '[G].[tblB]'
                  sequences: []
            """, "repository.yml");

        assertThat(config.modules()).containsExactly("Core", "Geo");
        assertThat(config.schemaOverrides()).containsEntry("Geo", "G");
        assertThat(config.tableMap().get("Core")).containsExactly("[Core].[tblA]");
        assertThat(config.sequenceMap().get("Core")).containsExactly("[Core].[tblASeq]");
        assertThat(config.sequenceMap().get("Geo")).isEmpty();
    }

    @Test
    void loadSupportsTaggedOmapStyleFromRubyDbt() {
        final var config = loader.load("""
            ---
            modules: !omap
            - CodeMetrics:
                schema: CodeMetrics
                tables:
                - '[CodeMetrics].[tblCollection]'
                - '[CodeMetrics].[tblMethodMetric]'
                sequences:
                - '[CodeMetrics].[tblCollection_IDSeq]'
            - Geo:
                schema: Geo
                tables:
                - '[Geo].[tblMobilePOI]'
                sequences: []
            """, "repository.yml");

        assertThat(config.modules()).containsExactly("CodeMetrics", "Geo");
        assertThat(config.tableMap().get("CodeMetrics"))
                .containsExactly("[CodeMetrics].[tblCollection]", "[CodeMetrics].[tblMethodMetric]");
        assertThat(config.sequenceMap().get("CodeMetrics")).containsExactly("[CodeMetrics].[tblCollection_IDSeq]");
    }

    @Test
    void loadSupportsMapStyle() {
        final var config = loader.load("""
            modules:
              Core:
                schema: Core
                tables: []
                sequences: []
              Billing:
                schema: Billing
                tables:
                  - '[Billing].[tblInvoice]'
            """, "repository.yml");

        assertThat(config.modules()).containsExactly("Core", "Billing");
        assertThat(config.tableMap().get("Billing")).containsExactly("[Billing].[tblInvoice]");
        assertThat(config.sequenceMap().get("Billing")).isEmpty();
    }

    @Test
    void loadRejectsUnknownTopLevelKey() {
        assertThatThrownBy(() -> loader.load("bad: true", "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'bad'");
    }

    @Test
    void loadAllowsMissingModulesKeyAsEmptyRepository() {
        final var config = loader.load("{}", "repository.yml");

        assertThat(config.modules()).isEmpty();
        assertThat(config.tableMap()).isEmpty();
        assertThat(config.sequenceMap()).isEmpty();
    }

    @Test
    void loadRejectsDuplicateModules() {
        assertThatThrownBy(() -> loader.load("""
            modules:
              - Core:
                  schema: Core
                  tables: []
              - Core:
                  schema: Core
                  tables: []
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Duplicate repository module");
    }

    @Test
    void loadRejectsUnknownModuleKey() {
        assertThatThrownBy(() -> loader.load("""
            modules:
              - Core:
                  schema: Core
                  unknown: true
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'unknown'");
    }

    @Test
    void loadRejectsUnexpectedModulesNodeType() {
        assertThatThrownBy(() -> loader.load("modules: true", "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Expected list or map");
    }

    @Test
    void loadRejectsModuleBodyWhenNotMap() {
        assertThatThrownBy(() -> loader.load("""
            modules:
              - Core: true
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Expected map body for module 'Core'");
    }
}
