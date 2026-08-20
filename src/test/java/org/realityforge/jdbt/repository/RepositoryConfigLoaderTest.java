package org.realityforge.jdbt.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
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
                    - name: '[Core].[tblA]'
                      columns:
                        - '[ID]'
                      indexes:
                        - '[PK_A]'
                  sequences:
                    - '[Core].[tblASeq]'
              - Geo:
                  schema: G
                  tables:
                    - name: '[G].[tblB]'
                      columns:
                        - '[ID]'
                      indexes: []
                      rowSource: deployment
                  sequences: []
            """, "repository.yml");

        assertThat(config.modules()).containsExactly("Core", "Geo");
        assertThat(config.schemaOverrides()).containsEntry("Geo", "G");
        assertThat(config.tablesForModule("Core"))
                .containsExactly(new RepositoryTable("[Core].[tblA]", List.of("[ID]"), List.of("[PK_A]")));
        assertThat(config.tablesForModule("Geo"))
                .containsExactly(new RepositoryTable("[G].[tblB]", List.of("[ID]"), RowSource.DEPLOYMENT));
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
                - name: '[CodeMetrics].[tblCollection]'
                  columns:
                  - '[ID]'
                  indexes: []
                - name: '[CodeMetrics].[tblMethodMetric]'
                  columns:
                  - '[ID]'
                  indexes: []
                sequences:
                - '[CodeMetrics].[tblCollection_IDSeq]'
            - Geo:
                schema: Geo
                tables:
                - name: '[Geo].[tblMobilePOI]'
                  columns:
                  - '[ID]'
                  indexes: []
                sequences: []
            """, "repository.yml");

        assertThat(config.modules()).containsExactly("CodeMetrics", "Geo");
        assertThat(config.tableOrdering("CodeMetrics"))
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
                  - name: '[Billing].[tblInvoice]'
                    columns:
                      - '[ID]'
                    indexes: []
            """, "repository.yml");

        assertThat(config.modules()).containsExactly("Core", "Billing");
        assertThat(config.tableOrdering("Billing")).containsExactly("[Billing].[tblInvoice]");
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

    @Test
    void loadRejectsScalarTableEntry() {
        assertThatThrownBy(() -> loader.load("""
            modules:
              Core:
                tables:
                  - '[Core].[tblA]'
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Expected table map at repository.yml.modules.Core.tables[0]");
    }

    @Test
    void loadRejectsInvalidTableObjects() {
        assertThatThrownBy(() -> loader.load("""
            modules:
              Core:
                tables:
                  - name: ''
                    columns: ['[ID]']
                    indexes: []
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("name must not be blank");

        assertThatThrownBy(() -> loader.load("""
            modules:
              Core:
                tables:
                  - name: 'tblA'
                    columns: ['[ID]']
                    indexes: []
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("must be a qualified SQL name");

        assertThatThrownBy(() -> loader.load("""
            modules:
              Core:
                tables:
                  - name: '[Core].[tblA]'
                    columns: ['ID']
                    indexes: []
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("must be a quoted SQL identifier");

        assertThatThrownBy(() -> loader.load("""
            modules:
              Core:
                tables:
                  - name: '[Core].[tblA]'
                    columns: []
                    indexes: []
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("columns must not be empty");

        assertThatThrownBy(() -> loader.load("""
            modules:
              Core:
                tables:
                  - name: '[Core].[tblA]'
                    columns: ['[ID]', '[ID]']
                    indexes: []
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("duplicate column '[ID]'");

        assertThatThrownBy(() -> loader.load("""
            modules:
              Core:
                tables:
                  - name: '[Core].[tblA]'
                    columns: [1]
                    indexes: []
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Expected string list entry");

        assertThatThrownBy(() -> loader.load("""
            modules:
              Core:
                tables:
                  - name: '[Core].[tblA]'
                    columns: ['[ID]']
                    indexes: []
                    rowSource: external
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Invalid Row Source 'external'");

        assertThatThrownBy(() -> loader.load("""
            modules:
              Core:
                tables:
                  - name: '[Core].[tblA]'
                    columns: ['[ID]']
                    indexes: []
                    unknown: true
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'unknown'");

        assertThatThrownBy(() -> loader.load("""
            modules:
              Core:
                tables:
                  - name: '[Core].[tblA]'
                    columns: ['[ID]']
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("indexes");

        assertThatThrownBy(() -> loader.load("""
            modules:
              Core:
                tables:
                  - name: '[Core].[tblA]'
                    columns: ['[ID]']
                    indexes: ['IX_A']
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("index 'IX_A' must be a quoted SQL identifier");

        assertThatThrownBy(() -> loader.load("""
            modules:
              Core:
                tables:
                  - name: '[Core].[tblA]'
                    columns: ['[ID]']
                    indexes: ['[IX_A]', '[IX_A]']
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("duplicate index '[IX_A]'");

        assertThatThrownBy(() -> loader.load("""
            modules:
              Core:
                tables:
                  - name: '[Core].[tblA]'
                    columns: ['[ID]']
                    indexes: ['[IX_A]', '"IX_A"']
            """, "repository.yml"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("duplicate index '\"IX_A\"'");
    }
}
