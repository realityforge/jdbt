package org.realityforge.jdbt.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.repository.RepositoryConfig;

final class JdbtProjectConfigLoaderTest {
    private final JdbtProjectConfigLoader loader = new JdbtProjectConfigLoader();
    private final RepositoryConfig repository = new RepositoryConfig(
            java.util.List.of("Core", "Geo"),
            java.util.Map.of(),
            java.util.Map.of("Core", java.util.List.of("[Core].[tblA]"), "Geo", java.util.List.of("[Geo].[tblB]")),
            java.util.Map.of("Core", java.util.List.of(), "Geo", java.util.List.of()));

    @Test
    void loadAppliesDefaultsAndBuildsDatabaseConfig() {
        final JdbtProjectConfig config = loader.load("""
            defaults:
              searchDirs:
                - databases/generated
            databases:
              default:
                imports:
                  default:
                    modules: [Core]
                moduleGroups:
                  reporting:
                    modules: [Geo]
                    importEnabled: true
            """, "jdbt.yml", repository);

        final DatabaseConfig database = config.databases().get("default");
        assertThat(database.searchDirs()).containsExactly("databases/generated");
        assertThat(database.upDirs()).containsExactly(".", "types", "views", "functions", "stored-procedures", "misc");
        assertThat(database.imports().get("default").modules()).containsExactly("Core");
        assertThat(database.imports().get("default").dir()).isEqualTo("import");
        assertThat(database.moduleGroups().get("reporting").modules()).containsExactly("Geo");
        assertThat(database.moduleGroups().get("reporting").importEnabled()).isTrue();
    }

    @Test
    void loadUsesRepositoryModulesWhenImportModulesMissing() {
        final JdbtProjectConfig config = loader.load("""
            defaults:
              searchDirs: [db]
              importDir: import-alt
            databases:
              default:
                imports:
                  default: {}
            """, "jdbt.yml", repository);

        final ImportConfig importConfig =
                config.databases().get("default").imports().get("default");
        assertThat(importConfig.modules()).containsExactly("Core", "Geo");
        assertThat(importConfig.dir()).isEqualTo("import-alt");
    }

    @Test
    void loadDefaultsMigrationsAppliedAtCreateToMigrationsValue() {
        final JdbtProjectConfig config = loader.load("""
            defaults:
              searchDirs: [db]
            databases:
              default:
                migrations: true
            """, "jdbt.yml", repository);

        assertThat(config.databases().get("default").migrations()).isTrue();
        assertThat(config.databases().get("default").migrationsAppliedAtCreate())
                .isTrue();
    }

    @Test
    void loadRejectsUnknownDatabaseKey() {
        assertThatThrownBy(() -> loader.load("""
                    defaults:
                      searchDirs: [db]
                    databases:
                      default:
                        unsupported: true
                    """, "jdbt.yml", repository))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'unsupported'");
    }

    @Test
    void loadRejectsUnknownDefaultsKey() {
        assertThatThrownBy(() -> loader.load("""
                    defaults:
                      bad: true
                    databases:
                      default:
                        searchDirs: [db]
                    """, "jdbt.yml", repository))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'bad'")
                .hasMessageContaining("defaults");
    }

    @Test
    void loadRejectsUnknownImportModule() {
        assertThatThrownBy(() -> loader.load("""
                    defaults:
                      searchDirs: [db]
                    databases:
                      default:
                        imports:
                          default:
                            modules: [Missing]
                    """, "jdbt.yml", repository))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Module 'Missing'")
                .hasMessageContaining("import 'default'");
    }

    @Test
    void loadRejectsModuleGroupWithoutModules() {
        assertThatThrownBy(() -> loader.load("""
                    defaults:
                      searchDirs: [db]
                    databases:
                      default:
                        moduleGroups:
                          reporting: {}
                    """, "jdbt.yml", repository))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Missing required list key 'modules'");
    }

    @Test
    void loadRejectsModuleGroupWithUnknownModule() {
        assertThatThrownBy(() -> loader.load("""
                    defaults:
                      searchDirs: [db]
                    databases:
                      default:
                        moduleGroups:
                          reporting:
                            modules: [Missing]
                    """, "jdbt.yml", repository))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Module 'Missing'")
                .hasMessageContaining("module group 'reporting'");
    }

    @Test
    void loadRejectsMissingDatabaseMap() {
        assertThatThrownBy(() -> loader.load("defaults: {}", "jdbt.yml", repository))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Missing required map key 'databases'");
    }

    @Test
    void loadRejectsDatabaseWhenNodeIsNotMap() {
        assertThatThrownBy(() -> loader.load("""
                    databases:
                      default: true
                    """, "jdbt.yml", repository))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Expected map for database 'default'");
    }

    @Test
    void loadRequiresSearchDirsFromDefaultsOrDatabase() {
        assertThatThrownBy(() -> loader.load("""
                    databases:
                      default: {}
                    """, "jdbt.yml", repository))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("searchDirs");
    }
}
