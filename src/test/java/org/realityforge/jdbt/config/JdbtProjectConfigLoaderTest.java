package org.realityforge.jdbt.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.realityforge.jdbt.repository.RepositoryConfig;

final class JdbtProjectConfigLoaderTest {
    private final JdbtProjectConfigLoader loader = new JdbtProjectConfigLoader();
    private final RepositoryConfig repository = new RepositoryConfig(
            List.of("Core", "Geo"),
            Map.of(),
            Map.of("Core", List.of("[Core].[tblA]"), "Geo", List.of("[Geo].[tblB]")),
            Map.of("Core", List.of(), "Geo", List.of()));

    @Test
    void loadAppliesHardcodedDefaultsAndBuildsDatabaseConfig() {
        final var config = loader.load("""
            imports:
              default:
                modules: [Core]
            moduleGroups:
              reporting:
                modules: [Geo]
                importEnabled: true
            """, "jdbt.yml", repository);

        final var database = config.database();
        assertThat(database.upDirs()).containsExactly(".", "types", "views", "functions", "stored-procedures", "misc");
        assertThat(database.imports().get("default").modules()).containsExactly("Core");
        assertThat(database.imports().get("default").dir()).isEqualTo("import");
        assertThat(database.moduleGroups().get("reporting").modules()).containsExactly("Geo");
        assertThat(database.moduleGroups().get("reporting").importEnabled()).isTrue();
    }

    @Test
    void loadUsesRepositoryModulesWhenImportModulesMissing() {
        final var config = loader.load("""
            imports:
              default: {}
            """, "jdbt.yml", repository);

        final var importConfig = config.database().imports().get("default");
        assertThat(importConfig.modules()).containsExactly("Core", "Geo");
        assertThat(importConfig.dir()).isEqualTo("import");
    }

    @Test
    void loadDefaultsMigrationsAppliedAtCreateToMigrationsValue() {
        final var config = loader.load("""
            migrations: true
            """, "jdbt.yml", repository);

        assertThat(config.database().migrations()).isTrue();
        assertThat(config.database().migrationsAppliedAtCreate()).isTrue();
    }

    @Test
    void loadRejectsUnknownDatabaseKey() {
        assertThatThrownBy(() -> loader.load("""
                    unsupported: true
                    """, "jdbt.yml", repository))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'unsupported'");
    }

    @Test
    void loadRejectsDefaultsTopLevelKey() {
        assertThatThrownBy(() -> loader.load("""
                    defaults:
                    """, "jdbt.yml", repository))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'defaults'");
    }

    @Test
    void loadRejectsUnknownImportModule() {
        assertThatThrownBy(() -> loader.load("""
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
                    moduleGroups:
                      reporting: {}
                    """, "jdbt.yml", repository))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Missing required list key 'modules'");
    }

    @Test
    void loadRejectsModuleGroupWithUnknownModule() {
        assertThatThrownBy(() -> loader.load("""
                    moduleGroups:
                      reporting:
                        modules: [Missing]
                    """, "jdbt.yml", repository))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Module 'Missing'")
                .hasMessageContaining("module group 'reporting'");
    }

    @Test
    void loadAllowsEmptyConfigWithHardcodedDefaults() {
        final var config = loader.load("{}", "jdbt.yml", repository);
        assertThat(config.database().key()).isEqualTo("default");
    }

    @Test
    void loadRejectsLegacyDatabasesKey() {
        assertThatThrownBy(() -> loader.load("""
                    databases:
                      default: {}
                    """, "jdbt.yml", repository))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'databases'");
    }

    @Test
    void loadRejectsSearchDirsKey() {
        assertThatThrownBy(() -> loader.load("""
                    searchDirs: [db]
                    """, "jdbt.yml", repository))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'searchDirs'");
    }
}
