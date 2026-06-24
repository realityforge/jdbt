package org.realityforge.jdbt.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;

final class JdbtProjectConfigLoaderTest {
    private final JdbtProjectConfigLoader loader = new JdbtProjectConfigLoader();
    private final List<String> repositoryModules = List.of("Core", "Geo");

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
            """, "jdbt.yml", repositoryModules);

        final var database = config.database();
        assertThat(database.upDirs()).containsExactly(".", "types", "views", "functions", "stored-procedures", "misc");
        final var importConfig = Objects.requireNonNull(database.imports().get("default"));
        final var moduleGroup = Objects.requireNonNull(database.moduleGroups().get("reporting"));
        assertThat(importConfig.modules()).containsExactly("Core");
        assertThat(importConfig.dir()).isEqualTo("import");
        assertThat(moduleGroup.modules()).containsExactly("Geo");
        assertThat(moduleGroup.importEnabled()).isTrue();
        assertThat(database.forceDrop()).isFalse();
        assertThat(database.deleteBackupHistory()).isTrue();
        assertThat(database.reindexOnImport()).isTrue();
        assertThat(database.shrinkOnImport()).isFalse();
    }

    @Test
    void loadParsesSqlServerRuntimeOptions() {
        final var config = loader.load("""
            dataPath: C:\\data
            logPath: C:\\log
            forceDrop: true
            deleteBackupHistory: false
            reindexOnImport: false
            shrinkOnImport: true
            """, "jdbt.yml", repositoryModules);

        final var database = config.database();
        assertThat(database.dataPath()).isEqualTo("C:\\data");
        assertThat(database.logPath()).isEqualTo("C:\\log");
        assertThat(database.forceDrop()).isTrue();
        assertThat(database.deleteBackupHistory()).isFalse();
        assertThat(database.reindexOnImport()).isFalse();
        assertThat(database.shrinkOnImport()).isTrue();
    }

    @Test
    void loadUsesRepositoryModulesWhenImportModulesMissing() {
        final var config = loader.load("""
            imports:
              default: {}
            """, "jdbt.yml", repositoryModules);

        final var importConfig =
                Objects.requireNonNull(config.database().imports().get("default"));
        assertThat(importConfig.modules()).containsExactly("Core", "Geo");
        assertThat(importConfig.dir()).isEqualTo("import");
    }

    @Test
    void loadDefaultsMigrationsAppliedAtCreateToMigrationsValue() {
        final var config = loader.load("""
            migrations: true
            """, "jdbt.yml", repositoryModules);

        assertThat(config.database().migrations()).isTrue();
        assertThat(config.database().migrationsAppliedAtCreate()).isTrue();
    }

    @Test
    void loadRejectsUnknownDatabaseKey() {
        assertThatThrownBy(() -> loader.load("""
            unsupported: true
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'unsupported'");
    }

    @Test
    void loadRejectsDefaultsTopLevelKey() {
        assertThatThrownBy(() -> loader.load("""
            defaults:
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'defaults'");
    }

    @Test
    void loadRejectsUnknownImportModule() {
        assertThatThrownBy(() -> loader.load("""
            imports:
              default:
                modules: [Missing]
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Module 'Missing'")
                .hasMessageContaining("import 'default'");
    }

    @Test
    void loadRejectsModuleGroupWithoutModules() {
        assertThatThrownBy(() -> loader.load("""
            moduleGroups:
              reporting: {}
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Missing required list key 'modules'");
    }

    @Test
    void loadRejectsModuleGroupWithUnknownModule() {
        assertThatThrownBy(() -> loader.load("""
            moduleGroups:
              reporting:
                modules: [Missing]
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Module 'Missing'")
                .hasMessageContaining("module group 'reporting'");
    }

    @Test
    void loadAllowsEmptyConfigWithHardcodedDefaults() {
        final var config = loader.load("{}", "jdbt.yml", repositoryModules);
        assertThat(config.database().key()).isEqualTo("default");
    }

    @Test
    void loadRejectsLegacyDatabasesKey() {
        assertThatThrownBy(() -> loader.load("""
            databases:
              default: {}
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'databases'");
    }

    @Test
    void loadRejectsSearchDirsKey() {
        assertThatThrownBy(() -> loader.load("""
            searchDirs: [db]
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'searchDirs'");
    }

    @Test
    void loadRejectsResourcePrefixKey() {
        assertThatThrownBy(() -> loader.load("""
            resourcePrefix: data
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'resourcePrefix'");
    }

    @Test
    void loadParsesFilterPropertiesWithDefaultAndSupportedValues() {
        final var config = loader.load("""
            filterProperties:
              mode:
                pattern: __MODE__
                default: bulk
                supportedValues: [bulk, delta]
              tenant:
                pattern: __TENANT__
            """, "jdbt.yml", repositoryModules);

        assertThat(config.database().filterProperties().keySet()).containsExactly("mode", "tenant");
        assertThat(config.database().filterProperties().get("mode"))
                .isEqualTo(new FilterPropertyConfig("__MODE__", "bulk", List.of("bulk", "delta")));
        assertThat(config.database().filterProperties().get("tenant"))
                .isEqualTo(new FilterPropertyConfig("__TENANT__", null, List.of()));
    }

    @Test
    void loadRejectsReservedFilterPropertyKeys() {
        assertThatThrownBy(() -> loader.load("""
            filterProperties:
              sourceDatabase:
                pattern: __SRC_DB__
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("reserved and tool-provided");
    }

    @Test
    void loadRejectsReservedFilterPropertyPatterns() {
        assertThatThrownBy(() -> loader.load("""
            filterProperties:
              mode:
                pattern: __SOURCE__
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("reserved pattern '__SOURCE__'");
    }

    @Test
    void loadRejectsEmptySupportedValuesWhenSpecified() {
        assertThatThrownBy(() -> loader.load("""
            filterProperties:
              mode:
                pattern: __MODE__
                supportedValues: []
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("non-empty 'supportedValues'");
    }

    @Test
    void loadRejectsDefaultOutsideSupportedValues() {
        assertThatThrownBy(() -> loader.load("""
            filterProperties:
              mode:
                pattern: __MODE__
                default: bulk
                supportedValues: [delta]
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("declares default 'bulk' not present in supportedValues");
    }

    @Test
    void loadRejectsDuplicateFilterPatterns() {
        assertThatThrownBy(() -> loader.load("""
            filterProperties:
              first:
                pattern: __MODE__
              second:
                pattern: __MODE__
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Duplicate filter pattern '__MODE__'");
    }

    @Test
    void loadRejectsBlankFilterPattern() {
        assertThatThrownBy(() -> loader.load("""
            filterProperties:
              mode:
                pattern: "   "
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("must define a non-empty pattern");
    }
}
