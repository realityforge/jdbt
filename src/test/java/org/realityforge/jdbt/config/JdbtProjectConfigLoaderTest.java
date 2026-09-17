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
        final var config = load("""
            imports:
              default:
                modules: [Core]
            """, "jdbt.yml", repositoryModules);

        final var database = config.database();
        assertThat(database.upDirs()).containsExactly(".", "types", "views", "functions", "stored-procedures", "misc");
        final var importConfig = Objects.requireNonNull(database.imports().get("default"));
        assertThat(importConfig.modules()).containsExactly("Core");
        assertThat(importConfig.dir()).isEqualTo("import");
        assertThat(database.forceDrop()).isFalse();
        assertThat(database.deleteBackupHistory()).isTrue();
        assertThat(database.reindexOnImport()).isTrue();
        assertThat(database.shrinkOnImport()).isFalse();
        assertThat(database.contributionDirs()).isEmpty();
        assertThat(importConfig.preLateImportDirs()).isEmpty();
        assertThat(importConfig.lateImportDir()).isNull();
    }

    @Test
    void loadParsesContributionAndLateImportConfiguration() {
        final var config = load("""
            contributionDirs: [Action/contributions, Admin/contributions]
            imports:
              default:
                preLateImportDirs: [import-hooks/pre-late, import-hooks/reconcile]
                lateImportDir: late-import
                requiredFiles: [import-hooks/post/metadata.sql, Core/late-import/Core.item.sql]
            """, "jdbt.yml", repositoryModules);

        final var database = config.database();
        final var importConfig = Objects.requireNonNull(database.imports().get("default"));
        assertThat(database.contributionDirs()).containsExactly("Action/contributions", "Admin/contributions");
        assertThat(importConfig.preLateImportDirs()).containsExactly("import-hooks/pre-late", "import-hooks/reconcile");
        assertThat(importConfig.lateImportDir()).isEqualTo("late-import");
        assertThat(importConfig.requiredFiles())
                .containsExactly("import-hooks/post/metadata.sql", "Core/late-import/Core.item.sql");
    }

    @Test
    void loadParsesSqlServerRuntimeOptions() {
        final var config = load("""
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
        final var config = load("""
            imports:
              default: {}
            """, "jdbt.yml", repositoryModules);

        final var importConfig =
                Objects.requireNonNull(config.database().imports().get("default"));
        assertThat(importConfig.modules()).containsExactly("Core", "Geo");
        assertThat(importConfig.dir()).isEqualTo("import");
    }

    @Test
    void loadDefaultsMigrationDirectory() {
        final var config = load("{}\n", "jdbt.yml", repositoryModules);

        assertThat(config.database().migrationDir()).isEqualTo("migrations");
    }

    @Test
    void loadAcceptsCustomMigrationDirectory() {
        final var config = load("migrationDir: upgrades\n", "jdbt.yml", repositoryModules);

        assertThat(config.database().migrationDir()).isEqualTo("upgrades");
    }

    @Test
    void loadRejectsRemovedMigrationFlags() {
        assertThatThrownBy(() -> load("migrations: true\n", "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'migrations'");
        assertThatThrownBy(() -> load("migrationsAppliedAtCreate: true\n", "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'migrationsAppliedAtCreate'");
    }

    @Test
    void loadRejectsUnknownDatabaseKey() {
        assertThatThrownBy(() -> load("""
            unsupported: true
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'unsupported'");
    }

    @Test
    void loadRejectsDefaultsTopLevelKey() {
        assertThatThrownBy(() -> load("""
            defaults:
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'defaults'");
    }

    @Test
    void loadRejectsUnknownImportModule() {
        assertThatThrownBy(() -> load("""
            imports:
              default:
                modules: [Missing]
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Module 'Missing'")
                .hasMessageContaining("import 'default'");
    }

    @Test
    void loadAllowsEmptyConfigWithHardcodedDefaults() {
        final var config = load("{}", "jdbt.yml", repositoryModules);
        assertThat(config.resourceRoot()).isEqualTo(".");
    }

    @Test
    void loadParsesSingularResourceRoot() {
        final var config = load("resourceRoot: ../../database\n", "jdbt.yml", repositoryModules);

        assertThat(config.resourceRoot()).isEqualTo("../../database");
    }

    @Test
    void loadRejectsLegacyDatabasesKey() {
        assertThatThrownBy(() -> load("""
            databases:
              default: {}
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'databases'");
    }

    @Test
    void loadRejectsSearchDirsKey() {
        assertThatThrownBy(() -> load("""
            searchDirs: [db]
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'searchDirs'");
    }

    @Test
    void loadRejectsResourcePrefixKey() {
        assertThatThrownBy(() -> load("""
            resourcePrefix: data
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'resourcePrefix'");
    }

    @Test
    void loadParsesFilterPropertiesWithDefaultAndSupportedValues() {
        final var config = load("""
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
        assertThatThrownBy(() -> load("""
            filterProperties:
              sourceDatabase:
                pattern: __SRC_DB__
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("reserved and tool-provided");
    }

    @Test
    void loadRejectsReservedFilterPropertyPatterns() {
        assertThatThrownBy(() -> load("""
            filterProperties:
              mode:
                pattern: __SOURCE__
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("reserved pattern '__SOURCE__'");
    }

    @Test
    void loadRejectsEmptySupportedValuesWhenSpecified() {
        assertThatThrownBy(() -> load("""
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
        assertThatThrownBy(() -> load("""
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
        assertThatThrownBy(() -> load("""
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
        assertThatThrownBy(() -> load("""
            filterProperties:
              mode:
                pattern: "   "
            """, "jdbt.yml", repositoryModules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("must define a non-empty pattern");
    }

    private JdbtProjectConfig load(final String yaml, final String sourceName, final List<String> repositoryModules) {
        return loader.load(loader.parse(yaml, sourceName), repositoryModules);
    }
}
