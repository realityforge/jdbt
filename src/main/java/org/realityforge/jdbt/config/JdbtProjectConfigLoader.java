package org.realityforge.jdbt.config;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.realityforge.jdbt.repository.RepositoryConfig;

public final class JdbtProjectConfigLoader {
    public JdbtProjectConfig load(final String yaml, final String sourceName, final RepositoryConfig repository) {
        final Map<String, Object> root = YamlMapSupport.parseRoot(yaml, sourceName);
        YamlMapSupport.assertKeys(
                root,
                Set.of(
                        "upDirs",
                        "downDirs",
                        "finalizeDirs",
                        "preCreateDirs",
                        "postCreateDirs",
                        "datasets",
                        "datasetsDirName",
                        "preDatasetDirs",
                        "postDatasetDirs",
                        "fixtureDirName",
                        "migrations",
                        "migrationsAppliedAtCreate",
                        "migrationsDirName",
                        "version",
                        "resourcePrefix",
                        "preDbArtifacts",
                        "postDbArtifacts",
                        "imports",
                        "moduleGroups"),
                sourceName);

        final DefaultsConfig defaults = DefaultsConfig.rubyCompatibleDefaults();
        final DatabaseConfig database =
                loadDatabase(defaults.defaultDatabase(), root, defaults, repository, sourceName);
        return new JdbtProjectConfig(defaults, database);
    }

    private static DatabaseConfig loadDatabase(
            final String key,
            final Map<String, Object> body,
            final DefaultsConfig defaults,
            final RepositoryConfig repository,
            final String sourceName) {
        final String path = sourceName;
        YamlMapSupport.assertKeys(
                body,
                Set.of(
                        "upDirs",
                        "downDirs",
                        "finalizeDirs",
                        "preCreateDirs",
                        "postCreateDirs",
                        "datasets",
                        "datasetsDirName",
                        "preDatasetDirs",
                        "postDatasetDirs",
                        "fixtureDirName",
                        "migrations",
                        "migrationsAppliedAtCreate",
                        "migrationsDirName",
                        "version",
                        "resourcePrefix",
                        "preDbArtifacts",
                        "postDbArtifacts",
                        "imports",
                        "moduleGroups"),
                path);

        final Boolean migrationsValue = YamlMapSupport.optionalBoolean(body, "migrations", path);
        final boolean migrations = migrationsValue != null && migrationsValue;
        final Boolean migrationsAppliedAtCreate =
                YamlMapSupport.optionalBoolean(body, "migrationsAppliedAtCreate", path);

        final Map<String, ImportConfig> imports = loadImports(key, body, defaults, repository, path);
        final Map<String, ModuleGroupConfig> moduleGroups = loadModuleGroups(key, body, repository, path);

        return new DatabaseConfig(
                key,
                YamlMapSupport.optionalStringList(body, "upDirs", path, defaults.upDirs()),
                YamlMapSupport.optionalStringList(body, "downDirs", path, defaults.downDirs()),
                YamlMapSupport.optionalStringList(body, "finalizeDirs", path, defaults.finalizeDirs()),
                YamlMapSupport.optionalStringList(body, "preCreateDirs", path, defaults.preCreateDirs()),
                YamlMapSupport.optionalStringList(body, "postCreateDirs", path, defaults.postCreateDirs()),
                YamlMapSupport.optionalStringList(body, "datasets", path, List.of()),
                YamlMapSupport.optionalString(body, "datasetsDirName", path) == null
                        ? defaults.datasetsDirName()
                        : YamlMapSupport.requireString(body, "datasetsDirName", path),
                YamlMapSupport.optionalStringList(body, "preDatasetDirs", path, defaults.preDatasetDirs()),
                YamlMapSupport.optionalStringList(body, "postDatasetDirs", path, defaults.postDatasetDirs()),
                YamlMapSupport.optionalString(body, "fixtureDirName", path) == null
                        ? defaults.fixtureDirName()
                        : YamlMapSupport.requireString(body, "fixtureDirName", path),
                migrations,
                migrationsAppliedAtCreate == null ? migrations : migrationsAppliedAtCreate,
                YamlMapSupport.optionalString(body, "migrationsDirName", path) == null
                        ? defaults.migrationsDirName()
                        : YamlMapSupport.requireString(body, "migrationsDirName", path),
                YamlMapSupport.optionalString(body, "version", path),
                YamlMapSupport.optionalString(body, "resourcePrefix", path),
                YamlMapSupport.optionalStringList(body, "preDbArtifacts", path, List.of()),
                YamlMapSupport.optionalStringList(body, "postDbArtifacts", path, List.of()),
                imports,
                moduleGroups);
    }

    private static Map<String, ImportConfig> loadImports(
            final String databaseKey,
            final Map<String, Object> body,
            final DefaultsConfig defaults,
            final RepositoryConfig repository,
            final String databasePath) {
        final Map<String, Object> importsNode = YamlMapSupport.optionalMap(body, "imports", databasePath);
        if (importsNode == null) {
            return Map.of();
        }

        final Map<String, ImportConfig> imports = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : importsNode.entrySet()) {
            final String importKey = entry.getKey();
            if (!(entry.getValue() instanceof Map<?, ?> importBody)) {
                throw new ConfigException(
                        "Expected map for import '" + importKey + "' in database '" + databaseKey + "'.");
            }
            final String path = databasePath + ".imports." + importKey;
            final Map<String, Object> importNode = YamlMapSupport.toStringMap(importBody, path);
            YamlMapSupport.assertKeys(importNode, Set.of("modules", "dir", "preImportDirs", "postImportDirs"), path);

            final List<String> modules =
                    YamlMapSupport.optionalStringList(importNode, "modules", path, repository.modules());
            validateModulesExist(modules, repository, "import", importKey, databaseKey);

            final String dir = YamlMapSupport.optionalString(importNode, "dir", path) == null
                    ? defaults.importDir()
                    : YamlMapSupport.requireString(importNode, "dir", path);
            imports.put(
                    importKey,
                    new ImportConfig(
                            importKey,
                            modules,
                            dir,
                            YamlMapSupport.optionalStringList(
                                    importNode, "preImportDirs", path, defaults.preImportDirs()),
                            YamlMapSupport.optionalStringList(
                                    importNode, "postImportDirs", path, defaults.postImportDirs())));
        }
        return Map.copyOf(imports);
    }

    private static Map<String, ModuleGroupConfig> loadModuleGroups(
            final String databaseKey,
            final Map<String, Object> body,
            final RepositoryConfig repository,
            final String databasePath) {
        final Map<String, Object> groupsNode = YamlMapSupport.optionalMap(body, "moduleGroups", databasePath);
        if (groupsNode == null) {
            return Map.of();
        }

        final Map<String, ModuleGroupConfig> groups = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : groupsNode.entrySet()) {
            final String groupKey = entry.getKey();
            if (!(entry.getValue() instanceof Map<?, ?> groupBody)) {
                throw new ConfigException(
                        "Expected map for module group '" + groupKey + "' in database '" + databaseKey + "'.");
            }
            final String path = databasePath + ".moduleGroups." + groupKey;
            final Map<String, Object> groupNode = YamlMapSupport.toStringMap(groupBody, path);
            YamlMapSupport.assertKeys(groupNode, Set.of("modules", "importEnabled"), path);
            final List<String> modules = YamlMapSupport.requireStringList(groupNode, "modules", path);
            validateModulesExist(modules, repository, "module group", groupKey, databaseKey);
            final Boolean importEnabledValue = YamlMapSupport.optionalBoolean(groupNode, "importEnabled", path);
            final boolean importEnabled = importEnabledValue != null && importEnabledValue;
            groups.put(groupKey, new ModuleGroupConfig(groupKey, modules, importEnabled));
        }
        return Map.copyOf(groups);
    }

    private static void validateModulesExist(
            final List<String> modules,
            final RepositoryConfig repository,
            final String context,
            final String contextKey,
            final String databaseKey) {
        for (final String module : modules) {
            if (!repository.modules().contains(module)) {
                throw new ConfigException("Module '"
                        + module
                        + "' in "
                        + context
                        + " '"
                        + contextKey
                        + "' for database '"
                        + databaseKey
                        + "' is not present in repository modules "
                        + repository.modules()
                        + '.');
            }
        }
    }
}
