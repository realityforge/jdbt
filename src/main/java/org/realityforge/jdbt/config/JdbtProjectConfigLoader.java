package org.realityforge.jdbt.config;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.repository.RepositoryConfig;

public final class JdbtProjectConfigLoader {
    public JdbtProjectConfig load(final String yaml, final String sourceName, final RepositoryConfig repository) {
        final Map<String, Object> root = YamlMapSupport.parseRoot(yaml, sourceName);
        YamlMapSupport.assertKeys(root, Set.of("defaults", "databases"), sourceName);

        final DefaultsConfig defaults = loadDefaults(root, sourceName);
        final Map<String, Object> databasesNode = YamlMapSupport.requireMap(root, "databases", sourceName);
        if (databasesNode.isEmpty()) {
            throw new ConfigException("No databases defined in " + sourceName + '.');
        }

        final Map<String, DatabaseConfig> databases = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : databasesNode.entrySet()) {
            if (!(entry.getValue() instanceof Map<?, ?> dbBody)) {
                throw new ConfigException("Expected map for database '" + entry.getKey() + "' in " + sourceName + '.');
            }
            final DatabaseConfig database = loadDatabase(
                    entry.getKey(),
                    YamlMapSupport.toStringMap(dbBody, sourceName + ".databases." + entry.getKey()),
                    defaults,
                    repository,
                    sourceName);
            databases.put(entry.getKey(), database);
        }

        return new JdbtProjectConfig(defaults, databases);
    }

    private DefaultsConfig loadDefaults(final Map<String, Object> root, final String sourceName) {
        final Map<String, Object> defaultsNode = YamlMapSupport.optionalMap(root, "defaults", sourceName);
        final DefaultsConfig rubyDefaults = DefaultsConfig.rubyCompatibleDefaults();
        if (defaultsNode == null) {
            return rubyDefaults;
        }

        YamlMapSupport.assertKeys(
                defaultsNode,
                Set.of(
                        "searchDirs",
                        "upDirs",
                        "downDirs",
                        "finalizeDirs",
                        "preCreateDirs",
                        "postCreateDirs",
                        "preImportDirs",
                        "postImportDirs",
                        "importDir",
                        "datasetsDirName",
                        "preDatasetDirs",
                        "postDatasetDirs",
                        "fixtureDirName",
                        "migrationsDirName",
                        "indexFileName",
                        "defaultDatabase",
                        "defaultImport"),
                sourceName + ".defaults");

        return rubyDefaults.merge(
                optionalList(defaultsNode, "searchDirs", sourceName + ".defaults"),
                optionalList(defaultsNode, "upDirs", sourceName + ".defaults"),
                optionalList(defaultsNode, "downDirs", sourceName + ".defaults"),
                optionalList(defaultsNode, "finalizeDirs", sourceName + ".defaults"),
                optionalList(defaultsNode, "preCreateDirs", sourceName + ".defaults"),
                optionalList(defaultsNode, "postCreateDirs", sourceName + ".defaults"),
                optionalList(defaultsNode, "preImportDirs", sourceName + ".defaults"),
                optionalList(defaultsNode, "postImportDirs", sourceName + ".defaults"),
                YamlMapSupport.optionalString(defaultsNode, "importDir", sourceName + ".defaults"),
                YamlMapSupport.optionalString(defaultsNode, "datasetsDirName", sourceName + ".defaults"),
                optionalList(defaultsNode, "preDatasetDirs", sourceName + ".defaults"),
                optionalList(defaultsNode, "postDatasetDirs", sourceName + ".defaults"),
                YamlMapSupport.optionalString(defaultsNode, "fixtureDirName", sourceName + ".defaults"),
                YamlMapSupport.optionalString(defaultsNode, "migrationsDirName", sourceName + ".defaults"),
                YamlMapSupport.optionalString(defaultsNode, "indexFileName", sourceName + ".defaults"),
                YamlMapSupport.optionalString(defaultsNode, "defaultDatabase", sourceName + ".defaults"),
                YamlMapSupport.optionalString(defaultsNode, "defaultImport", sourceName + ".defaults"));
    }

    private DatabaseConfig loadDatabase(
            final String key,
            final Map<String, Object> body,
            final DefaultsConfig defaults,
            final RepositoryConfig repository,
            final String sourceName) {
        final String path = sourceName + ".databases." + key;
        YamlMapSupport.assertKeys(
                body,
                Set.of(
                        "searchDirs",
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

        final List<String> searchDirs =
                YamlMapSupport.optionalStringList(body, "searchDirs", path, defaults.searchDirs());
        if (searchDirs.isEmpty()) {
            throw new ConfigException(
                    "Database '" + key + "' must define non-empty searchDirs (directly or via defaults).");
        }

        final @Nullable Boolean migrationsValue = YamlMapSupport.optionalBoolean(body, "migrations", path);
        final boolean migrations = migrationsValue != null && migrationsValue;
        final @Nullable Boolean migrationsAppliedAtCreate =
                YamlMapSupport.optionalBoolean(body, "migrationsAppliedAtCreate", path);

        final Map<String, ImportConfig> imports = loadImports(key, body, defaults, repository, path);
        final Map<String, ModuleGroupConfig> moduleGroups = loadModuleGroups(key, body, repository, path);

        return new DatabaseConfig(
                key,
                searchDirs,
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

    private Map<String, ImportConfig> loadImports(
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

    private Map<String, ModuleGroupConfig> loadModuleGroups(
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
            final @Nullable Boolean importEnabledValue =
                    YamlMapSupport.optionalBoolean(groupNode, "importEnabled", path);
            final boolean importEnabled = importEnabledValue != null && importEnabledValue;
            groups.put(groupKey, new ModuleGroupConfig(groupKey, modules, importEnabled));
        }
        return Map.copyOf(groups);
    }

    private @Nullable List<String> optionalList(final Map<String, Object> map, final String key, final String path) {
        return map.containsKey(key) ? YamlMapSupport.requireStringList(map, key, path) : null;
    }

    private void validateModulesExist(
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
