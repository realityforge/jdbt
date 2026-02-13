package org.realityforge.jdbt.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.realityforge.jdbt.repository.RepositoryConfig;

public final class JdbtProjectConfigLoader {
    private static final Set<String> RESERVED_FILTER_PROPERTY_KEYS =
            Set.of("sourceDatabase", "targetDatabase", "table");
    private static final Set<String> RESERVED_FILTER_PATTERNS = Set.of("__SOURCE__", "__TARGET__", "__TABLE__");

    public JdbtProjectConfig load(final String yaml, final String sourceName, final RepositoryConfig repository) {
        final var root = YamlMapSupport.parseRoot(yaml, sourceName);
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
                        "filterProperties",
                        "imports",
                        "moduleGroups"),
                sourceName);

        final var defaults = DefaultsConfig.rubyCompatibleDefaults();
        final var database = loadDatabase(defaults.defaultDatabase(), root, defaults, repository, sourceName);
        return new JdbtProjectConfig(defaults, database);
    }

    private static DatabaseConfig loadDatabase(
            final String key,
            final Map<String, Object> body,
            final DefaultsConfig defaults,
            final RepositoryConfig repository,
            final String sourceName) {
        final var path = sourceName;
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
                        "filterProperties",
                        "imports",
                        "moduleGroups"),
                path);

        final var migrationsValue = YamlMapSupport.optionalBoolean(body, "migrations", path);
        final var migrations = migrationsValue != null && migrationsValue;
        final var migrationsAppliedAtCreate = YamlMapSupport.optionalBoolean(body, "migrationsAppliedAtCreate", path);

        final var filterProperties = loadFilterProperties(body, path);
        final var imports = loadImports(key, body, defaults, repository, path);
        final var moduleGroups = loadModuleGroups(key, body, repository, path);

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
                filterProperties,
                imports,
                moduleGroups);
    }

    private static Map<String, FilterPropertyConfig> loadFilterProperties(
            final Map<String, Object> body, final String databasePath) {
        final var propertiesNode = YamlMapSupport.optionalMap(body, "filterProperties", databasePath);
        if (null == propertiesNode) {
            return Map.of();
        }

        final var filterProperties = new LinkedHashMap<String, FilterPropertyConfig>();
        final var seenPatterns = new LinkedHashMap<String, String>();
        for (final var entry : propertiesNode.entrySet()) {
            final var propertyKey = entry.getKey();
            if (RESERVED_FILTER_PROPERTY_KEYS.contains(propertyKey)) {
                throw new ConfigException("Filter property '"
                        + propertyKey
                        + "' in "
                        + databasePath
                        + ".filterProperties is reserved and tool-provided.");
            }
            if (!(entry.getValue() instanceof Map<?, ?> propertyBody)) {
                throw new ConfigException(
                        "Expected map for filter property '" + propertyKey + "' in " + databasePath + '.');
            }

            final var propertyPath = databasePath + ".filterProperties." + propertyKey;
            final var propertyNode = YamlMapSupport.toStringMap(propertyBody, propertyPath);
            YamlMapSupport.assertKeys(propertyNode, Set.of("pattern", "default", "supportedValues"), propertyPath);

            final var pattern = YamlMapSupport.requireString(propertyNode, "pattern", propertyPath);
            if (pattern.isBlank()) {
                throw new ConfigException("Filter property '"
                        + propertyKey
                        + "' in "
                        + propertyPath
                        + " must define a non-empty pattern.");
            }
            if (RESERVED_FILTER_PATTERNS.contains(pattern)) {
                throw new ConfigException("Filter property '"
                        + propertyKey
                        + "' in "
                        + propertyPath
                        + " defines reserved pattern '"
                        + pattern
                        + "'.");
            }
            final var existingPatternOwner = seenPatterns.putIfAbsent(pattern, propertyKey);
            if (null != existingPatternOwner) {
                throw new ConfigException("Duplicate filter pattern '"
                        + pattern
                        + "' declared for '"
                        + propertyKey
                        + "' and '"
                        + existingPatternOwner
                        + "' in "
                        + databasePath
                        + ".filterProperties.");
            }

            final var defaultValue = YamlMapSupport.optionalString(propertyNode, "default", propertyPath);
            final List<String> supportedValues;
            if (propertyNode.containsKey("supportedValues")) {
                supportedValues = YamlMapSupport.requireStringList(propertyNode, "supportedValues", propertyPath);
                if (supportedValues.isEmpty()) {
                    throw new ConfigException("Filter property '"
                            + propertyKey
                            + "' in "
                            + propertyPath
                            + " must define non-empty 'supportedValues' when specified.");
                }
                final var uniqueValues = new LinkedHashMap<String, Boolean>();
                for (final var value : supportedValues) {
                    uniqueValues.put(value, Boolean.TRUE);
                }
                if (uniqueValues.size() != supportedValues.size()) {
                    throw new ConfigException("Filter property '"
                            + propertyKey
                            + "' in "
                            + propertyPath
                            + " contains duplicate entries in 'supportedValues'.");
                }
                if (null != defaultValue && !supportedValues.contains(defaultValue)) {
                    throw new ConfigException("Filter property '"
                            + propertyKey
                            + "' in "
                            + propertyPath
                            + " declares default '"
                            + defaultValue
                            + "' not present in supportedValues "
                            + supportedValues
                            + '.');
                }
            } else {
                supportedValues = List.of();
            }

            filterProperties.put(propertyKey, new FilterPropertyConfig(pattern, defaultValue, supportedValues));
        }

        return Collections.unmodifiableMap(new LinkedHashMap<>(filterProperties));
    }

    private static Map<String, ImportConfig> loadImports(
            final String databaseKey,
            final Map<String, Object> body,
            final DefaultsConfig defaults,
            final RepositoryConfig repository,
            final String databasePath) {
        final var importsNode = YamlMapSupport.optionalMap(body, "imports", databasePath);
        if (importsNode == null) {
            return Map.of();
        }

        final var imports = new LinkedHashMap<String, ImportConfig>();
        for (final var entry : importsNode.entrySet()) {
            final var importKey = entry.getKey();
            if (!(entry.getValue() instanceof Map<?, ?> importBody)) {
                throw new ConfigException(
                        "Expected map for import '" + importKey + "' in database '" + databaseKey + "'.");
            }
            final var path = databasePath + ".imports." + importKey;
            final var importNode = YamlMapSupport.toStringMap(importBody, path);
            YamlMapSupport.assertKeys(importNode, Set.of("modules", "dir", "preImportDirs", "postImportDirs"), path);

            final var modules = YamlMapSupport.optionalStringList(importNode, "modules", path, repository.modules());
            validateModulesExist(modules, repository, "import", importKey, databaseKey);

            final var dir = YamlMapSupport.optionalString(importNode, "dir", path) == null
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
        final var groupsNode = YamlMapSupport.optionalMap(body, "moduleGroups", databasePath);
        if (groupsNode == null) {
            return Map.of();
        }

        final var groups = new LinkedHashMap<String, ModuleGroupConfig>();
        for (final var entry : groupsNode.entrySet()) {
            final var groupKey = entry.getKey();
            if (!(entry.getValue() instanceof Map<?, ?> groupBody)) {
                throw new ConfigException(
                        "Expected map for module group '" + groupKey + "' in database '" + databaseKey + "'.");
            }
            final var path = databasePath + ".moduleGroups." + groupKey;
            final var groupNode = YamlMapSupport.toStringMap(groupBody, path);
            YamlMapSupport.assertKeys(groupNode, Set.of("modules", "importEnabled"), path);
            final var modules = YamlMapSupport.requireStringList(groupNode, "modules", path);
            validateModulesExist(modules, repository, "module group", groupKey, databaseKey);
            final var importEnabledValue = YamlMapSupport.optionalBoolean(groupNode, "importEnabled", path);
            final var importEnabled = importEnabledValue != null && importEnabledValue;
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
        for (final var module : modules) {
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
