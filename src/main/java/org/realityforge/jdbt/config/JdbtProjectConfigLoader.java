package org.realityforge.jdbt.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class JdbtProjectConfigLoader {
    private static final Set<String> SUPPORTED_KEYS = Set.of(
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
            "dataPath",
            "logPath",
            "forceDrop",
            "deleteBackupHistory",
            "reindexOnImport",
            "shrinkOnImport",
            "preDbArtifacts",
            "postDbArtifacts",
            "filterProperties",
            "imports",
            "moduleGroups",
            "resourceRoot");
    private static final List<String> DEFAULT_UP_DIRS =
            List.of(".", "types", "views", "functions", "stored-procedures", "misc");
    private static final List<String> DEFAULT_DOWN_DIRS = List.of("down");
    private static final List<String> DEFAULT_FINALIZE_DIRS = List.of("triggers", "finalize");
    private static final List<String> DEFAULT_PRE_CREATE_DIRS = List.of("db-hooks/pre");
    private static final List<String> DEFAULT_POST_CREATE_DIRS = List.of("db-hooks/post");
    private static final List<String> DEFAULT_PRE_IMPORT_DIRS = List.of("import-hooks/pre");
    private static final List<String> DEFAULT_POST_IMPORT_DIRS = List.of("import-hooks/post");
    private static final String DEFAULT_IMPORT_DIR = "import";
    private static final String DEFAULT_DATASETS_DIR_NAME = "datasets";
    private static final List<String> DEFAULT_PRE_DATASET_DIRS = List.of("pre");
    private static final List<String> DEFAULT_POST_DATASET_DIRS = List.of("post");
    private static final String DEFAULT_FIXTURE_DIR_NAME = "fixtures";
    private static final String DEFAULT_MIGRATIONS_DIR_NAME = "migrations";
    private static final Set<String> RESERVED_FILTER_PROPERTY_KEYS =
            Set.of("sourceDatabase", "targetDatabase", "table");
    private static final Set<String> RESERVED_FILTER_PATTERNS = Set.of("__SOURCE__", "__TARGET__", "__TABLE__");

    public ParsedProjectConfig parse(final String yaml, final String sourceName) {
        final var root = YamlMapSupport.parseRoot(yaml, sourceName);
        YamlMapSupport.assertKeys(root, SUPPORTED_KEYS, sourceName);
        return new ParsedProjectConfig(root, sourceName);
    }

    public JdbtProjectConfig load(final ParsedProjectConfig parsed, final List<String> repositoryModules) {
        final var root = parsed.root();
        final var sourceName = parsed.sourceName();
        final var database = loadDatabase(root, repositoryModules, sourceName);
        final var resourceRoot = YamlMapSupport.optionalString(root, "resourceRoot", sourceName);
        return new JdbtProjectConfig(database, null == resourceRoot ? "." : resourceRoot);
    }

    private static DatabaseConfig loadDatabase(
            final Map<String, Object> body, final List<String> repositoryModules, final String sourceName) {
        final var path = sourceName;
        final var migrationsValue = YamlMapSupport.optionalBoolean(body, "migrations", path);
        final var migrations = migrationsValue != null && migrationsValue;
        final var migrationsAppliedAtCreate = YamlMapSupport.optionalBoolean(body, "migrationsAppliedAtCreate", path);

        final var filterProperties = loadFilterProperties(body, path);
        final var imports = loadImports(body, repositoryModules, path);
        final var moduleGroups = loadModuleGroups(body, repositoryModules, path);

        return new DatabaseConfig(
                YamlMapSupport.optionalStringList(body, "upDirs", path, DEFAULT_UP_DIRS),
                YamlMapSupport.optionalStringList(body, "downDirs", path, DEFAULT_DOWN_DIRS),
                YamlMapSupport.optionalStringList(body, "finalizeDirs", path, DEFAULT_FINALIZE_DIRS),
                YamlMapSupport.optionalStringList(body, "preCreateDirs", path, DEFAULT_PRE_CREATE_DIRS),
                YamlMapSupport.optionalStringList(body, "postCreateDirs", path, DEFAULT_POST_CREATE_DIRS),
                YamlMapSupport.optionalStringList(body, "datasets", path, List.of()),
                YamlMapSupport.optionalString(body, "datasetsDirName", path) == null
                        ? DEFAULT_DATASETS_DIR_NAME
                        : YamlMapSupport.requireString(body, "datasetsDirName", path),
                YamlMapSupport.optionalStringList(body, "preDatasetDirs", path, DEFAULT_PRE_DATASET_DIRS),
                YamlMapSupport.optionalStringList(body, "postDatasetDirs", path, DEFAULT_POST_DATASET_DIRS),
                YamlMapSupport.optionalString(body, "fixtureDirName", path) == null
                        ? DEFAULT_FIXTURE_DIR_NAME
                        : YamlMapSupport.requireString(body, "fixtureDirName", path),
                migrations,
                migrationsAppliedAtCreate == null ? migrations : migrationsAppliedAtCreate,
                YamlMapSupport.optionalString(body, "migrationsDirName", path) == null
                        ? DEFAULT_MIGRATIONS_DIR_NAME
                        : YamlMapSupport.requireString(body, "migrationsDirName", path),
                YamlMapSupport.optionalString(body, "version", path),
                YamlMapSupport.optionalString(body, "dataPath", path),
                YamlMapSupport.optionalString(body, "logPath", path),
                booleanDefault(body, "forceDrop", path, false),
                booleanDefault(body, "deleteBackupHistory", path, true),
                booleanDefault(body, "reindexOnImport", path, true),
                booleanDefault(body, "shrinkOnImport", path, false),
                YamlMapSupport.optionalStringList(body, "preDbArtifacts", path, List.of()),
                YamlMapSupport.optionalStringList(body, "postDbArtifacts", path, List.of()),
                filterProperties,
                imports,
                moduleGroups);
    }

    private static boolean booleanDefault(
            final Map<String, Object> body, final String key, final String path, final boolean defaultValue) {
        final var value = YamlMapSupport.optionalBoolean(body, key, path);
        return null == value ? defaultValue : value;
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
            final Map<String, Object> body, final List<String> repositoryModules, final String databasePath) {
        final var importsNode = YamlMapSupport.optionalMap(body, "imports", databasePath);
        if (importsNode == null) {
            return Map.of();
        }

        final var imports = new LinkedHashMap<String, ImportConfig>();
        for (final var entry : importsNode.entrySet()) {
            final var importKey = entry.getKey();
            if (!(entry.getValue() instanceof Map<?, ?> importBody)) {
                throw new ConfigException("Expected map for import '" + importKey + "' in " + databasePath + '.');
            }
            final var path = databasePath + ".imports." + importKey;
            final var importNode = YamlMapSupport.toStringMap(importBody, path);
            YamlMapSupport.assertKeys(importNode, Set.of("modules", "dir", "preImportDirs", "postImportDirs"), path);

            final var modules = YamlMapSupport.optionalStringList(importNode, "modules", path, repositoryModules);
            validateModulesExist(modules, repositoryModules, "import", importKey, databasePath);

            final var dir = YamlMapSupport.optionalString(importNode, "dir", path) == null
                    ? DEFAULT_IMPORT_DIR
                    : YamlMapSupport.requireString(importNode, "dir", path);
            imports.put(
                    importKey,
                    new ImportConfig(
                            importKey,
                            modules,
                            dir,
                            YamlMapSupport.optionalStringList(
                                    importNode, "preImportDirs", path, DEFAULT_PRE_IMPORT_DIRS),
                            YamlMapSupport.optionalStringList(
                                    importNode, "postImportDirs", path, DEFAULT_POST_IMPORT_DIRS)));
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(imports));
    }

    private static Map<String, ModuleGroupConfig> loadModuleGroups(
            final Map<String, Object> body, final List<String> repositoryModules, final String databasePath) {
        final var groupsNode = YamlMapSupport.optionalMap(body, "moduleGroups", databasePath);
        if (groupsNode == null) {
            return Map.of();
        }

        final var groups = new LinkedHashMap<String, ModuleGroupConfig>();
        for (final var entry : groupsNode.entrySet()) {
            final var groupKey = entry.getKey();
            if (!(entry.getValue() instanceof Map<?, ?> groupBody)) {
                throw new ConfigException("Expected map for module group '" + groupKey + "' in " + databasePath + '.');
            }
            final var path = databasePath + ".moduleGroups." + groupKey;
            final var groupNode = YamlMapSupport.toStringMap(groupBody, path);
            YamlMapSupport.assertKeys(groupNode, Set.of("modules", "importEnabled"), path);
            final var modules = YamlMapSupport.requireStringList(groupNode, "modules", path);
            validateModulesExist(modules, repositoryModules, "module group", groupKey, databasePath);
            final var importEnabledValue = YamlMapSupport.optionalBoolean(groupNode, "importEnabled", path);
            final var importEnabled = importEnabledValue != null && importEnabledValue;
            groups.put(groupKey, new ModuleGroupConfig(groupKey, modules, importEnabled));
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(groups));
    }

    private static void validateModulesExist(
            final List<String> modules,
            final List<String> repositoryModules,
            final String context,
            final String contextKey,
            final String sourceName) {
        for (final var module : modules) {
            if (!repositoryModules.contains(module)) {
                throw new ConfigException("Module '"
                        + module
                        + "' in "
                        + context
                        + " '"
                        + contextKey
                        + "' in "
                        + sourceName
                        + " is not present in repository modules "
                        + repositoryModules
                        + '.');
            }
        }
    }

    public record ParsedProjectConfig(Map<String, Object> root, String sourceName) {
        public ParsedProjectConfig {
            root = Collections.unmodifiableMap(new LinkedHashMap<>(root));
        }
    }
}
