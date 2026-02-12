package org.realityforge.jdbt.runtime;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.config.ModuleGroupConfig;
import org.realityforge.jdbt.db.DatabaseConnection;
import org.realityforge.jdbt.db.DbDriver;
import org.realityforge.jdbt.files.ArtifactContent;
import org.realityforge.jdbt.files.FileResolver;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

public final class RuntimeEngine {
    private static final Pattern ARTIFACT_FILE_PATTERN = Pattern.compile("^zip:([^:]+):(.+)$");
    private static final Pattern GO_SPLIT_PATTERN = Pattern.compile("(?im)(?:^|\\s)GO(?:\\s|$)");

    private final DbDriver db;
    private final FileResolver fileResolver;

    public RuntimeEngine(final DbDriver db, final FileResolver fileResolver) {
        this.db = db;
        this.fileResolver = fileResolver;
    }

    public String status(final RuntimeDatabase database) {
        return "Database Version: "
                + database.version()
                + '\n'
                + "Database Schema Hash: "
                + database.schemaHash()
                + '\n'
                + "Migration Support: "
                + (database.migrationsEnabled() ? "Yes" : "No")
                + '\n';
    }

    public void create(final RuntimeDatabase database, final DatabaseConnection target, final boolean noCreate) {
        createDatabaseIfRequired(database, target, noCreate);
        withDatabaseConnection(target, false, () -> {
            for (final String dir : database.preCreateDirs()) {
                processDirSet(database, dir);
            }
            performCreateAction(database, ModuleMode.UP);
            performCreateAction(database, ModuleMode.FINALIZE);
            for (final String dir : database.postCreateDirs()) {
                processDirSet(database, dir);
            }
        });
    }

    public void createWithDataset(
            final RuntimeDatabase database,
            final DatabaseConnection target,
            final boolean noCreate,
            final String datasetName) {
        ensureDatasetExists(database, datasetName);
        createDatabaseIfRequired(database, target, noCreate);
        withDatabaseConnection(target, false, () -> {
            for (final String dir : database.preCreateDirs()) {
                processDirSet(database, dir);
            }
            performCreateAction(database, ModuleMode.UP);
            performPreDatasetHooks(database, datasetName);
            performLoadDataset(database, datasetName);
            performPostDatasetHooks(database, datasetName);
            performCreateAction(database, ModuleMode.FINALIZE);
            for (final String dir : database.postCreateDirs()) {
                processDirSet(database, dir);
            }
        });
    }

    public void drop(final RuntimeDatabase database, final DatabaseConnection target) {
        withDatabaseConnection(target, true, () -> db.drop(database, target));
    }

    public void upModuleGroup(
            final RuntimeDatabase database, final String moduleGroupKey, final DatabaseConnection target) {
        final ModuleGroupConfig moduleGroup = moduleGroup(database, moduleGroupKey);
        withDatabaseConnection(target, false, () -> {
            for (final String moduleName : database.repository().modules()) {
                if (!moduleGroup.modules().contains(moduleName)) {
                    continue;
                }
                createModule(database, moduleName, ModuleMode.UP);
                createModule(database, moduleName, ModuleMode.FINALIZE);
            }
        });
    }

    public void downModuleGroup(
            final RuntimeDatabase database, final String moduleGroupKey, final DatabaseConnection target) {
        final ModuleGroupConfig moduleGroup = moduleGroup(database, moduleGroupKey);
        withDatabaseConnection(target, false, () -> {
            final List<String> modules = new ArrayList<>(database.repository().modules());
            java.util.Collections.reverse(modules);
            for (final String moduleName : modules) {
                if (!moduleGroup.modules().contains(moduleName)) {
                    continue;
                }
                processModule(database, moduleName, ModuleMode.DOWN);
                final List<String> tables = new ArrayList<>(database.tableOrdering(moduleName));
                java.util.Collections.reverse(tables);
                db.dropSchema(database.schemaNameForModule(moduleName), tables);
            }
        });
    }

    public void loadDataset(final RuntimeDatabase database, final String datasetName, final DatabaseConnection target) {
        ensureDatasetExists(database, datasetName);
        withDatabaseConnection(target, false, () -> {
            performPreDatasetHooks(database, datasetName);
            performLoadDataset(database, datasetName);
            performPostDatasetHooks(database, datasetName);
        });
    }

    private void createDatabaseIfRequired(
            final RuntimeDatabase database, final DatabaseConnection target, final boolean noCreate) {
        if (noCreate) {
            return;
        }
        withDatabaseConnection(target, true, () -> {
            db.drop(database, target);
            db.createDatabase(database, target);
        });
    }

    private void withDatabaseConnection(
            final DatabaseConnection target, final boolean openControlDatabase, final Runnable action) {
        db.open(target, openControlDatabase);
        try {
            action.run();
        } finally {
            db.close();
        }
    }

    private void performCreateAction(final RuntimeDatabase database, final ModuleMode mode) {
        for (final String moduleName : database.repository().modules()) {
            createModule(database, moduleName, mode);
        }
    }

    private void createModule(final RuntimeDatabase database, final String moduleName, final ModuleMode mode) {
        if (ModuleMode.UP == mode) {
            db.createSchema(database.schemaNameForModule(moduleName));
        }
        processModule(database, moduleName, mode);
    }

    private void processModule(final RuntimeDatabase database, final String moduleName, final ModuleMode mode) {
        final List<String> dirs =
                switch (mode) {
                    case UP -> database.upDirs();
                    case DOWN -> database.downDirs();
                    case FINALIZE -> database.finalizeDirs();
                };
        for (final String dir : dirs) {
            processDirSet(database, moduleName + '/' + dir);
        }
        if (ModuleMode.UP == mode) {
            loadFixturesFromDir(database, moduleName, database.fixtureDirName());
        }
    }

    private void processDirSet(final RuntimeDatabase database, final String dir) {
        final List<String> files = fileResolver.collectFiles(
                database.searchDirs(),
                dir,
                "sql",
                database.indexFileName(),
                database.postDbArtifacts(),
                database.preDbArtifacts());
        for (final String file : files) {
            final String sql = loadData(database, file);
            runSqlBatch(sql, false);
        }
    }

    private void performPreDatasetHooks(final RuntimeDatabase database, final String datasetName) {
        for (final String preDir : database.preDatasetDirs()) {
            processDirSet(database, database.datasetsDirName() + '/' + datasetName + '/' + preDir);
        }
    }

    private void performPostDatasetHooks(final RuntimeDatabase database, final String datasetName) {
        for (final String postDir : database.postDatasetDirs()) {
            processDirSet(database, database.datasetsDirName() + '/' + datasetName + '/' + postDir);
        }
    }

    private void performLoadDataset(final RuntimeDatabase database, final String datasetName) {
        final String subdir = database.datasetsDirName() + '/' + datasetName;
        final Map<String, String> fixtures = new LinkedHashMap<>();
        for (final String moduleName : database.repository().modules()) {
            fixtures.putAll(collectFixtures(database, moduleName, subdir));
        }

        final List<String> modules = new ArrayList<>(database.repository().modules());
        final List<String> reversedModules = new ArrayList<>(modules);
        java.util.Collections.reverse(reversedModules);
        for (final String moduleName : reversedModules) {
            downFixtures(database, moduleName, fixtures);
        }
        for (final String moduleName : modules) {
            upFixtures(database, moduleName, fixtures);
        }
    }

    private void loadFixturesFromDir(final RuntimeDatabase database, final String moduleName, final String subdir) {
        final Map<String, String> fixtures = collectFixtures(database, moduleName, subdir);
        downFixtures(database, moduleName, fixtures);
        upFixtures(database, moduleName, fixtures);
    }

    private Map<String, String> collectFixtures(
            final RuntimeDatabase database, final String moduleName, final String subdir) {
        return fileResolver.collectFixtures(
                database.searchDirs(),
                moduleName,
                subdir,
                database.orderedElementsForModule(moduleName),
                database.postDbArtifacts(),
                database.preDbArtifacts());
    }

    private void downFixtures(
            final RuntimeDatabase database, final String moduleName, final Map<String, String> fixtures) {
        final List<String> tables = new ArrayList<>(database.tableOrdering(moduleName));
        java.util.Collections.reverse(tables);
        for (final String tableName : tables) {
            if (fixtures.containsKey(tableName)) {
                db.execute("DELETE FROM " + tableName, false);
            }
        }

        final List<String> sequences = new ArrayList<>(database.sequenceOrdering(moduleName));
        java.util.Collections.reverse(sequences);
        for (final String sequenceName : sequences) {
            if (fixtures.containsKey(sequenceName)) {
                db.updateSequence(sequenceName, 1L);
            }
        }
    }

    private void upFixtures(
            final RuntimeDatabase database, final String moduleName, final Map<String, String> fixtures) {
        for (final String tableName : database.tableOrdering(moduleName)) {
            final @Nullable String fixture = fixtures.get(tableName);
            if (null != fixture) {
                loadFixture(tableName, loadData(database, fixture));
            }
        }

        for (final String sequenceName : database.sequenceOrdering(moduleName)) {
            final @Nullable String fixture = fixtures.get(sequenceName);
            if (null != fixture) {
                loadSequenceFixture(sequenceName, loadData(database, fixture));
            }
        }
    }

    private void loadFixture(final String tableName, final String content) {
        final Object parsed = parseYaml(content);
        if (null == parsed) {
            return;
        }

        final List<Map<String, Object>> fixtureGroups = toFixtureGroupList(parsed, tableName);
        db.preFixtureImport(tableName);
        for (final Map<String, Object> fixtureGroup : fixtureGroups) {
            for (final Map.Entry<String, Object> fixture : fixtureGroup.entrySet()) {
                if (!(fixture.getValue() instanceof Map<?, ?> data)) {
                    throw new RuntimeExecutionException(
                            "Bad data for " + tableName + " fixture named " + fixture.getKey() + " (not map)");
                }
                db.insert(tableName, toStringObjectMap(data));
            }
            db.postFixtureImport(tableName);
        }
    }

    private void loadSequenceFixture(final String sequenceName, final String content) {
        final Object parsed = parseYaml(content);
        if (null == parsed) {
            return;
        }

        final long value;
        if (parsed instanceof Number number) {
            value = number.longValue();
        } else if (parsed instanceof String text) {
            value = Long.parseLong(text);
        } else {
            throw new RuntimeExecutionException("Bad sequence fixture for " + sequenceName + ": " + parsed);
        }
        db.updateSequence(sequenceName, value);
    }

    private List<Map<String, Object>> toFixtureGroupList(final Object parsed, final String tableName) {
        if (parsed instanceof Map<?, ?> map) {
            return List.of(toStringObjectMap(map));
        }
        if (parsed instanceof List<?> list) {
            final List<Map<String, Object>> groups = new ArrayList<>(list.size());
            for (final Object entry : list) {
                if (!(entry instanceof Map<?, ?> map)) {
                    throw new RuntimeExecutionException("Bad data for " + tableName + " fixture group " + entry);
                }
                groups.add(toStringObjectMap(map));
            }
            return List.copyOf(groups);
        }
        throw new RuntimeExecutionException("Bad data for " + tableName + " fixture payload " + parsed);
    }

    private Map<String, Object> toStringObjectMap(final Map<?, ?> data) {
        final Map<String, Object> values = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : data.entrySet()) {
            values.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return Map.copyOf(values);
    }

    private Object parseYaml(final String content) {
        final LoadSettings settings =
                LoadSettings.builder().setAllowDuplicateKeys(false).build();
        return new Load(settings).loadFromString(content);
    }

    private String loadData(final RuntimeDatabase database, final String location) {
        final Matcher matcher = ARTIFACT_FILE_PATTERN.matcher(location);
        if (matcher.matches()) {
            final String artifactId = matcher.group(1);
            final String path = matcher.group(2);
            final ArtifactContent artifact = database.artifactById(artifactId);
            if (null == artifact) {
                throw new RuntimeExecutionException("Unable to locate artifact with id '" + artifactId + "'.");
            }
            return artifact.readText(path);
        }
        try {
            return Files.readString(Path.of(location));
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to read file " + location, ioe);
        }
    }

    private void runSqlBatch(final String sql, final boolean executeInControlDatabase) {
        final String normalizedSql = sql.replace("\r", "");
        for (final String batch : GO_SPLIT_PATTERN.splitAsStream(normalizedSql).toList()) {
            if (!batch.trim().isEmpty()) {
                db.execute(batch, executeInControlDatabase);
            }
        }
    }

    private ModuleGroupConfig moduleGroup(final RuntimeDatabase database, final String moduleGroupKey) {
        final ModuleGroupConfig moduleGroup = database.moduleGroups().get(moduleGroupKey);
        if (null == moduleGroup) {
            throw new RuntimeExecutionException(
                    "Unable to locate module group definition by key '" + moduleGroupKey + "'");
        }
        return moduleGroup;
    }

    private void ensureDatasetExists(final RuntimeDatabase database, final String datasetName) {
        if (!database.datasets().contains(datasetName)) {
            throw new RuntimeExecutionException("Unknown dataset '" + datasetName + "'");
        }
    }

    private enum ModuleMode {
        UP,
        DOWN,
        FINALIZE
    }
}
