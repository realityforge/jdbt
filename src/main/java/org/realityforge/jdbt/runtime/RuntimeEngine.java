package org.realityforge.jdbt.runtime;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.config.ImportConfig;
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
            performPostCreateMigrationsSetup(database);
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
            performPostCreateMigrationsSetup(database);
        });
    }

    public void drop(final RuntimeDatabase database, final DatabaseConnection target) {
        withDatabaseConnection(target, true, () -> db.drop(database, target));
    }

    public void migrate(final RuntimeDatabase database, final DatabaseConnection target) {
        withDatabaseConnection(target, false, () -> performMigration(database, MigrationAction.PERFORM));
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
            Collections.reverse(modules);
            for (final String moduleName : modules) {
                if (!moduleGroup.modules().contains(moduleName)) {
                    continue;
                }
                processModule(database, moduleName, ModuleMode.DOWN);
                final List<String> tables = new ArrayList<>(database.tableOrdering(moduleName));
                Collections.reverse(tables);
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

    public void databaseImport(
            final RuntimeDatabase database,
            final String importKey,
            final @Nullable String moduleGroupKey,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final @Nullable String resumeAt) {
        final ImportConfig importConfig = importByKey(database, importKey);
        final ModuleGroupConfig moduleGroup = null == moduleGroupKey ? null : moduleGroup(database, moduleGroupKey);
        withDatabaseConnection(
                target,
                false,
                () -> performImportAction(database, importConfig, target, source, true, moduleGroup, resumeAt));
    }

    public void createByImport(
            final RuntimeDatabase database,
            final String importKey,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final @Nullable String resumeAt,
            final boolean noCreate) {
        final ImportConfig importConfig = importByKey(database, importKey);
        if (null == resumeAt) {
            createDatabaseIfRequired(database, target, noCreate);
        }
        withDatabaseConnection(target, false, () -> {
            if (null == resumeAt) {
                for (final String dir : database.preCreateDirs()) {
                    processDirSet(database, dir);
                }
                performCreateAction(database, ModuleMode.UP);
            }
            performImportAction(database, importConfig, target, source, false, null, resumeAt);
            performCreateAction(database, ModuleMode.FINALIZE);
            for (final String dir : database.postCreateDirs()) {
                processDirSet(database, dir);
            }
            performPostCreateMigrationsSetup(database);
        });
    }

    private void performImportAction(
            final RuntimeDatabase database,
            final ImportConfig importConfig,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final boolean shouldPerformDelete,
            final @Nullable ModuleGroupConfig moduleGroup,
            final @Nullable String resumeAtInput) {
        final var resumeAt = new ResumeState(resumeAtInput);
        final List<String> selectedModules = selectedImportModules(database, importConfig, moduleGroup);

        if (null == moduleGroup && null == resumeAt.value) {
            for (final String dir : importConfig.preImportDirs()) {
                processImportDirSet(database, dir, target, source);
            }
        }

        for (final String moduleName : selectedModules) {
            verifyNoUnexpectedImportFiles(database, moduleName, importConfig.dir());
        }

        if (shouldPerformDelete && null == resumeAt.value) {
            final List<String> deleteOrder = new ArrayList<>();
            for (final String moduleName : selectedModules) {
                final List<String> tables = new ArrayList<>(database.tableOrdering(moduleName));
                Collections.reverse(tables);
                deleteOrder.addAll(tables);
            }
            for (final String table : deleteOrder) {
                db.execute("DELETE FROM " + table, false);
            }
        }

        for (final String moduleName : selectedModules) {
            importModule(database, importConfig, target, source, moduleName, resumeAt);
        }

        if (null != resumeAt.value) {
            throw new RuntimeExecutionException(
                    "Partial import unable to be completed as bad table name supplied " + resumeAt.value);
        }

        if (null == moduleGroup) {
            for (final String dir : importConfig.postImportDirs()) {
                processImportDirSet(database, dir, target, source);
            }
        }
        db.postDatabaseImport(importConfig);
    }

    private static List<String> selectedImportModules(
            final RuntimeDatabase database,
            final ImportConfig importConfig,
            final @Nullable ModuleGroupConfig moduleGroup) {
        final List<String> selectedModules = new ArrayList<>();
        for (final String moduleName : database.repository().modules()) {
            if (!importConfig.modules().contains(moduleName)) {
                continue;
            }
            if (null != moduleGroup && !moduleGroup.modules().contains(moduleName)) {
                continue;
            }
            selectedModules.add(moduleName);
        }
        return List.copyOf(selectedModules);
    }

    private void importModule(
            final RuntimeDatabase database,
            final ImportConfig importConfig,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final String moduleName,
            final ResumeState resumeAt) {
        final List<String> orderedTables = new ArrayList<>(database.tableOrdering(moduleName));
        final List<String> orderedSequences = new ArrayList<>(database.sequenceOrdering(moduleName));

        final List<String> tables = new ArrayList<>();
        for (final String table : orderedTables) {
            final String fixture = fileResolver.findFileInModule(
                    database.searchDirs(),
                    moduleName,
                    database.fixtureDirName(),
                    table,
                    "yml",
                    database.postDbArtifacts(),
                    database.preDbArtifacts());
            if (null == fixture) {
                tables.add(table);
            }
        }

        for (final String table : tables) {
            final String cleanName = cleanObjectName(table);
            if (cleanName.equals(resumeAt.value)) {
                db.execute("DELETE FROM " + table, false);
                resumeAt.value = null;
            }
            if (null == resumeAt.value) {
                db.preTableImport(importConfig, table);
                performImport(database, importConfig, target, source, moduleName, table);
                db.postTableImport(importConfig, table);
            }
        }

        for (final String sequence : orderedSequences) {
            if (cleanObjectName(sequence).equals(resumeAt.value)) {
                resumeAt.value = null;
            }
            if (null == resumeAt.value) {
                performSequenceImport(database, importConfig, target, source, moduleName, sequence);
            }
        }

        if (null == resumeAt.value) {
            db.postDataModuleImport(importConfig, moduleName);
        }
    }

    private void performImport(
            final RuntimeDatabase database,
            final ImportConfig importConfig,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final String moduleName,
            final String tableName) {
        final String fixtureFile = fileResolver.findFileInModule(
                database.searchDirs(),
                moduleName,
                importConfig.dir(),
                tableName,
                "yml",
                database.postDbArtifacts(),
                database.preDbArtifacts());
        final String sqlFile = fileResolver.findFileInModule(
                database.searchDirs(),
                moduleName,
                importConfig.dir(),
                tableName,
                "sql",
                database.postDbArtifacts(),
                database.preDbArtifacts());

        if (null != fixtureFile && null != sqlFile) {
            throw new RuntimeExecutionException("Unexpectedly found both import fixture (" + fixtureFile
                    + ") and import sql (" + sqlFile + ") files.");
        }

        if (null != fixtureFile) {
            loadFixture(tableName, loadData(database, fixtureFile));
        } else if (null != sqlFile) {
            runImportSql(tableName, loadData(database, sqlFile), target.database(), source.database());
        } else {
            performStandardImport(tableName, target.database(), source.database());
        }
    }

    private void performSequenceImport(
            final RuntimeDatabase database,
            final ImportConfig importConfig,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final String moduleName,
            final String sequenceName) {
        final String fixtureFile = fileResolver.findFileInModule(
                database.searchDirs(),
                moduleName,
                importConfig.dir(),
                sequenceName,
                "yml",
                database.postDbArtifacts(),
                database.preDbArtifacts());
        final String sqlFile = fileResolver.findFileInModule(
                database.searchDirs(),
                moduleName,
                importConfig.dir(),
                sequenceName,
                "sql",
                database.postDbArtifacts(),
                database.preDbArtifacts());

        if (null != fixtureFile && null != sqlFile) {
            throw new RuntimeExecutionException("Unexpectedly found both fixture ("
                    + fixtureFile
                    + ") and sql ("
                    + sqlFile
                    + ") files for "
                    + cleanObjectName(sequenceName)
                    + '.');
        }

        if (null != fixtureFile) {
            loadSequenceFixture(sequenceName, loadData(database, fixtureFile));
        } else if (null != sqlFile) {
            runImportSql(sequenceName, loadData(database, sqlFile), target.database(), source.database());
        } else {
            runImportSql(
                    sequenceName,
                    db.generateStandardSequenceImportSql(sequenceName, target.database(), source.database()),
                    target.database(),
                    source.database());
        }
    }

    private void performStandardImport(
            final String tableName, final String targetDatabase, final String sourceDatabase) {
        final List<String> columns = db.columnNamesForTable(tableName);
        runImportSql(
                tableName,
                db.generateStandardImportSql(tableName, targetDatabase, sourceDatabase, columns),
                targetDatabase,
                sourceDatabase);
    }

    private void runImportSql(
            final @Nullable String tableName,
            final String sql,
            final String targetDatabase,
            final String sourceDatabase) {
        String effectiveSql = sql;
        if (null != tableName) {
            effectiveSql = effectiveSql.replace("@@TABLE@@", tableName).replace("__TABLE__", tableName);
        }
        effectiveSql = effectiveSql.replace("@@SOURCE@@", sourceDatabase).replace("__SOURCE__", sourceDatabase);
        effectiveSql = effectiveSql.replace("@@TARGET@@", targetDatabase).replace("__TARGET__", targetDatabase);
        runSqlBatch(effectiveSql, true);
    }

    private void processImportDirSet(
            final RuntimeDatabase database,
            final String dir,
            final DatabaseConnection target,
            final DatabaseConnection source) {
        final List<String> files = fileResolver.collectFiles(
                database.searchDirs(),
                dir,
                "sql",
                database.indexFileName(),
                database.postDbArtifacts(),
                database.preDbArtifacts());
        for (final String file : files) {
            runImportSql(null, loadData(database, file), target.database(), source.database());
        }
    }

    private static void verifyNoUnexpectedImportFiles(
            final RuntimeDatabase database, final String moduleName, final String importDir) {
        final List<String> expected = new ArrayList<>();
        final List<String> orderedElements = new ArrayList<>(database.tableOrdering(moduleName));
        orderedElements.addAll(database.sequenceOrdering(moduleName));
        for (final String table : orderedElements) {
            final String clean = cleanObjectName(table);
            expected.add(clean + ".yml");
            expected.add(clean + ".sql");
        }

        final List<String> additional = new ArrayList<>();
        for (final Path searchDir : database.searchDirs()) {
            final Path directory = searchDir.resolve(moduleName).resolve(importDir);
            if (!Files.isDirectory(directory)) {
                continue;
            }
            try (var stream = Files.list(directory)) {
                stream.filter(Files::isRegularFile)
                        .filter(file -> {
                            final String fileName = file.getFileName().toString();
                            return fileName.endsWith(".yml") || fileName.endsWith(".sql");
                        })
                        .forEach(file -> {
                            final String fileName = file.getFileName().toString();
                            if (!expected.contains(fileName)) {
                                additional.add(file.toString());
                            }
                        });
            } catch (final IOException ioe) {
                throw new UncheckedIOException("Failed scanning import directory " + directory, ioe);
            }
        }

        if (!additional.isEmpty()) {
            throw new RuntimeExecutionException(
                    "Discovered additional files in import directory in database search path. Files: " + additional);
        }
    }

    private static String cleanObjectName(final String value) {
        return value.replace("[", "")
                .replace("]", "")
                .replace("\"", "")
                .replace("'", "")
                .replace(" ", "");
    }

    @SuppressWarnings("SameParameterValue")
    private static String basenameWithoutExtension(final String value, final String extension) {
        final int slash = Math.max(value.lastIndexOf('/'), value.lastIndexOf('\\'));
        final String basename = -1 == slash ? value : value.substring(slash + 1);
        return basename.endsWith(extension) ? basename.substring(0, basename.length() - extension.length()) : basename;
    }

    private static ImportConfig importByKey(final RuntimeDatabase database, final String importKey) {
        final ImportConfig importConfig = database.imports().get(importKey);
        if (null == importConfig) {
            throw new RuntimeExecutionException("Unable to locate import definition by key '" + importKey + "'");
        }
        return importConfig;
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

    private void performPostCreateMigrationsSetup(final RuntimeDatabase database) {
        if (!database.migrationsEnabled()) {
            return;
        }
        db.setupMigrations();
        performMigration(
                database, database.migrationsAppliedAtCreate() ? MigrationAction.RECORD : MigrationAction.FORCE);
    }

    private void performMigration(final RuntimeDatabase database, final MigrationAction action) {
        final List<String> files = fileResolver.collectFiles(
                database.searchDirs(),
                database.migrationsDirName(),
                "sql",
                database.indexFileName(),
                database.postDbArtifacts(),
                database.preDbArtifacts());

        final Integer versionIndex = releaseVersionIndex(database, files);
        for (int i = 0; i < files.size(); i++) {
            final String filename = files.get(i);
            final String migrationName = basenameWithoutExtension(filename, ".sql");
            final boolean shouldCheck = action == MigrationAction.PERFORM;
            if (!shouldCheck || db.shouldMigrate(database.key(), migrationName)) {
                final boolean shouldRun =
                        action != MigrationAction.RECORD && (null == versionIndex || versionIndex < i);
                if (shouldRun) {
                    runSqlBatch(loadData(database, filename), false);
                }
                db.markMigrationAsRun(database.key(), migrationName);
            }
        }
    }

    private static @Nullable Integer releaseVersionIndex(final RuntimeDatabase database, final List<String> files) {
        if (null == database.version()) {
            return null;
        }
        final String targetRelease = "Release-" + database.version();
        for (int i = 0; i < files.size(); i++) {
            final String migrationName = basenameWithoutExtension(files.get(i), ".sql");
            final int separator = migrationName.indexOf('_');
            if (-1 != separator) {
                final String key = migrationName.substring(separator + 1);
                if (targetRelease.equals(key)) {
                    return i;
                }
            }
        }
        return null;
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
        Collections.reverse(reversedModules);
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
        Collections.reverse(tables);
        for (final String tableName : tables) {
            if (fixtures.containsKey(tableName)) {
                db.execute("DELETE FROM " + tableName, false);
            }
        }

        final List<String> sequences = new ArrayList<>(database.sequenceOrdering(moduleName));
        Collections.reverse(sequences);
        for (final String sequenceName : sequences) {
            if (fixtures.containsKey(sequenceName)) {
                db.updateSequence(sequenceName, 1L);
            }
        }
    }

    private void upFixtures(
            final RuntimeDatabase database, final String moduleName, final Map<String, String> fixtures) {
        for (final String tableName : database.tableOrdering(moduleName)) {
            final String fixture = fixtures.get(tableName);
            if (null != fixture) {
                loadFixture(tableName, loadData(database, fixture));
            }
        }

        for (final String sequenceName : database.sequenceOrdering(moduleName)) {
            final String fixture = fixtures.get(sequenceName);
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

    private static List<Map<String, Object>> toFixtureGroupList(final Object parsed, final String tableName) {
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

    private static Map<String, Object> toStringObjectMap(final Map<?, ?> data) {
        final Map<String, Object> values = new LinkedHashMap<>();
        for (final Map.Entry<?, ?> entry : data.entrySet()) {
            values.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return Map.copyOf(values);
    }

    private static @Nullable Object parseYaml(final String content) {
        final var settings = LoadSettings.builder().setAllowDuplicateKeys(false).build();
        return new Load(settings).loadFromString(content);
    }

    private static String loadData(final RuntimeDatabase database, final String location) {
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

    private static ModuleGroupConfig moduleGroup(final RuntimeDatabase database, final String moduleGroupKey) {
        final ModuleGroupConfig moduleGroup = database.moduleGroups().get(moduleGroupKey);
        if (null == moduleGroup) {
            throw new RuntimeExecutionException(
                    "Unable to locate module group definition by key '" + moduleGroupKey + "'");
        }
        return moduleGroup;
    }

    private static void ensureDatasetExists(final RuntimeDatabase database, final String datasetName) {
        if (!database.datasets().contains(datasetName)) {
            throw new RuntimeExecutionException("Unknown dataset '" + datasetName + "'");
        }
    }

    private enum ModuleMode {
        UP,
        DOWN,
        FINALIZE
    }

    private enum MigrationAction {
        PERFORM,
        RECORD,
        FORCE
    }

    private static final class ResumeState {
        private @Nullable String value;

        private ResumeState(final @Nullable String value) {
            this.value = value;
        }
    }
}
