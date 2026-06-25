package org.realityforge.jdbt.runtime;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.config.FilterPropertyConfig;
import org.realityforge.jdbt.config.ImportConfig;
import org.realityforge.jdbt.config.ModuleGroupConfig;
import org.realityforge.jdbt.db.DatabaseConnection;
import org.realityforge.jdbt.db.DatabaseMetadata;
import org.realityforge.jdbt.db.DbDriver;
import org.realityforge.jdbt.db.QueryResult;
import org.realityforge.jdbt.files.FileResolver;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

public final class RuntimeEngine {
    private static final Pattern ARTIFACT_FILE_PATTERN = Pattern.compile("^zip:([^:]+):(.+)$");
    private static final Pattern GO_SPLIT_PATTERN = Pattern.compile("(?im)(?:^|\\s)GO(?:\\s|$)");
    private static final DateTimeFormatter FIXTURE_DATE_TIME_FORMAT =
            DateTimeFormatter.ofPattern("dd MMM yyyy HH:mm:ss", Locale.ENGLISH);
    private static final DateTimeFormatter FIXTURE_DATE_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter FIXTURE_TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT);

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

    public void create(
            final RuntimeDatabase database,
            final DatabaseConnection target,
            final boolean noCreate,
            final Map<String, String> filterProperties) {
        final var declaredFilters = resolveDeclaredFilterValues(database, filterProperties);
        createDatabaseIfRequired(database, target, noCreate);
        withDatabaseConnection(target, false, () -> {
            for (final var dir : database.preCreateDirs()) {
                processDirSet(database, dir, declaredFilters);
            }
            performCreateAction(database, ModuleMode.UP, declaredFilters);
            performCreateAction(database, ModuleMode.FINALIZE, declaredFilters);
            for (final var dir : database.postCreateDirs()) {
                processDirSet(database, dir, declaredFilters);
            }
            performPostCreateMigrationsSetup(database, declaredFilters);
        });
    }

    public void createWithDataset(
            final RuntimeDatabase database,
            final DatabaseConnection target,
            final boolean noCreate,
            final String datasetName,
            final Map<String, String> filterProperties) {
        final var declaredFilters = resolveDeclaredFilterValues(database, filterProperties);
        ensureDatasetExists(database, datasetName);
        createDatabaseIfRequired(database, target, noCreate);
        withDatabaseConnection(target, false, () -> {
            for (final var dir : database.preCreateDirs()) {
                processDirSet(database, dir, declaredFilters);
            }
            performCreateAction(database, ModuleMode.UP, declaredFilters);
            performPreDatasetHooks(database, datasetName, declaredFilters);
            performLoadDataset(database, datasetName);
            performPostDatasetHooks(database, datasetName, declaredFilters);
            performCreateAction(database, ModuleMode.FINALIZE, declaredFilters);
            for (final var dir : database.postCreateDirs()) {
                processDirSet(database, dir, declaredFilters);
            }
            performPostCreateMigrationsSetup(database, declaredFilters);
        });
    }

    public void drop(
            final RuntimeDatabase database,
            final DatabaseConnection target,
            final Map<String, String> filterProperties) {
        validateProvidedFilterProperties(database, filterProperties);
        withDatabaseConnection(target, true, () -> db.drop(databaseMetadata(database), target));
    }

    public void migrate(
            final RuntimeDatabase database,
            final DatabaseConnection target,
            final Map<String, String> filterProperties) {
        final var declaredFilters = resolveDeclaredFilterValues(database, filterProperties);
        withDatabaseConnection(
                target, false, () -> performMigration(database, MigrationAction.PERFORM, declaredFilters));
    }

    public void upModuleGroup(
            final RuntimeDatabase database,
            final String moduleGroupKey,
            final DatabaseConnection target,
            final Map<String, String> filterProperties) {
        final var declaredFilters = resolveDeclaredFilterValues(database, filterProperties);
        final var moduleGroup = moduleGroup(database, moduleGroupKey);
        withDatabaseConnection(target, false, () -> {
            for (final var moduleName : database.repository().modules()) {
                if (!moduleGroup.modules().contains(moduleName)) {
                    continue;
                }
                createModule(database, moduleName, ModuleMode.UP, declaredFilters);
                createModule(database, moduleName, ModuleMode.FINALIZE, declaredFilters);
            }
        });
    }

    public void downModuleGroup(
            final RuntimeDatabase database,
            final String moduleGroupKey,
            final DatabaseConnection target,
            final Map<String, String> filterProperties) {
        final var declaredFilters = resolveDeclaredFilterValues(database, filterProperties);
        final var moduleGroup = moduleGroup(database, moduleGroupKey);
        withDatabaseConnection(target, false, () -> {
            final var modules = new ArrayList<>(database.repository().modules());
            Collections.reverse(modules);
            for (final var moduleName : modules) {
                if (!moduleGroup.modules().contains(moduleName)) {
                    continue;
                }
                processModule(database, moduleName, ModuleMode.DOWN, declaredFilters);
                final var tables = new ArrayList<>(database.tableOrdering(moduleName));
                Collections.reverse(tables);
                db.dropSchema(database.schemaNameForModule(moduleName), tables);
            }
        });
    }

    public void loadDataset(
            final RuntimeDatabase database,
            final String datasetName,
            final DatabaseConnection target,
            final Map<String, String> filterProperties) {
        final var declaredFilters = resolveDeclaredFilterValues(database, filterProperties);
        ensureDatasetExists(database, datasetName);
        withDatabaseConnection(target, false, () -> {
            performPreDatasetHooks(database, datasetName, declaredFilters);
            performLoadDataset(database, datasetName);
            performPostDatasetHooks(database, datasetName, declaredFilters);
        });
    }

    public void exportFixtures(
            final RuntimeDatabase database,
            final DatabaseConnection target,
            final Path propertiesFile,
            final @Nullable String datasetName,
            final Path outputDirectory,
            final Map<String, String> filterProperties) {
        final var declaredFilters = resolveDeclaredFilterValues(database, filterProperties);
        if (null != datasetName) {
            ensureDatasetExists(database, datasetName);
        }
        final var exportSqlByCleanName = loadExportProperties(propertiesFile);
        final var exportObjects = exportObjects(database);
        validateExportKeys(exportSqlByCleanName, exportObjects);
        withDatabaseConnection(target, false, () -> {
            for (final var exportObject : exportObjects.values()) {
                final var configuredSql = exportSqlByCleanName.get(exportObject.cleanName());
                if (null == configuredSql) {
                    continue;
                }
                final var outputFile = outputDirectory
                        .resolve(exportObject.moduleName())
                        .resolve(exportFixtureDirectory(database, datasetName))
                        .resolve(exportObject.cleanName() + ".yml");
                if (exportObject.sequence()) {
                    exportSequence(exportObject.objectName(), configuredSql, outputFile, declaredFilters);
                } else {
                    exportTable(exportObject.objectName(), configuredSql, outputFile, declaredFilters);
                }
            }
        });
    }

    public void verifyConstraints(
            final RuntimeDatabase database,
            final DatabaseConnection target,
            final List<String> schemas,
            final List<String> checkQueries,
            final Map<String, String> filterProperties) {
        validateProvidedFilterProperties(database, filterProperties);
        withDatabaseConnection(target, false, () -> {
            final var errorMessage = new StringBuilder();
            for (final var schema : schemas) {
                final var result = db.verifySchemaConstraints(schema);
                if (!result.rows().isEmpty()) {
                    errorMessage
                            .append("Failed Constraints:\n")
                            .append(formatQueryRows(result))
                            .append('\n');
                }
            }
            for (final var query : checkQueries) {
                final var result = db.query(query);
                if (!result.rows().isEmpty()) {
                    errorMessage
                            .append("Failed Checks:\n")
                            .append(formatQueryRows(result))
                            .append('\n');
                }
            }
            if (!errorMessage.isEmpty()) {
                throw new RuntimeExecutionException(errorMessage.toString().trim());
            }
        });
    }

    public void databaseImport(
            final RuntimeDatabase database,
            final String importKey,
            final @Nullable String moduleGroupKey,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final @Nullable String resumeAt,
            final Map<String, String> filterProperties) {
        final var declaredFilters = resolveDeclaredFilterValues(database, filterProperties);
        final var importConfig = importByKey(database, importKey);
        final var moduleGroup = null == moduleGroupKey ? null : moduleGroup(database, moduleGroupKey);
        final var metadata = databaseMetadata(database);
        withDatabaseConnection(
                target,
                false,
                () -> performImportAction(
                        database,
                        metadata,
                        importConfig,
                        target,
                        source,
                        true,
                        moduleGroup,
                        resumeAt,
                        declaredFilters));
    }

    public void createByImport(
            final RuntimeDatabase database,
            final String importKey,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final @Nullable String resumeAt,
            final boolean noCreate,
            final Map<String, String> filterProperties) {
        final var declaredFilters = resolveDeclaredFilterValues(database, filterProperties);
        final var importConfig = importByKey(database, importKey);
        if (null == resumeAt) {
            createDatabaseIfRequired(database, target, noCreate);
        }
        final var metadata = databaseMetadata(database);
        withDatabaseConnection(target, false, () -> {
            if (null == resumeAt) {
                for (final var dir : database.preCreateDirs()) {
                    processDirSet(database, dir, declaredFilters);
                }
                performCreateAction(database, ModuleMode.UP, declaredFilters);
            }
            performImportAction(
                    database, metadata, importConfig, target, source, false, null, resumeAt, declaredFilters);
            performCreateAction(database, ModuleMode.FINALIZE, declaredFilters);
            for (final var dir : database.postCreateDirs()) {
                processDirSet(database, dir, declaredFilters);
            }
            performPostCreateMigrationsSetup(database, declaredFilters);
        });
    }

    private void performImportAction(
            final RuntimeDatabase database,
            final DatabaseMetadata metadata,
            final ImportConfig importConfig,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final boolean shouldPerformDelete,
            final @Nullable ModuleGroupConfig moduleGroup,
            final @Nullable String resumeAtInput,
            final Map<String, String> declaredFilters) {
        final var resumeAt = new ResumeState(resumeAtInput);
        final var selectedModules = selectedImportModules(database, importConfig, moduleGroup);

        if (null == moduleGroup && null == resumeAt.value) {
            for (final var dir : importConfig.preImportDirs()) {
                processImportDirSet(database, dir, target, source, declaredFilters);
            }
        }

        for (final var moduleName : selectedModules) {
            verifyNoUnexpectedImportFiles(database, moduleName, importConfig.dir());
        }

        if (shouldPerformDelete && null != moduleGroup && null == resumeAt.value) {
            final var deleteOrder = new ArrayList<String>();
            for (final var moduleName : selectedModules) {
                final var tables = new ArrayList<>(database.tableOrdering(moduleName));
                Collections.reverse(tables);
                deleteOrder.addAll(tables);
            }
            for (final var table : deleteOrder) {
                db.execute("DELETE FROM " + table, false);
            }
        }

        final var deleteWithinModule = shouldPerformDelete && null == moduleGroup;
        for (final var moduleName : selectedModules) {
            importModule(
                    database,
                    metadata,
                    importConfig,
                    target,
                    source,
                    moduleName,
                    deleteWithinModule,
                    resumeAt,
                    declaredFilters);
        }

        if (null != resumeAt.value) {
            throw new RuntimeExecutionException(
                    "Partial import unable to be completed as bad table name supplied " + resumeAt.value);
        }

        if (null == moduleGroup) {
            for (final var dir : importConfig.postImportDirs()) {
                processImportDirSet(database, dir, target, source, declaredFilters);
            }
        }
        db.postDatabaseImport(metadata, importConfig);
    }

    private static List<String> selectedImportModules(
            final RuntimeDatabase database,
            final ImportConfig importConfig,
            final @Nullable ModuleGroupConfig moduleGroup) {
        final var selectedModules = new ArrayList<String>();
        for (final var moduleName : importConfig.modules()) {
            if (!database.repository().modules().contains(moduleName)) {
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
            final DatabaseMetadata metadata,
            final ImportConfig importConfig,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final String moduleName,
            final boolean shouldPerformDelete,
            final ResumeState resumeAt,
            final Map<String, String> declaredFilters) {
        final var orderedTables = new ArrayList<>(database.tableOrdering(moduleName));
        final var orderedSequences = new ArrayList<>(database.sequenceOrdering(moduleName));

        final var tables = new ArrayList<String>();
        for (final var table : orderedTables) {
            final var fixture = fileResolver.findFileInModule(
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

        if (shouldPerformDelete && null == resumeAt.value) {
            final var deleteOrder = new ArrayList<>(tables);
            Collections.reverse(deleteOrder);
            for (final var table : deleteOrder) {
                db.execute("DELETE FROM " + table, false);
            }
        }

        for (final var table : tables) {
            final var cleanName = cleanObjectName(table);
            if (cleanName.equals(resumeAt.value)) {
                db.execute("DELETE FROM " + table, false);
                resumeAt.value = null;
            }
            if (null == resumeAt.value) {
                db.preTableImport(metadata, importConfig, table);
                try {
                    performImport(database, importConfig, target, source, moduleName, table, declaredFilters);
                } catch (final RuntimeException e) {
                    throw importFailure(cleanName, e);
                }
                db.postTableImport(metadata, importConfig, table);
            }
        }

        for (final var sequence : orderedSequences) {
            if (cleanObjectName(sequence).equals(resumeAt.value)) {
                resumeAt.value = null;
            }
            if (null == resumeAt.value) {
                final var cleanName = cleanObjectName(sequence);
                try {
                    performSequenceImport(
                            database, importConfig, target, source, moduleName, sequence, declaredFilters);
                } catch (final RuntimeException e) {
                    throw importFailure(cleanName, e);
                }
            }
        }

        if (null == resumeAt.value) {
            db.postDataModuleImport(metadata, importConfig, moduleName, orderedTables);
        }
    }

    private static RuntimeExecutionException importFailure(final String cleanObjectName, final RuntimeException cause) {
        return new RuntimeExecutionException(
                "Problem importing "
                        + cleanObjectName
                        + ". Fix the problem and retry import specifying --resume-at="
                        + cleanObjectName
                        + " to re-attempt import from current position.",
                cause);
    }

    private void performImport(
            final RuntimeDatabase database,
            final ImportConfig importConfig,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final String moduleName,
            final String tableName,
            final Map<String, String> declaredFilters) {
        final var fixtureFile = fileResolver.findFileInModule(
                database.searchDirs(),
                moduleName,
                importConfig.dir(),
                tableName,
                "yml",
                database.postDbArtifacts(),
                database.preDbArtifacts());
        final var sqlFile = fileResolver.findFileInModule(
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
            runImportSql(tableName, loadData(database, sqlFile), target.database(), source.database(), declaredFilters);
        } else {
            performStandardImport(tableName, target.database(), source.database(), declaredFilters);
        }
    }

    private void performSequenceImport(
            final RuntimeDatabase database,
            final ImportConfig importConfig,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final String moduleName,
            final String sequenceName,
            final Map<String, String> declaredFilters) {
        final var fixtureFile = fileResolver.findFileInModule(
                database.searchDirs(),
                moduleName,
                importConfig.dir(),
                sequenceName,
                "yml",
                database.postDbArtifacts(),
                database.preDbArtifacts());
        final var sqlFile = fileResolver.findFileInModule(
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
            runImportSql(
                    sequenceName, loadData(database, sqlFile), target.database(), source.database(), declaredFilters);
        } else {
            runImportSql(
                    sequenceName,
                    db.generateStandardSequenceImportSql(sequenceName, target.database(), source.database()),
                    target.database(),
                    source.database(),
                    declaredFilters);
        }
    }

    private void performStandardImport(
            final String tableName,
            final String targetDatabase,
            final String sourceDatabase,
            final Map<String, String> declaredFilters) {
        final var columns = db.columnNamesForTable(tableName);
        runImportSql(
                tableName,
                db.generateStandardImportSql(tableName, targetDatabase, sourceDatabase, columns),
                targetDatabase,
                sourceDatabase,
                declaredFilters);
    }

    private void runImportSql(
            final @Nullable String tableName,
            final String sql,
            final String targetDatabase,
            final String sourceDatabase,
            final Map<String, String> declaredFilters) {
        var effectiveSql = db.supportsImportAssertFilters() ? SqlServerImportAssertExpander.expand(sql) : sql;
        effectiveSql = applyDeclaredFilterProperties(effectiveSql, declaredFilters);
        if (null != tableName) {
            effectiveSql = effectiveSql.replace("__TABLE__", tableName);
        }
        effectiveSql = effectiveSql.replace("__SOURCE__", sourceDatabase);
        effectiveSql = effectiveSql.replace("__TARGET__", targetDatabase);
        runSqlBatch(effectiveSql, true);
    }

    private void processImportDirSet(
            final RuntimeDatabase database,
            final String dir,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final Map<String, String> declaredFilters) {
        final var files = fileResolver.collectFiles(
                database.searchDirs(),
                dir,
                "sql",
                database.indexFileName(),
                database.postDbArtifacts(),
                database.preDbArtifacts());
        for (final var file : files) {
            runImportSql(null, loadData(database, file), target.database(), source.database(), declaredFilters);
        }
    }

    private static void verifyNoUnexpectedImportFiles(
            final RuntimeDatabase database, final String moduleName, final String importDir) {
        final var expected = new ArrayList<>();
        final var orderedElements = new ArrayList<>(database.tableOrdering(moduleName));
        orderedElements.addAll(database.sequenceOrdering(moduleName));
        for (final var table : orderedElements) {
            final var clean = cleanObjectName(table);
            expected.add(clean + ".yml");
            expected.add(clean + ".sql");
        }

        final var additional = new ArrayList<>();
        for (final var searchDir : database.searchDirs()) {
            final var directory = searchDir.resolve(moduleName).resolve(importDir);
            if (!Files.isDirectory(directory)) {
                continue;
            }
            try (var stream = Files.list(directory)) {
                stream.filter(Files::isRegularFile)
                        .filter(file -> {
                            final var fileName = file.getFileName().toString();
                            return fileName.endsWith(".yml") || fileName.endsWith(".sql");
                        })
                        .forEach(file -> {
                            final var fileName = file.getFileName().toString();
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
        final var slash = Math.max(value.lastIndexOf('/'), value.lastIndexOf('\\'));
        final var basename = -1 == slash ? value : value.substring(slash + 1);
        return basename.endsWith(extension) ? basename.substring(0, basename.length() - extension.length()) : basename;
    }

    private static ImportConfig importByKey(final RuntimeDatabase database, final String importKey) {
        final var importConfig = database.imports().get(importKey);
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
            final var metadata = databaseMetadata(database);
            db.drop(metadata, target);
            db.createDatabase(metadata, target);
        });
    }

    private static DatabaseMetadata databaseMetadata(final RuntimeDatabase database) {
        return new DatabaseMetadata(
                database.key(),
                database.version(),
                database.schemaHash(),
                database.dataPath(),
                database.logPath(),
                database.forceDrop(),
                database.deleteBackupHistory(),
                database.reindexOnImport(),
                database.shrinkOnImport());
    }

    private void performPostCreateMigrationsSetup(
            final RuntimeDatabase database, final Map<String, String> declaredFilters) {
        if (!database.migrationsEnabled()) {
            return;
        }
        db.setupMigrations();
        performMigration(
                database,
                database.migrationsAppliedAtCreate() ? MigrationAction.RECORD : MigrationAction.FORCE,
                declaredFilters);
    }

    private void performMigration(
            final RuntimeDatabase database, final MigrationAction action, final Map<String, String> declaredFilters) {
        final var files = fileResolver.collectFiles(
                database.searchDirs(),
                database.migrationsDirName(),
                "sql",
                database.indexFileName(),
                database.postDbArtifacts(),
                database.preDbArtifacts());

        final var versionIndex = releaseVersionIndex(database, files);
        for (int i = 0; i < files.size(); i++) {
            final var filename = files.get(i);
            final var migrationName = basenameWithoutExtension(filename, ".sql");
            final var shouldCheck = action == MigrationAction.PERFORM;
            if (!shouldCheck || db.shouldMigrate(database.key(), migrationName)) {
                final var shouldRun = action != MigrationAction.RECORD && (null == versionIndex || versionIndex < i);
                if (shouldRun) {
                    runSqlBatch(applyDeclaredFilterProperties(loadData(database, filename), declaredFilters), false);
                }
                db.markMigrationAsRun(database.key(), migrationName);
            }
        }
    }

    private static @Nullable Integer releaseVersionIndex(final RuntimeDatabase database, final List<String> files) {
        if (null == database.version()) {
            return null;
        }
        final var targetRelease = "Release-" + database.version();
        for (int i = 0; i < files.size(); i++) {
            final var migrationName = basenameWithoutExtension(files.get(i), ".sql");
            final var separator = migrationName.indexOf('_');
            if (-1 != separator) {
                final var key = migrationName.substring(separator + 1);
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

    private void performCreateAction(
            final RuntimeDatabase database, final ModuleMode mode, final Map<String, String> declaredFilters) {
        for (final var moduleName : database.repository().modules()) {
            createModule(database, moduleName, mode, declaredFilters);
        }
    }

    private void createModule(
            final RuntimeDatabase database,
            final String moduleName,
            final ModuleMode mode,
            final Map<String, String> declaredFilters) {
        if (ModuleMode.UP == mode) {
            db.createSchema(database.schemaNameForModule(moduleName));
        }
        processModule(database, moduleName, mode, declaredFilters);
    }

    private void processModule(
            final RuntimeDatabase database,
            final String moduleName,
            final ModuleMode mode,
            final Map<String, String> declaredFilters) {
        final var dirs =
                switch (mode) {
                    case UP -> database.upDirs();
                    case DOWN -> database.downDirs();
                    case FINALIZE -> database.finalizeDirs();
                };
        for (final var dir : dirs) {
            processDirSet(database, moduleName + '/' + dir, declaredFilters);
        }
        if (ModuleMode.UP == mode) {
            loadFixturesFromDir(database, moduleName, database.fixtureDirName());
        }
    }

    private void processDirSet(
            final RuntimeDatabase database, final String dir, final Map<String, String> declaredFilters) {
        final var files = fileResolver.collectFiles(
                database.searchDirs(),
                dir,
                "sql",
                database.indexFileName(),
                database.postDbArtifacts(),
                database.preDbArtifacts());
        for (final var file : files) {
            final var sql = loadData(database, file);
            runSqlBatch(applyDeclaredFilterProperties(sql, declaredFilters), false);
        }
    }

    private void performPreDatasetHooks(
            final RuntimeDatabase database, final String datasetName, final Map<String, String> declaredFilters) {
        for (final var preDir : database.preDatasetDirs()) {
            processDirSet(database, database.datasetsDirName() + '/' + datasetName + '/' + preDir, declaredFilters);
        }
    }

    private void performPostDatasetHooks(
            final RuntimeDatabase database, final String datasetName, final Map<String, String> declaredFilters) {
        for (final var postDir : database.postDatasetDirs()) {
            processDirSet(database, database.datasetsDirName() + '/' + datasetName + '/' + postDir, declaredFilters);
        }
    }

    private void performLoadDataset(final RuntimeDatabase database, final String datasetName) {
        final var subdir = database.datasetsDirName() + '/' + datasetName;
        final var fixtures = new LinkedHashMap<String, String>();
        for (final var moduleName : database.repository().modules()) {
            fixtures.putAll(collectFixtures(database, moduleName, subdir));
        }

        final var modules = new ArrayList<>(database.repository().modules());
        final var reversedModules = new ArrayList<>(modules);
        Collections.reverse(reversedModules);
        for (final var moduleName : reversedModules) {
            downFixtures(database, moduleName, fixtures);
        }
        for (final var moduleName : modules) {
            upFixtures(database, moduleName, fixtures);
        }
    }

    private static Map<String, String> loadExportProperties(final Path propertiesFile) {
        final var properties = new DuplicateDetectingProperties(propertiesFile);
        try (var reader = new InputStreamReader(Files.newInputStream(propertiesFile), StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to read export properties " + propertiesFile, ioe);
        }
        return properties.entries();
    }

    private static Map<String, ExportObject> exportObjects(final RuntimeDatabase database) {
        final var objects = new LinkedHashMap<String, ExportObject>();
        for (final var moduleName : database.repository().modules()) {
            for (final var tableName : database.tableOrdering(moduleName)) {
                addExportObject(objects, new ExportObject(moduleName, tableName, false, cleanObjectName(tableName)));
            }
            for (final var sequenceName : database.sequenceOrdering(moduleName)) {
                addExportObject(
                        objects, new ExportObject(moduleName, sequenceName, true, cleanObjectName(sequenceName)));
            }
        }
        return Collections.unmodifiableMap(objects);
    }

    private static void addExportObject(final Map<String, ExportObject> objects, final ExportObject object) {
        final var previous = objects.putIfAbsent(object.cleanName(), object);
        if (null != previous) {
            throw new RuntimeExecutionException("Duplicate clean fixture export key '"
                    + object.cleanName()
                    + "' for "
                    + previous.objectName()
                    + " and "
                    + object.objectName());
        }
    }

    private static void validateExportKeys(
            final Map<String, String> exportSqlByCleanName, final Map<String, ExportObject> exportObjects) {
        for (final var cleanName : exportSqlByCleanName.keySet()) {
            if (!exportObjects.containsKey(cleanName)) {
                throw new RuntimeExecutionException(
                        "Export properties contain unknown table or sequence key '" + cleanName + "'");
            }
        }
    }

    private static String exportFixtureDirectory(final RuntimeDatabase database, final @Nullable String datasetName) {
        return null == datasetName ? database.fixtureDirName() : database.datasetsDirName() + '/' + datasetName;
    }

    private static String formatQueryRows(final QueryResult result) {
        final var output = new StringBuilder();
        for (final var row : result.rows()) {
            output.append('\t');
            for (int i = 0; i < result.columnLabels().size(); i++) {
                if (0 != i) {
                    output.append(", ");
                }
                output.append(result.columnLabels().get(i)).append('=').append(row.get(i));
            }
            output.append('\n');
        }
        return output.toString();
    }

    private void exportTable(
            final String tableName,
            final String configuredSql,
            final Path outputFile,
            final Map<String, String> declaredFilters) {
        final var sql = configuredSql.trim().isEmpty()
                ? defaultTableExportSql(tableName)
                : applyDeclaredFilterProperties(configuredSql, declaredFilters);
        final var result = db.query(sql);
        validateTableQueryResult(tableName, result.columnLabels());
        writeText(outputFile, tableYaml(result));
    }

    private void exportSequence(
            final String sequenceName,
            final String configuredSql,
            final Path outputFile,
            final Map<String, String> declaredFilters) {
        final var sql = configuredSql.trim().isEmpty()
                ? db.generateDefaultSequenceExportSql(sequenceName)
                : applyDeclaredFilterProperties(configuredSql, declaredFilters);
        final var result = db.query(sql);
        if (1 != result.columnLabels().size() || 1 != result.rows().size()) {
            throw new RuntimeExecutionException("Sequence export SQL for "
                    + sequenceName
                    + " must return exactly one row with exactly one column.");
        }
        final var value = result.rows().get(0).get(0);
        if (null == value) {
            throw new RuntimeExecutionException("Sequence export SQL for " + sequenceName + " returned null.");
        }
        final var normalizedValue = normalizeFixtureValue(value);
        if (null == normalizedValue) {
            throw new RuntimeExecutionException("Sequence export SQL for " + sequenceName + " returned null.");
        }
        writeText(outputFile, yamlScalar(normalizedValue) + '\n');
    }

    private String defaultTableExportSql(final String tableName) {
        final var primaryKeys = db.primaryKeyColumnNamesForTable(tableName);
        if (primaryKeys.isEmpty()) {
            throw new RuntimeExecutionException("Unable to generate default fixture export SQL for " + tableName
                    + " because it has no primary key.");
        }
        return "SELECT * FROM "
                + tableName
                + " ORDER BY "
                + String.join(
                        ", ",
                        primaryKeys.stream().map(column -> column + " ASC").toList());
    }

    private static void validateTableQueryResult(final String tableName, final List<String> columnLabels) {
        final var seen = new HashSet<String>();
        for (final var columnLabel : columnLabels) {
            if (columnLabel.trim().isEmpty()) {
                throw new RuntimeExecutionException(
                        "Fixture export SQL for " + tableName + " returned blank column label.");
            }
            if (!seen.add(columnLabel)) {
                throw new RuntimeExecutionException("Fixture export SQL for " + tableName
                        + " returned duplicate column label '" + columnLabel + "'.");
            }
        }
    }

    private static String tableYaml(final QueryResult result) {
        if (result.rows().isEmpty()) {
            return "{}\n";
        }
        final var output = new StringBuilder();
        for (int rowIndex = 0; rowIndex < result.rows().size(); rowIndex++) {
            final var row = result.rows().get(rowIndex);
            final var rowOutput = new StringBuilder();
            for (int columnIndex = 0; columnIndex < result.columnLabels().size(); columnIndex++) {
                final var value = normalizeFixtureValue(row.get(columnIndex));
                if (null != value) {
                    rowOutput
                            .append("  ")
                            .append(result.columnLabels().get(columnIndex))
                            .append(": ")
                            .append(yamlScalar(value))
                            .append('\n');
                }
            }
            if (rowOutput.isEmpty()) {
                output.append('r').append(rowIndex + 1).append(": {}\n");
            } else {
                output.append('r').append(rowIndex + 1).append(":\n").append(rowOutput);
            }
        }
        return output.toString();
    }

    private static @Nullable Object normalizeFixtureValue(final @Nullable Object value) {
        if (null == value) {
            return null;
        }
        if (value instanceof Timestamp timestamp) {
            return FIXTURE_DATE_TIME_FORMAT.format(timestamp.toLocalDateTime());
        }
        if (value instanceof java.sql.Date date) {
            return FIXTURE_DATE_FORMAT.format(date.toLocalDate());
        }
        if (value instanceof Time time) {
            return FIXTURE_TIME_FORMAT.format(time.toLocalTime());
        }
        if (value instanceof LocalDateTime localDateTime) {
            return FIXTURE_DATE_TIME_FORMAT.format(localDateTime);
        }
        if (value instanceof LocalDate localDate) {
            return FIXTURE_DATE_FORMAT.format(localDate);
        }
        if (value instanceof LocalTime localTime) {
            return FIXTURE_TIME_FORMAT.format(localTime);
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return FIXTURE_DATE_TIME_FORMAT.format(offsetDateTime.toLocalDateTime());
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return FIXTURE_DATE_TIME_FORMAT.format(zonedDateTime.toLocalDateTime());
        }
        if (value instanceof Instant instant) {
            return FIXTURE_DATE_TIME_FORMAT.format(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
        }
        if (value instanceof java.util.Date date) {
            return FIXTURE_DATE_TIME_FORMAT.format(LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC));
        }
        return value;
    }

    private static String yamlScalar(final Object value) {
        if (value instanceof Number || value instanceof Boolean) {
            return String.valueOf(value);
        }
        return '"' + escapeYamlString(String.valueOf(value)) + '"';
    }

    private static String escapeYamlString(final String value) {
        final var output = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            final var c = value.charAt(i);
            switch (c) {
                case '\\' -> output.append("\\\\");
                case '"' -> output.append("\\\"");
                case '\n' -> output.append("\\n");
                case '\r' -> output.append("\\r");
                case '\t' -> output.append("\\t");
                default -> output.append(c);
            }
        }
        return output.toString();
    }

    private static void writeText(final Path outputFile, final String content) {
        try {
            final var parent = outputFile.getParent();
            if (null != parent) {
                Files.createDirectories(parent);
            }
            Files.writeString(outputFile, content, StandardCharsets.UTF_8);
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to write fixture export " + outputFile, ioe);
        }
    }

    private void loadFixturesFromDir(final RuntimeDatabase database, final String moduleName, final String subdir) {
        final var fixtures = collectFixtures(database, moduleName, subdir);
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
        final var tables = new ArrayList<>(database.tableOrdering(moduleName));
        Collections.reverse(tables);
        for (final var tableName : tables) {
            if (fixtures.containsKey(tableName)) {
                db.execute("DELETE FROM " + tableName, false);
            }
        }

        final var sequences = new ArrayList<>(database.sequenceOrdering(moduleName));
        Collections.reverse(sequences);
        for (final var sequenceName : sequences) {
            if (fixtures.containsKey(sequenceName)) {
                db.updateSequence(sequenceName, 1L);
            }
        }
    }

    private void upFixtures(
            final RuntimeDatabase database, final String moduleName, final Map<String, String> fixtures) {
        for (final var tableName : database.tableOrdering(moduleName)) {
            final var fixture = fixtures.get(tableName);
            if (null != fixture) {
                loadFixture(tableName, loadData(database, fixture));
            }
        }

        for (final var sequenceName : database.sequenceOrdering(moduleName)) {
            final var fixture = fixtures.get(sequenceName);
            if (null != fixture) {
                loadSequenceFixture(sequenceName, loadData(database, fixture));
            }
        }
    }

    private void loadFixture(final String tableName, final String content) {
        final var parsed = parseYaml(content);
        if (null == parsed) {
            return;
        }

        final var fixtureGroups = toFixtureGroupList(parsed, tableName);
        db.preFixtureImport(tableName);
        for (final var fixtureGroup : fixtureGroups) {
            for (final var fixture : fixtureGroup.entrySet()) {
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
        final var parsed = parseYaml(content);
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
            final var groups = new ArrayList<Map<String, Object>>(list.size());
            for (final var entry : list) {
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
        final var values = new LinkedHashMap<String, Object>();
        for (final var entry : data.entrySet()) {
            values.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return Map.copyOf(values);
    }

    private static @Nullable Object parseYaml(final String content) {
        final var settings = LoadSettings.builder().setAllowDuplicateKeys(false).build();
        return new Load(settings).loadFromString(content);
    }

    private static String loadData(final RuntimeDatabase database, final String location) {
        final var matcher = ARTIFACT_FILE_PATTERN.matcher(location);
        if (matcher.matches()) {
            final var artifactId = matcher.group(1);
            final var path = matcher.group(2);
            final var artifact = database.artifactById(artifactId);
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
        final var normalizedSql = sql.replace("\r", "");
        for (final var batch : GO_SPLIT_PATTERN.splitAsStream(normalizedSql).toList()) {
            if (!batch.trim().isEmpty()) {
                db.execute(batch, executeInControlDatabase);
            }
        }
    }

    private static Map<String, String> resolveDeclaredFilterValues(
            final RuntimeDatabase database, final Map<String, String> providedFilterProperties) {
        final var configuredProperties = database.filterProperties();
        validateProvidedFilterProperties(database, providedFilterProperties);

        final var resolvedValuesByPattern = new LinkedHashMap<String, String>();
        for (final var entry : configuredProperties.entrySet()) {
            final var key = entry.getKey();
            final var config = entry.getValue();
            final var providedValue = providedFilterProperties.get(key);
            final var value = null != providedValue ? providedValue : config.defaultValue();
            if (null == value) {
                throw new RuntimeExecutionException(
                        "Filter property '" + key + "' is required and has no configured default value.");
            }
            validateSupportedFilterValue(key, value, config);
            resolvedValuesByPattern.put(config.pattern(), value);
        }

        return Collections.unmodifiableMap(new LinkedHashMap<>(resolvedValuesByPattern));
    }

    private static void validateProvidedFilterProperties(
            final RuntimeDatabase database, final Map<String, String> providedFilterProperties) {
        final var configuredProperties = database.filterProperties();

        for (final var reservedKey : List.of("sourceDatabase", "targetDatabase", "table")) {
            if (providedFilterProperties.containsKey(reservedKey)) {
                throw new RuntimeExecutionException(
                        "Filter property '" + reservedKey + "' is tool-provided and can not be overridden.");
            }
        }

        for (final var entry : providedFilterProperties.entrySet()) {
            final var key = entry.getKey();
            final var value = entry.getValue();
            final var config = configuredProperties.get(key);
            if (null == config) {
                throw new RuntimeExecutionException(
                        "Filter property '" + key + "' is not declared in jdbt.yml filterProperties.");
            }
            validateSupportedFilterValue(key, value, config);
        }
    }

    private static void validateSupportedFilterValue(
            final String propertyKey, final String value, final FilterPropertyConfig config) {
        if (!config.supportedValues().isEmpty() && !config.supportedValues().contains(value)) {
            throw new RuntimeExecutionException("Filter property '"
                    + propertyKey
                    + "' has unsupported value '"
                    + value
                    + "'. Supported values: "
                    + config.supportedValues()
                    + '.');
        }
    }

    private static String applyDeclaredFilterProperties(final String sql, final Map<String, String> declaredFilters) {
        var output = sql;
        for (final var entry : declaredFilters.entrySet()) {
            output = output.replace(entry.getKey(), entry.getValue());
        }
        return output;
    }

    private static ModuleGroupConfig moduleGroup(final RuntimeDatabase database, final String moduleGroupKey) {
        final var moduleGroup = database.moduleGroups().get(moduleGroupKey);
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

    private record ExportObject(String moduleName, String objectName, boolean sequence, String cleanName) {}

    private static final class DuplicateDetectingProperties extends Properties {
        private final Path propertiesFile;
        private final LinkedHashMap<String, String> entries = new LinkedHashMap<>();

        private DuplicateDetectingProperties(final Path propertiesFile) {
            this.propertiesFile = propertiesFile;
        }

        @Override
        public synchronized Object put(final Object key, final Object value) {
            final var keyText = String.valueOf(key);
            if (entries.containsKey(keyText)) {
                throw new RuntimeExecutionException(
                        "Duplicate export properties key '" + keyText + "' in " + propertiesFile);
            }
            entries.put(keyText, String.valueOf(value));
            return super.put(key, value);
        }

        private Map<String, String> entries() {
            return Collections.unmodifiableMap(new LinkedHashMap<>(entries));
        }
    }

    private static final class ResumeState {
        private @Nullable String value;

        private ResumeState(final @Nullable String value) {
            this.value = value;
        }
    }
}
