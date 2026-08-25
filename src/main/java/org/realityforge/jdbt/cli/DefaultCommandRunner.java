package org.realityforge.jdbt.cli;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.db.DatabaseConnection;
import org.realityforge.jdbt.db.DbDriverFactory;
import org.realityforge.jdbt.db.sqlserver.SqlServerDatabaseStatisticsExporter;
import org.realityforge.jdbt.files.FileResolver;
import org.realityforge.jdbt.packaging.DatabaseDataPackager;
import org.realityforge.jdbt.packaging.DeterministicZipPackager;
import org.realityforge.jdbt.runtime.ImportTimingRecorder;
import org.realityforge.jdbt.runtime.RuntimeEngine;
import org.realityforge.jdbt.runtime.RuntimeExecutionException;
import org.realityforge.jdbt.runtime.StandardImportEmitter;

final class DefaultCommandRunner implements CommandRunner {
    private final ProjectRuntimeLoader projectRuntimeLoader;
    private final DbDriverFactory dbDriverFactory;
    private final FileResolver fileResolver;

    DefaultCommandRunner(final ProjectRuntimeLoader projectRuntimeLoader) {
        this(projectRuntimeLoader, new DbDriverFactory(), new FileResolver());
    }

    DefaultCommandRunner(
            final ProjectRuntimeLoader projectRuntimeLoader,
            final DbDriverFactory dbDriverFactory,
            final FileResolver fileResolver) {
        this.projectRuntimeLoader = projectRuntimeLoader;
        this.dbDriverFactory = dbDriverFactory;
        this.fileResolver = fileResolver;
    }

    @Override
    public void validateProject() {
        projectRuntimeLoader.validate();
    }

    @Override
    public void status(final String driver) {
        final var runtime = projectRuntimeLoader.load();
        final var runtimeEngine = runtimeEngine(driver);
        System.out.print(runtimeEngine.status(runtime.database()));
    }

    @Override
    public void create(
            final String driver,
            final DatabaseConnection target,
            final boolean noCreate,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        runtimeEngine(driver).create(runtime.database(), target, noCreate, filterProperties);
    }

    @Override
    public void createWithDataset(
            final String driver,
            final DatabaseConnection target,
            final boolean noCreate,
            final String dataset,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        runtimeEngine(driver).createWithDataset(runtime.database(), target, noCreate, dataset, filterProperties);
    }

    @Override
    public void drop(final String driver, final DatabaseConnection target, final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        runtimeEngine(driver).drop(runtime.database(), target, filterProperties);
    }

    @Override
    public void migrate(
            final String driver, final DatabaseConnection target, final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        runtimeEngine(driver).migrate(runtime.database(), target, filterProperties);
    }

    @Override
    public void databaseImport(
            final String driver,
            final @Nullable String importKey,
            final @Nullable String moduleGroup,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final @Nullable String resumeAt,
            final @Nullable Path timingOutput,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        final var resolvedImport = resolveImportKey(runtime, importKey);
        withImportTiming(
                runtime,
                timingOutput,
                timing -> runtimeEngine(driver, timing)
                        .databaseImport(
                                runtime.database(),
                                resolvedImport,
                                moduleGroup,
                                target,
                                source,
                                resumeAt,
                                filterProperties));
    }

    @Override
    public void createByImport(
            final String driver,
            final @Nullable String importKey,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final @Nullable String resumeAt,
            final boolean noCreate,
            final @Nullable Path timingOutput,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        final var resolvedImport = resolveImportKey(runtime, importKey);
        withImportTiming(
                runtime,
                timingOutput,
                timing -> runtimeEngine(driver, timing)
                        .createByImport(
                                runtime.database(),
                                resolvedImport,
                                target,
                                source,
                                resumeAt,
                                noCreate,
                                filterProperties));
    }

    @Override
    public void loadDataset(
            final String driver,
            final String dataset,
            final DatabaseConnection target,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        runtimeEngine(driver).loadDataset(runtime.database(), dataset, target, filterProperties);
    }

    @Override
    public void upModuleGroup(
            final String driver,
            final String moduleGroup,
            final DatabaseConnection target,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        runtimeEngine(driver).upModuleGroup(runtime.database(), moduleGroup, target, filterProperties);
    }

    @Override
    public void downModuleGroup(
            final String driver,
            final String moduleGroup,
            final DatabaseConnection target,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        runtimeEngine(driver).downModuleGroup(runtime.database(), moduleGroup, target, filterProperties);
    }

    @Override
    public void packageData(final Path outputFile) {
        final var runtime = projectRuntimeLoader.load();
        final Path stagingDirectory;
        try {
            stagingDirectory = Files.createTempDirectory("jdbt-package-data-");
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed creating package staging directory", ioe);
        }
        try {
            new DatabaseDataPackager(fileResolver)
                    .packageDatabaseData(runtime.database(), stagingDirectory.resolve("data"));
            new DeterministicZipPackager().write(stagingDirectory, outputFile);
        } finally {
            deleteRecursively(stagingDirectory);
        }
    }

    @Override
    public void emitStandardImports(
            final @Nullable String importKey, final @Nullable Path outputDirectory, final boolean replace) {
        final var runtime = projectRuntimeLoader.load();
        final var resolvedImport = resolveImportKey(runtime, importKey);
        new StandardImportEmitter(dbDriverFactory.create("sqlserver"))
                .emit(runtime.database(), resolvedImport, outputDirectory, replace);
    }

    @Override
    public void verifyConstraints(
            final String driver,
            final DatabaseConnection target,
            final List<String> schemas,
            final List<String> checkQueries,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        runtimeEngine(driver).verifyConstraints(runtime.database(), target, schemas, checkQueries, filterProperties);
    }

    @Override
    public void exportFixtures(
            final String driver,
            final DatabaseConnection target,
            final Path propertiesFile,
            final @Nullable String dataset,
            final @Nullable Path outputDirectory,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        final var resolvedOutputDirectory = null == outputDirectory ? runtime.projectDirectory() : outputDirectory;
        runtimeEngine(driver)
                .exportFixtures(
                        runtime.database(), target, propertiesFile, dataset, resolvedOutputDirectory, filterProperties);
    }

    @Override
    public void exportDatabaseStatistics(final String driver, final DatabaseConnection target, final Path outputFile) {
        if (!"sqlserver".equalsIgnoreCase(driver)) {
            throw new RuntimeExecutionException(
                    "Database statistics export only supports the sqlserver driver, not '" + driver + "'");
        }
        final var runtime = projectRuntimeLoader.load();
        final var count = new SqlServerDatabaseStatisticsExporter(dbDriverFactory.create("sqlserver"))
                .export(runtime.database().repository(), target, outputFile);
        System.out.println("Exported " + count + " database statistics to "
                + outputFile.toAbsolutePath().normalize());
    }

    private static String resolveImportKey(
            final ProjectRuntimeLoader.LoadedRuntime runtime, final @Nullable String importKey) {
        if (null != importKey) {
            return importKey;
        }
        final var defaultImport = runtime.defaults().defaultImport();
        if (!runtime.database().imports().containsKey(defaultImport)) {
            throw new RuntimeExecutionException("Unable to locate import definition by key '" + defaultImport + "'");
        }
        return defaultImport;
    }

    private RuntimeEngine runtimeEngine(final String driver) {
        final var dbDriver = dbDriverFactory.create(driver);
        return new RuntimeEngine(dbDriver, fileResolver);
    }

    private RuntimeEngine runtimeEngine(final String driver, final ImportTimingRecorder timing) {
        final var dbDriver = dbDriverFactory.create(driver);
        return new RuntimeEngine(dbDriver, fileResolver, System.out::println, timing);
    }

    private static void withImportTiming(
            final ProjectRuntimeLoader.LoadedRuntime runtime,
            final @Nullable Path timingOutput,
            final Consumer<ImportTimingRecorder> action) {
        if (null == timingOutput) {
            action.accept(ImportTimingRecorder.disabled());
            return;
        }
        final var resolvedOutput = (timingOutput.isAbsolute()
                        ? timingOutput
                        : runtime.projectDirectory().resolve(timingOutput))
                .normalize();
        try (var timing = ImportTimingRecorder.open(resolvedOutput)) {
            action.accept(timing);
        }
    }

    private static void deleteRecursively(final Path directory) {
        if (!Files.exists(directory)) {
            return;
        }
        try (var stream = Files.walk(directory)) {
            stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (final IOException ioe) {
                    throw new UncheckedIOException("Failed deleting " + path, ioe);
                }
            });
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed deleting staging directory " + directory, ioe);
        }
    }
}
