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
import org.realityforge.jdbt.db.DbDriver;
import org.realityforge.jdbt.db.sqlserver.SqlServerDatabaseStatisticsExporter;
import org.realityforge.jdbt.db.sqlserver.SqlServerDbDriver;
import org.realityforge.jdbt.files.FileResolver;
import org.realityforge.jdbt.packaging.DatabaseDataPackager;
import org.realityforge.jdbt.packaging.DeterministicZipPackager;
import org.realityforge.jdbt.runtime.ImportTimingRecorder;
import org.realityforge.jdbt.runtime.RuntimeEngine;
import org.realityforge.jdbt.runtime.RuntimeExecutionException;
import org.realityforge.jdbt.runtime.StandardImportEmitter;

final class DefaultCommandRunner implements CommandRunner {
    private static final String DEFAULT_IMPORT_KEY = "default";
    private final ProjectRuntimeLoader projectRuntimeLoader;
    private final DbDriver dbDriver;
    private final FileResolver fileResolver;

    DefaultCommandRunner(final ProjectRuntimeLoader projectRuntimeLoader) {
        this(projectRuntimeLoader, new SqlServerDbDriver(), new FileResolver());
    }

    DefaultCommandRunner(
            final ProjectRuntimeLoader projectRuntimeLoader, final DbDriver dbDriver, final FileResolver fileResolver) {
        this.projectRuntimeLoader = projectRuntimeLoader;
        this.dbDriver = dbDriver;
        this.fileResolver = fileResolver;
    }

    @Override
    public void validateProject() {
        projectRuntimeLoader.validate();
    }

    @Override
    public void status(final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        final var runtimeEngine = runtimeEngine();
        System.out.print(runtimeEngine.status(runtime.database(), filterProperties));
    }

    @Override
    public void create(
            final DatabaseConnection target, final boolean noCreate, final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        runtimeEngine().create(runtime.database(), target, noCreate, filterProperties);
    }

    @Override
    public void createWithDataset(
            final DatabaseConnection target,
            final boolean noCreate,
            final String dataset,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        runtimeEngine().createWithDataset(runtime.database(), target, noCreate, dataset, filterProperties);
    }

    @Override
    public void drop(final DatabaseConnection target, final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        runtimeEngine().drop(runtime.database(), target, filterProperties);
    }

    @Override
    public void migrate(final DatabaseConnection target, final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        runtimeEngine().migrate(runtime.database(), target, filterProperties);
    }

    @Override
    public void databaseImport(
            final @Nullable String importKey,
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
                timing -> runtimeEngine(timing)
                        .databaseImport(
                                runtime.database(), resolvedImport, target, source, resumeAt, filterProperties));
    }

    @Override
    public void createByImport(
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
                timing -> runtimeEngine(timing)
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
            final String dataset, final DatabaseConnection target, final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        runtimeEngine().loadDataset(runtime.database(), dataset, target, filterProperties);
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
        new StandardImportEmitter(dbDriver).emit(runtime.database(), resolvedImport, outputDirectory, replace);
    }

    @Override
    public void verifyConstraints(
            final DatabaseConnection target,
            final List<String> schemas,
            final List<String> checkQueries,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        runtimeEngine().verifyConstraints(runtime.database(), target, schemas, checkQueries, filterProperties);
    }

    @Override
    public void exportFixtures(
            final DatabaseConnection target,
            final Path propertiesFile,
            final @Nullable String dataset,
            final @Nullable Path outputDirectory,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load();
        final var resolvedOutputDirectory = null == outputDirectory ? runtime.projectDirectory() : outputDirectory;
        runtimeEngine()
                .exportFixtures(
                        runtime.database(), target, propertiesFile, dataset, resolvedOutputDirectory, filterProperties);
    }

    @Override
    public void exportDatabaseStatistics(final DatabaseConnection target, final Path outputFile) {
        final var runtime = projectRuntimeLoader.load();
        final var count = new SqlServerDatabaseStatisticsExporter(dbDriver)
                .export(runtime.database().repository(), target, outputFile);
        System.out.println("Exported " + count + " database statistics to "
                + outputFile.toAbsolutePath().normalize());
    }

    private static String resolveImportKey(
            final ProjectRuntimeLoader.LoadedRuntime runtime, final @Nullable String importKey) {
        if (null != importKey) {
            return importKey;
        }
        if (!runtime.database().imports().containsKey(DEFAULT_IMPORT_KEY)) {
            throw new RuntimeExecutionException(
                    "Unable to locate import definition by key '" + DEFAULT_IMPORT_KEY + "'");
        }
        return DEFAULT_IMPORT_KEY;
    }

    private RuntimeEngine runtimeEngine() {
        return new RuntimeEngine(dbDriver, fileResolver);
    }

    private RuntimeEngine runtimeEngine(final ImportTimingRecorder timing) {
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
