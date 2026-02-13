package org.realityforge.jdbt.cli;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.db.DatabaseConnection;
import org.realityforge.jdbt.db.DbDriverFactory;
import org.realityforge.jdbt.files.FileResolver;
import org.realityforge.jdbt.packaging.DatabaseDataPackager;
import org.realityforge.jdbt.packaging.DeterministicZipPackager;
import org.realityforge.jdbt.runtime.RuntimeEngine;
import org.realityforge.jdbt.runtime.RuntimeExecutionException;

public final class DefaultCommandRunner implements CommandRunner {
    private final ProjectRuntimeLoader projectRuntimeLoader;
    private final DbDriverFactory dbDriverFactory;
    private final FileResolver fileResolver;

    public DefaultCommandRunner(final ProjectRuntimeLoader projectRuntimeLoader) {
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
    public void status(final @Nullable String databaseKey, final String driver) {
        final var runtime = projectRuntimeLoader.load(databaseKey);
        final var runtimeEngine = runtimeEngine(driver);
        System.out.print(runtimeEngine.status(runtime.database()));
    }

    @Override
    public void create(
            final @Nullable String databaseKey,
            final String driver,
            final DatabaseConnection target,
            final boolean noCreate,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load(databaseKey);
        runtimeEngine(driver).create(runtime.database(), target, noCreate, filterProperties);
    }

    @Override
    public void drop(
            final @Nullable String databaseKey,
            final String driver,
            final DatabaseConnection target,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load(databaseKey);
        runtimeEngine(driver).drop(runtime.database(), target, filterProperties);
    }

    @Override
    public void migrate(
            final @Nullable String databaseKey,
            final String driver,
            final DatabaseConnection target,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load(databaseKey);
        runtimeEngine(driver).migrate(runtime.database(), target, filterProperties);
    }

    @Override
    public void databaseImport(
            final @Nullable String databaseKey,
            final String driver,
            final @Nullable String importKey,
            final @Nullable String moduleGroup,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final @Nullable String resumeAt,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load(databaseKey);
        final var resolvedImport = resolveImportKey(runtime, importKey);
        runtimeEngine(driver)
                .databaseImport(
                        runtime.database(), resolvedImport, moduleGroup, target, source, resumeAt, filterProperties);
    }

    @Override
    public void createByImport(
            final @Nullable String databaseKey,
            final String driver,
            final @Nullable String importKey,
            final DatabaseConnection target,
            final DatabaseConnection source,
            final @Nullable String resumeAt,
            final boolean noCreate,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load(databaseKey);
        final var resolvedImport = resolveImportKey(runtime, importKey);
        runtimeEngine(driver)
                .createByImport(
                        runtime.database(), resolvedImport, target, source, resumeAt, noCreate, filterProperties);
    }

    @Override
    public void loadDataset(
            final @Nullable String databaseKey,
            final String driver,
            final String dataset,
            final DatabaseConnection target,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load(databaseKey);
        runtimeEngine(driver).loadDataset(runtime.database(), dataset, target, filterProperties);
    }

    @Override
    public void upModuleGroup(
            final @Nullable String databaseKey,
            final String driver,
            final String moduleGroup,
            final DatabaseConnection target,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load(databaseKey);
        runtimeEngine(driver).upModuleGroup(runtime.database(), moduleGroup, target, filterProperties);
    }

    @Override
    public void downModuleGroup(
            final @Nullable String databaseKey,
            final String driver,
            final String moduleGroup,
            final DatabaseConnection target,
            final Map<String, String> filterProperties) {
        final var runtime = projectRuntimeLoader.load(databaseKey);
        runtimeEngine(driver).downModuleGroup(runtime.database(), moduleGroup, target, filterProperties);
    }

    @Override
    public void packageData(final @Nullable String databaseKey, final Path outputFile) {
        final var runtime = projectRuntimeLoader.load(databaseKey);
        final Path stagingDirectory;
        try {
            stagingDirectory = Files.createTempDirectory("jdbt-package-data-");
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed creating package staging directory", ioe);
        }
        try {
            new DatabaseDataPackager(fileResolver).packageDatabaseData(runtime.database(), stagingDirectory);
            new DeterministicZipPackager().write(stagingDirectory, outputFile);
        } finally {
            deleteRecursively(stagingDirectory);
        }
    }

    @Override
    public void dumpFixtures(final @Nullable String databaseKey, final String driver, final DatabaseConnection target) {
        throw new RuntimeExecutionException("dump-fixtures command is not yet implemented.");
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
