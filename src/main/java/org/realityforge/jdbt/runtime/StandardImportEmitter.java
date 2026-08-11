package org.realityforge.jdbt.runtime;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.db.DbDriver;
import org.realityforge.jdbt.repository.RowSource;

public final class StandardImportEmitter {
    private static final String TARGET_DATABASE = "__TARGET__";
    private static final String SOURCE_DATABASE = "__SOURCE__";

    private final DbDriver driver;

    public StandardImportEmitter(final DbDriver driver) {
        this.driver = driver;
    }

    public Path emit(
            final RuntimeDatabase database,
            final String importKey,
            final @Nullable Path outputDirectory,
            final boolean replace) {
        if (!driver.supportsStandardImportScripts()) {
            throw new RuntimeExecutionException(
                    "Database driver does not support offline Standard Import Script emission");
        }

        final var importConfig = database.imports().get(importKey);
        if (null == importConfig) {
            throw new RuntimeExecutionException("Unable to locate import definition by key '" + importKey + "'");
        }

        final var scripts = generateScripts(database, importConfig.modules());
        final var projectDirectory = database.searchDirs().get(0);
        final var requestedDestination = null == outputDirectory
                ? projectDirectory.resolve("tmp/imports")
                : outputDirectory.isAbsolute() ? outputDirectory : projectDirectory.resolve(outputDirectory);
        final var destination = resolveSafeDestination(projectDirectory, requestedDestination);
        if (null != outputDirectory && Files.exists(destination) && isNonEmptyDirectory(destination) && !replace) {
            throw new RuntimeExecutionException(
                    "Output directory " + destination + " is not empty; specify --replace to replace it");
        }

        writeStagedAndReplace(scripts, destination);
        return destination;
    }

    private List<Script> generateScripts(final RuntimeDatabase database, final List<String> modules) {
        final var scripts = new ArrayList<Script>();
        final var paths = new HashSet<Path>();
        for (final var moduleName : modules) {
            final var importDirectory = Path.of(pathSegment(moduleName, "Database Module"), "import");
            for (final var table : database.tablesForModule(moduleName)) {
                if (RowSource.IMPORT == table.rowSource()) {
                    addScript(
                            scripts,
                            paths,
                            importDirectory.resolve(pathSegment(cleanObjectName(table.name()), "table") + ".sql"),
                            driver.generateStandardImportSql(
                                    table.name(), TARGET_DATABASE, SOURCE_DATABASE, table.columns()));
                }
            }
            for (final var sequence : database.sequenceOrdering(moduleName)) {
                addScript(
                        scripts,
                        paths,
                        importDirectory.resolve(pathSegment(cleanObjectName(sequence), "sequence") + ".sql"),
                        driver.generateStandardSequenceImportSql(sequence, TARGET_DATABASE, SOURCE_DATABASE));
            }
        }
        return List.copyOf(scripts);
    }

    private static void addScript(
            final List<Script> scripts, final HashSet<Path> paths, final Path path, final String content) {
        if (!paths.add(path)) {
            throw new RuntimeExecutionException("Multiple repository objects emit to " + path);
        }
        scripts.add(new Script(path, content));
    }

    private static String pathSegment(final String value, final String description) {
        if (value.isBlank()
                || ".".equals(value)
                || "..".equals(value)
                || value.contains("/")
                || value.contains("\\")
                || Path.of(value).isAbsolute()) {
            throw new RuntimeExecutionException("Unable to emit Standard Import Script for " + description
                    + " with unsafe path name '" + value + "'");
        }
        return value;
    }

    private static Path resolveSafeDestination(final Path projectDirectory, final Path requestedDestination) {
        final var project = toRealPath(projectDirectory.toAbsolutePath().normalize(), "Database Project directory");
        final var requested = requestedDestination.toAbsolutePath().normalize();
        if (Files.isSymbolicLink(requested)) {
            throw new RuntimeExecutionException("Output directory must not be a symbolic link: " + requested);
        }

        var existingAncestor = requested;
        while (null != existingAncestor && !Files.exists(existingAncestor, LinkOption.NOFOLLOW_LINKS)) {
            existingAncestor = existingAncestor.getParent();
        }
        if (null == existingAncestor) {
            throw new RuntimeExecutionException(
                    "Unable to locate an existing ancestor for output directory " + requested);
        }
        if (!Files.isDirectory(existingAncestor)) {
            throw new RuntimeExecutionException("Output directory has a non-directory ancestor: " + existingAncestor);
        }

        final var canonicalAncestor = toRealPath(existingAncestor, "output directory ancestor");
        final var canonicalDestination = canonicalAncestor
                .resolve(existingAncestor.relativize(requested))
                .normalize();
        if (canonicalDestination.equals(canonicalDestination.getRoot())) {
            throw new RuntimeExecutionException(
                    "Output directory must not be a filesystem root: " + canonicalDestination);
        }
        if (project.startsWith(canonicalDestination)) {
            throw new RuntimeExecutionException(
                    "Output directory must not be the Database Project directory or one of its ancestors: "
                            + canonicalDestination);
        }
        if (Files.exists(canonicalDestination, LinkOption.NOFOLLOW_LINKS)
                && !Files.isDirectory(canonicalDestination, LinkOption.NOFOLLOW_LINKS)) {
            throw new RuntimeExecutionException("Output directory is not a directory: " + canonicalDestination);
        }
        return canonicalDestination;
    }

    private static Path toRealPath(final Path path, final String description) {
        try {
            return path.toRealPath();
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed resolving " + description + ' ' + path, ioe);
        }
    }

    private static boolean isNonEmptyDirectory(final Path directory) {
        try (var entries = Files.list(directory)) {
            return entries.findAny().isPresent();
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed reading output directory " + directory, ioe);
        }
    }

    private static void writeStagedAndReplace(final List<Script> scripts, final Path destination) {
        final var stagingParent = existingDirectoryForStaging(destination);
        final Path stagingDirectory;
        try {
            stagingDirectory = Files.createTempDirectory(stagingParent, ".jdbt-standard-imports-");
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed creating Standard Import Script staging directory", ioe);
        }

        try {
            for (final var script : scripts) {
                final var file = stagingDirectory.resolve(script.path()).normalize();
                if (!file.startsWith(stagingDirectory)) {
                    throw new RuntimeExecutionException(
                            "Standard Import Script path escapes staging directory: " + script.path());
                }
                Files.createDirectories(file.getParent());
                Files.writeString(file, script.content(), StandardCharsets.UTF_8);
            }
            replaceDestination(stagingDirectory, destination);
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed writing Standard Import Scripts to " + destination, ioe);
        } finally {
            deleteRecursively(stagingDirectory);
        }
    }

    private static Path existingDirectoryForStaging(final Path destination) {
        var candidate = destination.getParent();
        while (null != candidate && !Files.exists(candidate, LinkOption.NOFOLLOW_LINKS)) {
            candidate = candidate.getParent();
        }
        if (null == candidate || !Files.isDirectory(candidate)) {
            throw new RuntimeExecutionException("Unable to locate staging directory for " + destination);
        }
        return toRealPath(candidate, "staging directory");
    }

    private static void replaceDestination(final Path stagingDirectory, final Path destination) throws IOException {
        Files.createDirectories(destination.getParent());
        @Nullable Path backup = null;
        if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
            backup = Files.createTempDirectory(destination.getParent(), ".jdbt-standard-imports-backup-");
            Files.delete(backup);
            Files.move(destination, backup);
        }

        try {
            Files.move(stagingDirectory, destination);
        } catch (final IOException ioe) {
            if (null != backup) {
                Files.move(backup, destination);
            }
            throw ioe;
        }
        if (null != backup) {
            deleteRecursively(backup);
        }
    }

    private static void deleteRecursively(final Path directory) {
        if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
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
            throw new UncheckedIOException("Failed deleting directory " + directory, ioe);
        }
    }

    private static String cleanObjectName(final String value) {
        return value.replace("[", "")
                .replace("]", "")
                .replace("\"", "")
                .replace("'", "")
                .replace(" ", "");
    }

    private record Script(Path path, String content) {}
}
