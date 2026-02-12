package org.realityforge.jdbt.packaging;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.files.ArtifactContent;
import org.realityforge.jdbt.files.FileResolver;
import org.realityforge.jdbt.runtime.RuntimeDatabase;
import org.realityforge.jdbt.runtime.RuntimeExecutionException;

public final class DatabaseDataPackager {
    private static final Pattern ARTIFACT_FILE_PATTERN = Pattern.compile("^zip:([^:]+):(.+)$");

    private final FileResolver fileResolver;

    public DatabaseDataPackager(final FileResolver fileResolver) {
        this.fileResolver = fileResolver;
    }

    public void packageDatabaseData(final RuntimeDatabase database, final Path packageDir) {
        createDirectories(packageDir);

        final List<String> importDirs = database.imports().values().stream()
                .map(config -> config.dir())
                .sorted()
                .distinct()
                .toList();
        final List<String> datasetDirs = database.datasets().stream()
                .map(dataset -> database.datasetsDirName() + '/' + dataset)
                .toList();

        final Set<String> fixtureStyleDirs = new LinkedHashSet<>();
        fixtureStyleDirs.add(database.fixtureDirName());
        fixtureStyleDirs.addAll(datasetDirs);

        final List<String> moduleDirs = new ArrayList<>();
        moduleDirs.addAll(database.upDirs());
        moduleDirs.addAll(database.downDirs());
        moduleDirs.addAll(database.finalizeDirs());
        moduleDirs.add(database.fixtureDirName());
        moduleDirs.addAll(importDirs);
        moduleDirs.addAll(datasetDirs);

        for (final String moduleName : database.repository().modules()) {
            for (final String relativeDirName : moduleDirs) {
                final String relativeModuleDir = moduleName + '/' + relativeDirName;
                final Path targetDir = packageDir.resolve(relativeModuleDir);
                if (fixtureStyleDirs.contains(relativeDirName)) {
                    final List<String> files = fileResolver.collectFiles(
                            database.searchDirs(),
                            relativeModuleDir,
                            "yml",
                            database.indexFileName(),
                            database.postDbArtifacts(),
                            database.preDbArtifacts());
                    copyFilesToDir(database, filesForKnownElements(database, moduleName, files, "yml"), targetDir);
                } else if (importDirs.contains(relativeDirName)) {
                    final List<String> files = new ArrayList<>();
                    files.addAll(fileResolver.collectFiles(
                            database.searchDirs(),
                            relativeModuleDir,
                            "yml",
                            database.indexFileName(),
                            database.postDbArtifacts(),
                            database.preDbArtifacts()));
                    files.addAll(fileResolver.collectFiles(
                            database.searchDirs(),
                            relativeModuleDir,
                            "sql",
                            database.indexFileName(),
                            database.postDbArtifacts(),
                            database.preDbArtifacts()));
                    copyFilesToDir(database, filesForKnownElements(database, moduleName, files, null), targetDir);
                } else {
                    final List<String> files = fileResolver.collectFiles(
                            database.searchDirs(),
                            relativeModuleDir,
                            "sql",
                            database.indexFileName(),
                            database.postDbArtifacts(),
                            database.preDbArtifacts());
                    copyFilesToDir(database, files, targetDir);
                    generateIndex(database.indexFileName(), targetDir, files);
                }
            }
        }

        for (final String databaseWideDir : databaseWideDirs(database)) {
            final Path targetDir = packageDir.resolve(databaseWideDir);
            final List<String> files = fileResolver.collectFiles(
                    database.searchDirs(),
                    databaseWideDir,
                    "sql",
                    database.indexFileName(),
                    database.postDbArtifacts(),
                    database.preDbArtifacts());
            copyFilesToDir(database, files, targetDir);
            generateIndex(database.indexFileName(), targetDir, files);
        }

        writeRepository(database, packageDir.resolve("repository.yml"));

        if (database.migrationsEnabled()) {
            final List<String> files = fileResolver.collectFiles(
                    database.searchDirs(),
                    database.migrationsDirName(),
                    "sql",
                    database.indexFileName(),
                    database.postDbArtifacts(),
                    database.preDbArtifacts());
            final Path targetDir = packageDir.resolve(database.migrationsDirName());
            copyFilesToDir(database, files, targetDir);
            generateIndex(database.indexFileName(), targetDir, files);
        }
    }

    private static List<String> databaseWideDirs(final RuntimeDatabase database) {
        final List<String> directories = new ArrayList<>();
        directories.addAll(database.preCreateDirs());
        directories.addAll(database.postCreateDirs());

        final List<String> importKeys =
                database.imports().keySet().stream().sorted().toList();
        for (final String importKey : importKeys) {
            final var importConfig = database.imports().get(importKey);
            if (null != importConfig) {
                directories.addAll(importConfig.preImportDirs());
                directories.addAll(importConfig.postImportDirs());
            }
        }

        for (final String dataset : database.datasets()) {
            final String root = database.datasetsDirName() + '/' + dataset;
            for (final String pre : database.preDatasetDirs()) {
                directories.add(root + '/' + pre);
            }
            for (final String post : database.postDatasetDirs()) {
                directories.add(root + '/' + post);
            }
        }

        return List.copyOf(directories);
    }

    private static List<String> filesForKnownElements(
            final RuntimeDatabase database,
            final String moduleName,
            final List<String> files,
            final @Nullable String fixedExtension) {
        final Set<String> knownElementNames = database.orderedElementsForModule(moduleName).stream()
                .map(DatabaseDataPackager::cleanObjectName)
                .collect(Collectors.toUnmodifiableSet());
        final List<String> output = new ArrayList<>();
        for (final String file : files) {
            final String basename = basename(file);
            final String extension = fileExtension(basename);
            if (null != fixedExtension && !fixedExtension.equals(extension)) {
                continue;
            }
            final String elementName = basenameWithoutExtension(basename);
            if (knownElementNames.contains(elementName)) {
                output.add(file);
            }
        }
        return List.copyOf(output);
    }

    private static void copyFilesToDir(final RuntimeDatabase database, final List<String> files, final Path targetDir) {
        if (files.isEmpty()) {
            return;
        }
        createDirectories(targetDir);
        for (final String file : files) {
            writeText(targetDir.resolve(basename(file)), readText(database, file));
        }
    }

    private static void generateIndex(final String indexFileName, final Path targetDir, final List<String> files) {
        if (files.isEmpty()) {
            return;
        }
        final String index = String.join(
                "\n", files.stream().map(DatabaseDataPackager::basename).toList());
        writeText(targetDir.resolve(indexFileName), index);
    }

    private static void writeRepository(final RuntimeDatabase database, final Path path) {
        final var yaml = new StringBuilder();
        yaml.append("modules:\n");
        for (final String module : database.repository().modules()) {
            yaml.append("  ").append(toYamlScalar(module)).append(":\n");

            final String schemaName = database.repository().schemaNameForModule(module);
            if (!module.equals(schemaName)) {
                yaml.append("    schema: ").append(toYamlScalar(schemaName)).append('\n');
            }

            appendYamlList(yaml, "tables", database.repository().tableOrdering(module));
            appendYamlList(yaml, "sequences", database.repository().sequenceOrdering(module));
        }
        writeText(path, yaml.toString());
    }

    private static void appendYamlList(final StringBuilder yaml, final String key, final List<String> values) {
        if (values.isEmpty()) {
            yaml.append("    ").append(key).append(": []\n");
            return;
        }
        yaml.append("    ").append(key).append(":\n");
        for (final String value : values) {
            yaml.append("      - ").append(toYamlScalar(value)).append('\n');
        }
    }

    private static String readText(final RuntimeDatabase database, final String location) {
        final Matcher matcher = ARTIFACT_FILE_PATTERN.matcher(location);
        if (matcher.matches()) {
            final String artifactId = matcher.group(1);
            final String file = matcher.group(2);
            final ArtifactContent artifact = database.artifactById(artifactId);
            if (null == artifact) {
                throw new RuntimeExecutionException("Unable to locate artifact with id '" + artifactId + "'.");
            }
            return artifact.readText(file);
        }
        try {
            return Files.readString(Path.of(location));
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to read file " + location, ioe);
        }
    }

    private static String toYamlScalar(final String value) {
        return '\'' + value.replace("'", "''") + '\'';
    }

    private static String cleanObjectName(final String value) {
        return value.replace("[", "")
                .replace("]", "")
                .replace("\"", "")
                .replace("'", "")
                .replace(" ", "");
    }

    private static String basename(final String value) {
        final Matcher matcher = ARTIFACT_FILE_PATTERN.matcher(value);
        final String candidate = matcher.matches() ? matcher.group(2) : value;
        final int slash = Math.max(candidate.lastIndexOf('/'), candidate.lastIndexOf('\\'));
        return -1 == slash ? candidate : candidate.substring(slash + 1);
    }

    private static String fileExtension(final String filename) {
        final int dot = filename.lastIndexOf('.');
        return -1 == dot ? "" : filename.substring(dot + 1);
    }

    private static String basenameWithoutExtension(final String filename) {
        final int dot = filename.lastIndexOf('.');
        return -1 == dot ? filename : filename.substring(0, dot);
    }

    private static void createDirectories(final Path directory) {
        try {
            Files.createDirectories(directory);
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to create directory " + directory, ioe);
        }
    }

    private static void writeText(final Path path, final String content) {
        try {
            final Path parent = path.getParent();
            if (null != parent) {
                Files.createDirectories(parent);
            }
            Files.writeString(path, content, StandardCharsets.UTF_8);
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to write file " + path, ioe);
        }
    }
}
