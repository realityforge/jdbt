package org.realityforge.jdbt.packaging;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;
import org.realityforge.jdbt.files.FileResolver;
import org.realityforge.jdbt.files.ResourceFile;
import org.realityforge.jdbt.repository.RepositoryTable;
import org.realityforge.jdbt.repository.RowSource;
import org.realityforge.jdbt.runtime.RuntimeDatabase;

public final class DatabaseDataPackager {
    private final FileResolver fileResolver;

    public DatabaseDataPackager(final FileResolver fileResolver) {
        this.fileResolver = fileResolver;
    }

    public void packageDatabaseData(final RuntimeDatabase database, final Path packageDir) {
        createDirectories(packageDir);

        final var importDirs = database.imports().values().stream()
                .map(config -> config.dir())
                .sorted()
                .distinct()
                .toList();
        final var datasetDirs = database.datasets().stream()
                .map(dataset -> database.datasetsDirName() + '/' + dataset)
                .toList();

        final var fixtureStyleDirs = new LinkedHashSet<String>();
        fixtureStyleDirs.add(database.fixtureDirName());
        fixtureStyleDirs.addAll(datasetDirs);

        final var moduleDirs = new ArrayList<String>();
        moduleDirs.addAll(database.upDirs());
        moduleDirs.addAll(database.downDirs());
        moduleDirs.addAll(database.finalizeDirs());
        moduleDirs.add(database.fixtureDirName());
        moduleDirs.addAll(importDirs);
        moduleDirs.addAll(datasetDirs);

        for (final var moduleName : database.repository().modules()) {
            for (final var relativeDirName : moduleDirs) {
                final var relativeModuleDir = moduleName + '/' + relativeDirName;
                final var targetDir = packageDir.resolve(relativeModuleDir);
                if (fixtureStyleDirs.contains(relativeDirName)) {
                    final var files = fileResolver.collectFiles(
                            database.resourceRoot(),
                            relativeModuleDir,
                            "yml",
                            database.indexFileName(),
                            database.postDbArtifacts(),
                            database.preDbArtifacts());
                    copyFilesToDir(filesForKnownElements(database, moduleName, files, "yml"), targetDir);
                } else if (importDirs.contains(relativeDirName)) {
                    final var files = new ArrayList<ResourceFile>();
                    files.addAll(fileResolver.collectFiles(
                            database.resourceRoot(),
                            relativeModuleDir,
                            "yml",
                            database.indexFileName(),
                            database.postDbArtifacts(),
                            database.preDbArtifacts()));
                    files.addAll(fileResolver.collectFiles(
                            database.resourceRoot(),
                            relativeModuleDir,
                            "sql",
                            database.indexFileName(),
                            database.postDbArtifacts(),
                            database.preDbArtifacts()));
                    copyFilesToDir(filesForKnownElements(database, moduleName, files, null), targetDir);
                } else {
                    final var files = fileResolver.collectFiles(
                            database.resourceRoot(),
                            relativeModuleDir,
                            "sql",
                            database.indexFileName(),
                            database.postDbArtifacts(),
                            database.preDbArtifacts());
                    copyFilesToDir(files, targetDir);
                    generateIndex(database.indexFileName(), targetDir, files);
                }
            }
        }

        for (final var databaseWideDir : databaseWideDirs(database)) {
            final var targetDir = packageDir.resolve(databaseWideDir);
            final var files = fileResolver.collectFiles(
                    database.resourceRoot(),
                    databaseWideDir,
                    "sql",
                    database.indexFileName(),
                    database.postDbArtifacts(),
                    database.preDbArtifacts());
            copyFilesToDir(files, targetDir);
            generateIndex(database.indexFileName(), targetDir, files);
        }

        writeRepository(database, packageDir.resolve("repository.yml"));

        final var files = fileResolver.collectFiles(
                database.resourceRoot(),
                database.migrationDir(),
                "sql",
                database.indexFileName(),
                database.postDbArtifacts(),
                database.preDbArtifacts());
        final var targetDir = packageDir.resolve(database.migrationDir());
        copyFilesToDir(files, targetDir);
        generateIndex(database.indexFileName(), targetDir, files);
    }

    private static List<String> databaseWideDirs(final RuntimeDatabase database) {
        final var directories = new ArrayList<String>();
        directories.addAll(database.preCreateDirs());
        directories.addAll(database.postCreateDirs());

        final var importKeys = database.imports().keySet().stream().sorted().toList();
        for (final var importKey : importKeys) {
            final var importConfig = database.imports().get(importKey);
            if (null != importConfig) {
                directories.addAll(importConfig.preImportDirs());
                directories.addAll(importConfig.postImportDirs());
            }
        }

        for (final var dataset : database.datasets()) {
            final var root = database.datasetsDirName() + '/' + dataset;
            for (final var pre : database.preDatasetDirs()) {
                directories.add(root + '/' + pre);
            }
            for (final var post : database.postDatasetDirs()) {
                directories.add(root + '/' + post);
            }
        }

        return List.copyOf(directories);
    }

    private static List<ResourceFile> filesForKnownElements(
            final RuntimeDatabase database,
            final String moduleName,
            final List<ResourceFile> files,
            final @Nullable String fixedExtension) {
        final var knownElementNames = database.orderedElementsForModule(moduleName).stream()
                .map(DatabaseDataPackager::cleanObjectName)
                .collect(Collectors.toUnmodifiableSet());
        final var output = new ArrayList<ResourceFile>();
        for (final var file : files) {
            final var basename = file.basename();
            final var extension = fileExtension(basename);
            if (null != fixedExtension && !fixedExtension.equals(extension)) {
                continue;
            }
            final var elementName = basenameWithoutExtension(basename);
            if (knownElementNames.contains(elementName)) {
                output.add(file);
            }
        }
        return List.copyOf(output);
    }

    private static void copyFilesToDir(final List<ResourceFile> files, final Path targetDir) {
        if (files.isEmpty()) {
            return;
        }
        createDirectories(targetDir);
        for (final var file : files) {
            writeText(targetDir.resolve(file.basename()), file.readText());
        }
    }

    private static void generateIndex(
            final String indexFileName, final Path targetDir, final List<ResourceFile> files) {
        if (files.isEmpty()) {
            return;
        }
        final var index =
                String.join("\n", files.stream().map(ResourceFile::basename).toList());
        writeText(targetDir.resolve(indexFileName), index);
    }

    private static void writeRepository(final RuntimeDatabase database, final Path path) {
        final var yaml = new StringBuilder();
        yaml.append("modules:\n");
        for (final var module : database.repository().modules()) {
            yaml.append("  ").append(toYamlScalar(module)).append(":\n");

            final var schemaName = database.repository().schemaNameForModule(module);
            if (!module.equals(schemaName)) {
                yaml.append("    schema: ").append(toYamlScalar(schemaName)).append('\n');
            }

            appendYamlTables(yaml, database.repository().tablesForModule(module));
            appendYamlList(yaml, "sequences", database.repository().sequenceOrdering(module));
        }
        writeText(path, yaml.toString());
    }

    private static void appendYamlTables(final StringBuilder yaml, final List<RepositoryTable> tables) {
        if (tables.isEmpty()) {
            yaml.append("    tables: []\n");
            return;
        }
        yaml.append("    tables:\n");
        for (final var table : tables) {
            yaml.append("      - name: ").append(toYamlScalar(table.name())).append('\n');
            yaml.append("        columns:\n");
            for (final var column : table.columns()) {
                yaml.append("          - ").append(toYamlScalar(column)).append('\n');
            }
            if (table.indexes().isEmpty()) {
                yaml.append("        indexes: []\n");
            } else {
                yaml.append("        indexes:\n");
                for (final var index : table.indexes()) {
                    yaml.append("          - ").append(toYamlScalar(index)).append('\n');
                }
            }
            if (RowSource.DEPLOYMENT == table.rowSource()) {
                yaml.append("        rowSource: deployment\n");
            }
        }
    }

    private static void appendYamlList(final StringBuilder yaml, final String key, final List<String> values) {
        if (values.isEmpty()) {
            yaml.append("    ").append(key).append(": []\n");
            return;
        }
        yaml.append("    ").append(key).append(":\n");
        for (final var value : values) {
            yaml.append("      - ").append(toYamlScalar(value)).append('\n');
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

    private static String fileExtension(final String filename) {
        final var dot = filename.lastIndexOf('.');
        return -1 == dot ? "" : filename.substring(dot + 1);
    }

    private static String basenameWithoutExtension(final String filename) {
        final var dot = filename.lastIndexOf('.');
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
            final var parent = path.getParent();
            if (null != parent) {
                Files.createDirectories(parent);
            }
            Files.writeString(path, content, StandardCharsets.UTF_8);
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to write file " + path, ioe);
        }
    }
}
