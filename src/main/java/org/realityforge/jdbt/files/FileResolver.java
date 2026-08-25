package org.realityforge.jdbt.files;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;

public final class FileResolver {
    public List<ResourceFile> collectFiles(
            final Path resourceRoot,
            final String relativeDir,
            final String extension,
            final String indexFileName,
            final List<ArtifactContent> postArtifacts,
            final List<ArtifactContent> preArtifacts) {
        final var directory = resourceRoot.resolve(relativeDir);

        final var index = new ArrayList<String>();
        final var files = new ArrayList<ResourceFile>();

        final var indexEntries = readIndexEntries(directory.resolve(indexFileName));
        validateIndexEntries(indexEntries, directory);
        index.addAll(indexEntries);
        files.addAll(readFiles(resourceRoot, directory, extension));

        final var prefix = normalizeRelativeDir(relativeDir);
        final var indexEntryPath = prefix + '/' + indexFileName;
        final var matcher = Pattern.compile("^" + Pattern.quote(prefix) + "/[^/]*\\." + Pattern.quote(extension) + "$");

        addArtifactFiles(files, index, postArtifacts, indexEntryPath, matcher);
        addArtifactFiles(files, index, preArtifacts, indexEntryPath, matcher);

        failIfDuplicateBasenames(files);

        files.sort(indexComparator(index));
        return List.copyOf(files);
    }

    public Map<String, ResourceFile> collectFixtures(
            final Path resourceRoot,
            final String moduleName,
            final @Nullable String subdir,
            final List<String> orderedElements,
            final List<ArtifactContent> postArtifacts,
            final List<ArtifactContent> preArtifacts) {
        final var relativeModuleDir = moduleName + (subdir == null ? "" : "/" + subdir);
        final var directory = resourceRoot.resolve(relativeModuleDir);

        final var filesystemYamlFiles = new ArrayList<>(readFiles(resourceRoot, directory, "yml"));
        final var filesystemSqlFiles = new ArrayList<>(readFiles(resourceRoot, directory, "sql"));

        final var fixtures = new LinkedHashMap<String, ResourceFile>();
        for (final var element : orderedElements) {
            final var fixtureBasename = cleanObjectName(element) + ".yml";
            final var filename = directory.resolve(fixtureBasename);
            final var resource = new ResourceFile.OnDisk(resourceRoot, filename);
            filesystemYamlFiles.remove(resource);
            if (Files.exists(filename)) {
                fixtures.put(element, resource);
            }

            if (!fixtures.containsKey(element)) {
                final var artifactFixtureName = relativeModuleDir + '/' + fixtureBasename;
                final var fixture = findFromArtifacts(artifactFixtureName, postArtifacts, preArtifacts);
                if (fixture != null) {
                    fixtures.put(element, fixture);
                }
            }
        }

        if (!filesystemYamlFiles.isEmpty()) {
            throw new FileCollectionException(
                    "Unexpected fixtures found in database search paths. Fixtures do not match existing tables. Files: "
                            + filesystemYamlFiles);
        }
        if (!filesystemSqlFiles.isEmpty()) {
            throw new FileCollectionException(
                    "Unexpected sql files found in fixture directories. SQL files are not processed. Files: "
                            + filesystemSqlFiles);
        }
        return Map.copyOf(fixtures);
    }

    public @Nullable ResourceFile findFileInModule(
            final Path resourceRoot,
            final String moduleName,
            final String subdir,
            final String tableName,
            final String extension,
            final List<ArtifactContent> postArtifacts,
            final List<ArtifactContent> preArtifacts) {
        final var filename = moduleFilename(moduleName, subdir, tableName, extension);

        final var file = resourceRoot.resolve(filename);
        if (Files.exists(file)) {
            return new ResourceFile.OnDisk(resourceRoot, file);
        }
        return findFromArtifacts(filename, postArtifacts, preArtifacts);
    }

    private static @Nullable ResourceFile findFromArtifacts(
            final String filename,
            final List<ArtifactContent> postArtifacts,
            final List<ArtifactContent> preArtifacts) {
        for (final var artifact : postArtifacts) {
            if (artifact.files().contains(filename)) {
                return new ResourceFile.InArtifact(artifact, filename);
            }
        }
        for (final var artifact : preArtifacts) {
            if (artifact.files().contains(filename)) {
                return new ResourceFile.InArtifact(artifact, filename);
            }
        }
        return null;
    }

    private static void validateIndexEntries(final List<String> entries, final Path directory) {
        for (final var entry : entries) {
            if (!Files.exists(directory.resolve(entry))) {
                throw new FileCollectionException("A specified index entry does not exist on the disk " + entry);
            }
        }
    }

    private static List<String> readIndexEntries(final Path indexFile) {
        if (!Files.exists(indexFile)) {
            return List.of();
        }

        try {
            return Files.readAllLines(indexFile).stream().map(String::trim).toList();
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to read index file " + indexFile, ioe);
        }
    }

    private static List<ResourceFile> readFiles(final Path resourceRoot, final Path directory, final String extension) {
        if (!Files.isDirectory(directory)) {
            return List.of();
        }

        try (var stream = Files.list(directory)) {
            return stream.filter(Files::isRegularFile)
                    .filter(file -> file.getFileName().toString().endsWith('.' + extension))
                    .map(file -> (ResourceFile) new ResourceFile.OnDisk(resourceRoot, file))
                    .toList();
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to read files in " + directory, ioe);
        }
    }

    private static void addArtifactFiles(
            final List<ResourceFile> files,
            final List<String> index,
            final List<ArtifactContent> artifacts,
            final String indexEntryPath,
            final Pattern matcher) {
        for (final var artifact : artifacts) {
            if (artifact.files().contains(indexEntryPath)) {
                index.addAll(splitIndexContent(artifact.readText(indexEntryPath)));
            }

            final var candidates = artifact.files().stream()
                    .filter(file -> matcher.matcher(file).matches())
                    .toList();
            for (final var candidate : candidates) {
                final var resource = new ResourceFile.InArtifact(artifact, candidate);
                if (!containsBasename(files, resource.basename())) {
                    files.add(resource);
                }
            }
        }
    }

    private static Comparator<ResourceFile> indexComparator(final List<String> index) {
        return (left, right) -> {
            final var leftBasename = left.basename();
            final var rightBasename = right.basename();
            final var leftIndex = index.indexOf(leftBasename);
            final var rightIndex = index.indexOf(rightBasename);
            if (-1 == leftIndex && -1 == rightIndex) {
                return leftBasename.compareTo(rightBasename);
            }
            if (-1 == leftIndex) {
                return 1;
            }
            if (-1 == rightIndex) {
                return -1;
            }
            return Integer.compare(leftIndex, rightIndex);
        };
    }

    private static void failIfDuplicateBasenames(final List<ResourceFile> files) {
        final var groups = files.stream()
                .collect(Collectors.groupingBy(ResourceFile::basename, LinkedHashMap::new, Collectors.toList()));
        final var duplicates =
                groups.values().stream().filter(values -> values.size() > 1).toList();
        if (!duplicates.isEmpty()) {
            final var detail = duplicates.stream()
                    .map(values -> values.stream().map(ResourceFile::sourceName).collect(Collectors.joining("\n\t")))
                    .collect(Collectors.joining("\n\t"));
            throw new FileCollectionException("Files with duplicate basename not allowed.\n\t" + detail);
        }
    }

    private static boolean containsBasename(final List<ResourceFile> files, final String basename) {
        return files.stream().anyMatch(file -> file.basename().equals(basename));
    }

    private static String moduleFilename(
            final String moduleName, final String subdir, final String tableName, final String extension) {
        return moduleName + '/' + subdir + '/' + cleanObjectName(tableName) + '.' + extension;
    }

    private static String cleanObjectName(final String tableName) {
        return tableName
                .replace("[", "")
                .replace("]", "")
                .replace("\"", "")
                .replace("'", "")
                .replace(" ", "");
    }

    private static String normalizeRelativeDir(final String relativeDir) {
        String value = relativeDir.replace("/./", "/");
        if (value.endsWith("/.")) {
            value = value.substring(0, value.length() - 2);
        }
        return value;
    }

    private static List<String> splitIndexContent(final String content) {
        return Pattern.compile("\\s+")
                .splitAsStream(content)
                .filter(token -> !token.isEmpty())
                .toList();
    }
}
