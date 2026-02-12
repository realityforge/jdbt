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
    public List<String> collectFiles(
            final List<Path> searchDirs,
            final String relativeDir,
            final String extension,
            final String indexFileName,
            final List<ArtifactContent> postArtifacts,
            final List<ArtifactContent> preArtifacts) {
        final var directories =
                searchDirs.stream().map(d -> d.resolve(relativeDir)).toList();

        final var index = new ArrayList<String>();
        final var files = new ArrayList<String>();

        for (final var directory : directories) {
            final var indexEntries = readIndexEntries(directory.resolve(indexFileName));
            validateIndexEntries(indexEntries, directories);
            index.addAll(indexEntries);
            files.addAll(readFiles(directory, extension));
        }

        final var prefix = normalizeRelativeDir(relativeDir);
        final var indexEntryPath = prefix + '/' + indexFileName;
        final var matcher = Pattern.compile("^" + Pattern.quote(prefix) + "/[^/]*\\." + Pattern.quote(extension) + "$");

        addArtifactFiles(files, index, postArtifacts, indexEntryPath, matcher);
        addArtifactFiles(files, index, preArtifacts, indexEntryPath, matcher);

        failIfDuplicateBasenames(files);

        files.sort(indexComparator(index));
        return List.copyOf(files);
    }

    public Map<String, String> collectFixtures(
            final List<Path> searchDirs,
            final String moduleName,
            final @Nullable String subdir,
            final List<String> orderedElements,
            final List<ArtifactContent> postArtifacts,
            final List<ArtifactContent> preArtifacts) {
        final var relativeModuleDir = moduleName + (subdir == null ? "" : "/" + subdir);
        final var directories =
                searchDirs.stream().map(d -> d.resolve(relativeModuleDir)).toList();

        final var filesystemYamlFiles = new ArrayList<>(
                directories.stream().flatMap(d -> readFiles(d, "yml").stream()).toList());
        final var filesystemSqlFiles = new ArrayList<>(
                directories.stream().flatMap(d -> readFiles(d, "sql").stream()).toList());

        final var fixtures = new LinkedHashMap<String, String>();
        for (final var element : orderedElements) {
            final var fixtureBasename = cleanObjectName(element) + ".yml";
            for (final var directory : directories) {
                final var filename = directory.resolve(fixtureBasename).toString();
                filesystemYamlFiles.remove(filename);
                if (Files.exists(Path.of(filename))) {
                    if (fixtures.containsKey(element)) {
                        throw new FileCollectionException(
                                "Duplicate fixture for " + element + " found in database search paths");
                    }
                    fixtures.put(element, filename);
                }
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

    public @Nullable String findFileInModule(
            final List<Path> searchDirs,
            final String moduleName,
            final String subdir,
            final String tableName,
            final String extension,
            final List<ArtifactContent> postArtifacts,
            final List<ArtifactContent> preArtifacts) {
        final var filename = moduleFilename(moduleName, subdir, tableName, extension);

        for (final var searchDir : searchDirs) {
            final var file = searchDir.resolve(filename);
            if (Files.exists(file)) {
                return file.toString();
            }
        }
        return findFromArtifacts(filename, postArtifacts, preArtifacts);
    }

    private static @Nullable String findFromArtifacts(
            final String filename,
            final List<ArtifactContent> postArtifacts,
            final List<ArtifactContent> preArtifacts) {
        for (final var artifact : postArtifacts) {
            if (artifact.files().contains(filename)) {
                return toArtifactLocation(artifact, filename);
            }
        }
        for (final var artifact : preArtifacts) {
            if (artifact.files().contains(filename)) {
                return toArtifactLocation(artifact, filename);
            }
        }
        return null;
    }

    private static void validateIndexEntries(final List<String> entries, final List<Path> directories) {
        for (final var entry : entries) {
            final var exists = directories.stream().anyMatch(dir -> Files.exists(dir.resolve(entry)));
            if (!exists) {
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

    private static List<String> readFiles(final Path directory, final String extension) {
        if (!Files.isDirectory(directory)) {
            return List.of();
        }

        try (var stream = Files.list(directory)) {
            return stream.filter(Files::isRegularFile)
                    .filter(file -> file.getFileName().toString().endsWith('.' + extension))
                    .map(Path::toString)
                    .toList();
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to read files in " + directory, ioe);
        }
    }

    private static void addArtifactFiles(
            final List<String> files,
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
                final var location = toArtifactLocation(artifact, candidate);
                if (!containsBasename(files, basename(location))) {
                    files.add(location);
                }
            }
        }
    }

    private static Comparator<String> indexComparator(final List<String> index) {
        return (left, right) -> {
            final var leftBasename = basename(left);
            final var rightBasename = basename(right);
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

    private static void failIfDuplicateBasenames(final List<String> files) {
        final var groups = files.stream()
                .collect(Collectors.groupingBy(FileResolver::basename, LinkedHashMap::new, Collectors.toList()));
        final var duplicates =
                groups.values().stream().filter(values -> values.size() > 1).toList();
        if (!duplicates.isEmpty()) {
            final var detail = duplicates.stream()
                    .map(values -> String.join("\n\t", values))
                    .collect(Collectors.joining("\n\t"));
            throw new FileCollectionException("Files with duplicate basename not allowed.\n\t" + detail);
        }
    }

    private static boolean containsBasename(final List<String> files, final String basename) {
        return files.stream().anyMatch(file -> basename(file).equals(basename));
    }

    private static String basename(final String value) {
        final var slash = Math.max(value.lastIndexOf('/'), value.lastIndexOf('\\'));
        return slash == -1 ? value : value.substring(slash + 1);
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

    private static String toArtifactLocation(final ArtifactContent artifact, final String candidate) {
        return "zip:" + artifact.id() + ':' + candidate;
    }
}
