package org.realityforge.jdbt.files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class FileResolverTest {
    private final FileResolver resolver = new FileResolver();

    @Test
    void collectFilesOrdersByIndexThenAlphabetical(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/Dir1/index.txt", "d.sql\ne.sql\n");
        createFile(tempDir, "db/MyModule/Dir1/c.sql", "");
        createFile(tempDir, "db/MyModule/Dir1/d.sql", "");
        createFile(tempDir, "db/MyModule/Dir1/e.sql", "");
        createFile(tempDir, "db/MyModule/Dir1/f.sql", "");

        final var files = resolver.collectFiles(
                List.of(tempDir.resolve("db")), "MyModule/Dir1", "sql", "index.txt", List.of(), List.of());

        assertThat(basenames(files)).containsExactly("d.sql", "e.sql", "c.sql", "f.sql");
    }

    @Test
    void collectFilesRejectsMissingIndexEntry(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/Dir1/index.txt", "d.sql\nmissing.sql\n");
        createFile(tempDir, "db/MyModule/Dir1/d.sql", "");

        assertThatThrownBy(() -> resolver.collectFiles(
                        List.of(tempDir.resolve("db")), "MyModule/Dir1", "sql", "index.txt", List.of(), List.of()))
                .isInstanceOf(FileCollectionException.class)
                .hasMessageContaining("index entry does not exist");
    }

    @Test
    void collectFilesRejectsDuplicateBasenamesAcrossSearchDirs(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db1/MyModule/base.sql", "");
        createFile(tempDir, "db2/MyModule/base.sql", "");

        assertThatThrownBy(() -> resolver.collectFiles(
                        List.of(tempDir.resolve("db1"), tempDir.resolve("db2")),
                        "MyModule",
                        "sql",
                        "index.txt",
                        List.of(),
                        List.of()))
                .isInstanceOf(FileCollectionException.class)
                .hasMessageContaining("duplicate basename");
    }

    @Test
    void collectFilesPrefersPostOverPreArtifacts(@TempDir final Path tempDir) throws IOException {
        final var post = artifact(tempDir, "post", Map.of("MyModule/base.sql", "--post"));
        final var pre = artifact(tempDir, "pre", Map.of("MyModule/base.sql", "--pre"));

        final var files = resolver.collectFiles(
                List.of(tempDir.resolve("db")), "MyModule", "sql", "index.txt", List.of(post), List.of(pre));

        assertThat(files).containsExactly("zip:post:MyModule/base.sql");
    }

    @Test
    void collectFilesPrefersDiskOverArtifacts(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/base.sql", "--disk");
        final var post = artifact(tempDir, "post", Map.of("MyModule/base.sql", "--post"));

        final var files = resolver.collectFiles(
                List.of(tempDir.resolve("db")), "MyModule", "sql", "index.txt", List.of(post), List.of());

        assertThat(basenames(files)).containsExactly("base.sql");
        assertThat(files.get(0)).doesNotContain("zip:");
    }

    @Test
    void collectFilesUsesArtifactIndexForOrdering(@TempDir final Path tempDir) throws IOException {
        final var entries = new LinkedHashMap<String, String>();
        entries.put("MyModule/index.txt", "b.sql a.sql");
        entries.put("MyModule/a.sql", "");
        entries.put("MyModule/b.sql", "");
        entries.put("MyModule/c.sql", "");
        final var post = artifact(tempDir, "post", entries);

        final var files = resolver.collectFiles(
                List.of(tempDir.resolve("db")), "MyModule", "sql", "index.txt", List.of(post), List.of());

        assertThat(basenames(files)).containsExactly("b.sql", "a.sql", "c.sql");
    }

    @Test
    void collectFixturesRejectsUnexpectedFixtureFiles(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/fixtures/foo.yml", "x");
        createFile(tempDir, "db/MyModule/fixtures/baz.yml", "x");

        assertThatThrownBy(() -> resolver.collectFixtures(
                        List.of(tempDir.resolve("db")),
                        "MyModule",
                        "fixtures",
                        List.of("[MyModule].[foo]"),
                        List.of(),
                        List.of()))
                .isInstanceOf(FileCollectionException.class)
                .hasMessageContaining("Unexpected fixtures");
    }

    @Test
    void collectFixturesRejectsSqlFiles(@TempDir final Path tempDir) throws IOException {
        createFile(tempDir, "db/MyModule/fixtures/foo.sql", "x");

        assertThatThrownBy(() -> resolver.collectFixtures(
                        List.of(tempDir.resolve("db")),
                        "MyModule",
                        "fixtures",
                        List.of("[MyModule].[foo]"),
                        List.of(),
                        List.of()))
                .isInstanceOf(FileCollectionException.class)
                .hasMessageContaining("Unexpected sql files");
    }

    @Test
    void collectFixturesPrefersPostArtifactOverPreArtifact(@TempDir final Path tempDir) throws IOException {
        final var post = artifact(tempDir, "post", Map.of("MyModule/fixtures/MyModule.foo.yml", "x"));
        final var pre = artifact(tempDir, "pre", Map.of("MyModule/fixtures/MyModule.foo.yml", "x"));

        final var fixtures = resolver.collectFixtures(
                List.of(tempDir.resolve("db")),
                "MyModule",
                "fixtures",
                List.of("[MyModule].[foo]"),
                List.of(post),
                List.of(pre));

        assertThat(fixtures.get("[MyModule].[foo]")).isEqualTo("zip:post:MyModule/fixtures/MyModule.foo.yml");
    }

    @Test
    void findFileInModulePrefersDiskThenPostThenPre(@TempDir final Path tempDir) throws IOException {
        final var post = artifact(tempDir, "post", Map.of("MyModule/import/MyModule.foo.sql", "x"));
        final var pre = artifact(tempDir, "pre", Map.of("MyModule/import/MyModule.foo.sql", "x"));

        final var fromPost = resolver.findFileInModule(
                List.of(tempDir.resolve("db")),
                "MyModule",
                "import",
                "[MyModule].[foo]",
                "sql",
                List.of(post),
                List.of(pre));
        assertThat(fromPost).isEqualTo("zip:post:MyModule/import/MyModule.foo.sql");

        createFile(tempDir, "db/MyModule/import/MyModule.foo.sql", "x");
        final var fromDisk = resolver.findFileInModule(
                List.of(tempDir.resolve("db")),
                "MyModule",
                "import",
                "[MyModule].[foo]",
                "sql",
                List.of(post),
                List.of(pre));
        assertThat(fromDisk).endsWith("MyModule.foo.sql");
        assertThat(fromDisk).doesNotContain("zip:");
    }

    private static List<String> basenames(final List<String> files) {
        return files.stream()
                .map(file -> file.substring(file.lastIndexOf('/') + 1))
                .toList();
    }

    private static void createFile(final Path root, final String relativePath, final String content)
            throws IOException {
        final var target = root.resolve(relativePath);
        Files.createDirectories(target.getParent());
        Files.writeString(target, content, StandardCharsets.UTF_8);
    }

    private static ArtifactContent artifact(final Path root, final String id, final Map<String, String> entries)
            throws IOException {
        final var zip = root.resolve(id + ".zip");
        Files.createDirectories(zip.getParent());
        try (var output = new ZipOutputStream(Files.newOutputStream(zip), StandardCharsets.UTF_8)) {
            for (final var entry : entries.entrySet()) {
                output.putNextEntry(new ZipEntry("data/" + entry.getKey()));
                output.write(entry.getValue().getBytes(StandardCharsets.UTF_8));
                output.closeEntry();
            }
        }
        return new ZipArtifactContent(id, zip, "data");
    }
}
