package org.realityforge.jdbt.packaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class DeterministicZipPackagerTest {
    @Test
    void writesStoredEntriesInStableLexicalOrder(@TempDir final Path tempDir) throws IOException {
        final var source = tempDir.resolve("source");
        writeFile(source, "z/2.sql", "two");
        writeFile(source, "a/1.sql", "one");
        writeFile(source, "a/0.sql", "zero");

        final var zipFile = tempDir.resolve("out.zip");
        new DeterministicZipPackager().write(source, zipFile);

        final var entries = readEntries(zipFile);
        assertThat(entries.stream().map(ZipEntry::getName).toList()).containsExactly("a/0.sql", "a/1.sql", "z/2.sql");
        assertThat(entries.stream().map(ZipEntry::getMethod).toList()).allMatch(method -> method == ZipEntry.STORED);
        assertThat(entries.stream().map(ZipEntry::getTime).toList()).containsOnly(0L);
    }

    @Test
    void producesByteForByteReproducibleOutput(@TempDir final Path tempDir) throws IOException {
        final var source = tempDir.resolve("source");
        writeFile(source, "a.sql", "A");
        writeFile(source, "b.sql", "B");

        final var zip1 = tempDir.resolve("one.zip");
        final var zip2 = tempDir.resolve("two.zip");
        final var packager = new DeterministicZipPackager();
        packager.write(source, zip1);

        Files.setLastModifiedTime(source.resolve("a.sql"), FileTime.fromMillis(System.currentTimeMillis()));
        Files.setLastModifiedTime(source.resolve("b.sql"), FileTime.fromMillis(System.currentTimeMillis() + 1000));
        packager.write(source, zip2);

        assertThat(Files.readAllBytes(zip1)).containsExactly(Files.readAllBytes(zip2));
    }

    @Test
    void writesEmptyZipWhenSourceDirectoryMissing(@TempDir final Path tempDir) throws IOException {
        final var missing = tempDir.resolve("missing");
        final var zipFile = tempDir.resolve("out.zip");

        new DeterministicZipPackager().write(missing, zipFile);

        assertThat(zipFile).exists();
        assertThat(readEntries(zipFile)).isEmpty();
    }

    private static void writeFile(final Path root, final String relativePath, final String content) throws IOException {
        final var file = root.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private static List<ZipEntry> readEntries(final Path zipFile) throws IOException {
        final var entries = new ArrayList<ZipEntry>();
        try (var input = Files.newInputStream(zipFile);
                var zip = new ZipInputStream(input, StandardCharsets.UTF_8)) {
            ZipEntry entry = zip.getNextEntry();
            while (null != entry) {
                entries.add(entry);
                entry = zip.getNextEntry();
            }
        }
        return List.copyOf(entries);
    }
}
