package org.realityforge.jdbt.files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ZipArtifactContentTest {
    @Test
    void loadsDataPrefixEntriesOnly(@TempDir final Path tempDir) throws IOException {
        final Path zip = tempDir.resolve("artifact.zip");
        try (var output = new ZipOutputStream(Files.newOutputStream(zip), StandardCharsets.UTF_8)) {
            output.putNextEntry(new ZipEntry("data/a.sql"));
            output.write("x".getBytes(StandardCharsets.UTF_8));
            output.closeEntry();

            output.putNextEntry(new ZipEntry("ignore/b.sql"));
            output.write("x".getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }

        final ZipArtifactContent content = new ZipArtifactContent("id", zip, "data");

        assertThat(content.id()).isEqualTo("id");
        assertThat(content.files()).containsExactly("a.sql");
        assertThat(content.readText("a.sql")).isEqualTo("x");
    }

    @Test
    void readTextFailsForMissingPath(@TempDir final Path tempDir) throws IOException {
        final Path zip = tempDir.resolve("artifact.zip");
        try (var output = new ZipOutputStream(Files.newOutputStream(zip), StandardCharsets.UTF_8)) {
            output.putNextEntry(new ZipEntry("data/a.sql"));
            output.write("x".getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }

        final ZipArtifactContent content = new ZipArtifactContent("id", zip, "data");
        assertThatThrownBy(() -> content.readText("missing.sql"))
                .isInstanceOf(FileCollectionException.class)
                .hasMessageContaining("Missing artifact path");
    }
}
