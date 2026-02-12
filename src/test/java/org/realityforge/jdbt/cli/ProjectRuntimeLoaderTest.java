package org.realityforge.jdbt.cli;

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
import org.realityforge.jdbt.config.ConfigException;

final class ProjectRuntimeLoaderTest {
    @Test
    void loadMergesRepositoryFromPreLocalAndPostArtifacts(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", """
                preDbArtifacts: [pre.zip]
                postDbArtifacts: [post.zip]
                """);
        writeFile(tempDir, "repository.yml", """
                modules:
                  Local:
                    tables: ["[Local].[tbl]"]
                    sequences: []
                """);
        writeArtifact(tempDir.resolve("pre.zip"), "data/repository.yml", """
                modules:
                  Pre:
                    tables: ["[Pre].[tbl]"]
                    sequences: []
                """);
        writeArtifact(tempDir.resolve("post.zip"), "data/repository.yml", """
                modules:
                  Post:
                    tables: ["[Post].[tbl]"]
                    sequences: []
                """);

        final var runtime = new ProjectRuntimeLoader(tempDir).load(null);

        assertThat(runtime.database().repository().modules()).containsExactly("Pre", "Local", "Post");
        assertThat(runtime.database().preDbArtifacts()).hasSize(1);
        assertThat(runtime.database().postDbArtifacts()).hasSize(1);
        assertThat(runtime.database().schemaHash()).isNotBlank();
    }

    @Test
    void loadRejectsSearchDirsSetting(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", """
                searchDirs: [db]
                """);
        writeFile(tempDir, "repository.yml", """
                modules:
                  A:
                    tables: []
                    sequences: []
                """);

        assertThatThrownBy(() -> new ProjectRuntimeLoader(tempDir).load(null))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unknown key 'searchDirs'");
    }

    @Test
    void loadUsesHardcodedDefaultDatabaseKey(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", "{}\n");
        writeFile(tempDir, "repository.yml", """
                modules:
                  A:
                    tables: []
                    sequences: []
                """);

        final var runtime = new ProjectRuntimeLoader(tempDir).load(null);
        assertThat(runtime.database().key()).isEqualTo("default");
        assertThat(runtime.database().searchDirs()).containsExactly(tempDir);
    }

    @Test
    void loadRejectsSelectedDatabaseWhenNotHardcodedDefault(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", "{}\n");
        writeFile(tempDir, "repository.yml", """
                modules:
                  A:
                    tables: []
                    sequences: []
                """);

        assertThatThrownBy(() -> new ProjectRuntimeLoader(tempDir).load("custom"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Unable to locate database 'custom'");
    }

    @Test
    void loadRejectsArtifactMissingRepositoryYml(@TempDir final Path tempDir) throws IOException {
        writeFile(tempDir, "jdbt.yml", """
                preDbArtifacts: [pre.zip]
                """);
        writeFile(tempDir, "repository.yml", """
                modules:
                  A:
                    tables: []
                    sequences: []
                """);
        writeArtifact(tempDir.resolve("pre.zip"), "data/not-repository.yml", "x");

        assertThatThrownBy(() -> new ProjectRuntimeLoader(tempDir).load("default"))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("does not contain data/repository.yml");
    }

    private static void writeFile(final Path root, final String relativePath, final String content) throws IOException {
        final var file = root.resolve(relativePath);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private static void writeArtifact(final Path zip, final String entryName, final String content) throws IOException {
        try (var output = new ZipOutputStream(Files.newOutputStream(zip), StandardCharsets.UTF_8)) {
            output.putNextEntry(new ZipEntry(entryName));
            output.write(content.getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
        }
    }
}
